// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/backupidentity"
)

const acceptanceDumpId = "1700000000000"

func acceptanceTocHeader() *toc.Header {
	dbName := "postgres"
	remoteVersion := "16.1"
	dumpVersion := "16.1"
	return &toc.Header{
		VersionMajor: 1,
		VersionMinor: 16,
		VersionRev:   0,
		Version:      toc.MakeArchiveVersion(1, 16, 0),
		IntSize:      4,
		OffSize:      8,
		Format:       toc.ArchTar,
		CompressionSpec: toc.CompressionSpecification{
			Algorithm: toc.PgCompressionNone,
		},
		ArchDbName:           &dbName,
		ArchiveRemoteVersion: &remoteVersion,
		ArchiveDumpVersion:   &dumpVersion,
	}
}

func writeTocObject(t *testing.T, ctx context.Context, st *directory.Storage, tocObj *toc.Toc) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	require.NoError(t, toc.NewWriter(buf).Write(tocObj))
	require.NoError(t, st.PutObject(ctx, "toc.dat", buf))
}

func newAcceptanceRestore(t *testing.T, root, dump *directory.Storage) *Restore {
	t.Helper()
	cfg := &domains.Restore{}
	return NewRestore("", root, acceptanceDumpId, dump, cfg, nil, t.TempDir())
}

func TestAcceptBackup_RejectsUnfinishedBackupWithoutTouchingDatabase(t *testing.T) {
	ctx := context.Background()

	// Empty dump directory: status is "unknown or failed".
	root0, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpDir0 := filepath.Join(root0.GetCwd(), acceptanceDumpId)
	require.NoError(t, os.Mkdir(dumpDir0, 0o750))
	dump0, err := directory.NewStorage(&directory.Config{Path: dumpDir0})
	require.NoError(t, err)

	r := newAcceptanceRestore(t, root0, dump0)
	_, err = r.acceptBackup(ctx)
	require.ErrorIs(t, err, backupidentity.ErrBackupIncomplete)
	require.Nil(t, r.identity)

	// Heartbeat says the dump is still running.
	root1, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpDir1 := filepath.Join(root1.GetCwd(), acceptanceDumpId)
	require.NoError(t, os.Mkdir(dumpDir1, 0o750))
	dump1, err := directory.NewStorage(&directory.Config{Path: dumpDir1})
	require.NoError(t, err)
	require.NoError(t, dump1.PutObject(ctx, backupidentity.HeartbeatObjectName,
		strings.NewReader(backupidentity.HeartbeatInProgressContent)))

	r = newAcceptanceRestore(t, root1, dump1)
	_, err = r.acceptBackup(ctx)
	require.ErrorIs(t, err, backupidentity.ErrBackupIncomplete)
}

func TestAcceptBackup_PinsGenerationOfFinishedBackup(t *testing.T) {
	ctx := context.Background()
	root, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpDir := filepath.Join(root.GetCwd(), acceptanceDumpId)
	require.NoError(t, os.Mkdir(dumpDir, 0o750))
	dumpSt, err := directory.NewStorage(&directory.Config{Path: dumpDir})
	require.NoError(t, err)

	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.HeartbeatObjectName,
		strings.NewReader(backupidentity.HeartbeatDoneContent)))
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.MetadataObjectName,
		strings.NewReader(`{"header":{"tocEntriesCount":0},"entries":[]}`)))
	writeTocObject(t, ctx, dumpSt, &toc.Toc{
		Header:  acceptanceTocHeader(),
		Entries: []*toc.Entry{},
	})

	r := newAcceptanceRestore(t, root, dumpSt)
	identity, err := r.acceptBackup(ctx)
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Equal(t, identity, r.identity)
	require.NotNil(t, r.tocObj)
	require.Empty(t, r.tocObj.Entries)
}

func TestAcceptBackup_RejectsCorruptedObjectSet(t *testing.T) {
	ctx := context.Background()
	root, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpDir := filepath.Join(root.GetCwd(), acceptanceDumpId)
	require.NoError(t, os.Mkdir(dumpDir, 0o750))
	dumpSt, err := directory.NewStorage(&directory.Config{Path: dumpDir})
	require.NoError(t, err)

	tag := "users"
	desc := toc.TableDataDesc
	namespace := "public"
	fileName := "5.dat.gz"
	tocObj := &toc.Toc{
		Header: acceptanceTocHeader(),
		Entries: []*toc.Entry{
			{
				DumpId:    5,
				HadDumper: 1,
				Section:   toc.SectionData,
				CatalogId: toc.CatalogId{TableOid: 16384, Oid: 16385},
				Tag:       &tag,
				Desc:      &desc,
				Namespace: &namespace,
				FileName:  &fileName,
			},
		},
	}

	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.HeartbeatObjectName,
		strings.NewReader(backupidentity.HeartbeatDoneContent)))
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.MetadataObjectName,
		strings.NewReader(`{"header":{"tocEntriesCount":1},"entries":[{"dumpId":5,"fileName":"5.dat.gz"}]}`)))
	writeTocObject(t, ctx, dumpSt, tocObj)
	// The referenced data object 5.dat.gz is missing: the backup was reaped
	// before the restore accepted it.

	r := newAcceptanceRestore(t, root, dumpSt)
	_, err = r.acceptBackup(ctx)
	require.ErrorIs(t, err, backupidentity.ErrObjectMissing)
	require.Nil(t, r.identity)

	// Metadata and toc.dat disagree about the entry set: reject as corrupted.
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.MetadataObjectName,
		strings.NewReader(`{"header":{"tocEntriesCount":1},"entries":[{"dumpId":5,"fileName":"9.dat.gz"}]}`)))
	r = newAcceptanceRestore(t, root, dumpSt)
	_, err = r.acceptBackup(ctx)
	require.ErrorIs(t, err, backupidentity.ErrBackupCorrupted)
}

func TestAcceptBackup_CompleteBackupWithDataObjectIsAccepted(t *testing.T) {
	ctx := context.Background()
	root, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpDir := filepath.Join(root.GetCwd(), acceptanceDumpId)
	require.NoError(t, os.Mkdir(dumpDir, 0o750))
	dumpSt, err := directory.NewStorage(&directory.Config{Path: dumpDir})
	require.NoError(t, err)

	tag := "users"
	desc := toc.TableDataDesc
	namespace := "public"
	fileName := "5.dat.gz"
	tocObj := &toc.Toc{
		Header: acceptanceTocHeader(),
		Entries: []*toc.Entry{
			{
				DumpId:    5,
				HadDumper: 1,
				Section:   toc.SectionData,
				CatalogId: toc.CatalogId{TableOid: 16384, Oid: 16385},
				Tag:       &tag,
				Desc:      &desc,
				Namespace: &namespace,
				FileName:  &fileName,
			},
		},
	}

	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.HeartbeatObjectName,
		strings.NewReader(backupidentity.HeartbeatDoneContent)))
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.MetadataObjectName,
		strings.NewReader(`{"header":{"tocEntriesCount":1},"entries":[{"dumpId":5,"fileName":"5.dat.gz"}]}`)))
	writeTocObject(t, ctx, dumpSt, tocObj)
	require.NoError(t, dumpSt.PutObject(ctx, "5.dat.gz", strings.NewReader("copy-data")))

	r := newAcceptanceRestore(t, root, dumpSt)
	identity, err := r.acceptBackup(ctx)
	require.NoError(t, err)
	require.True(t, identity.HasObject("5.dat.gz"))
	require.Len(t, r.tocObj.Entries, 1)

	// Sanity: the sentinels used by the data phase are detectable via errors.Is.
	require.True(t, errors.Is(backupidentity.ErrObjectMissing, backupidentity.ErrObjectMissing))
}
