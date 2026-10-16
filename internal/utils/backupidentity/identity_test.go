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

package backupidentity

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/restorelock"
)

const testDumpId = "1700000000000"

func newDumpStorage(t *testing.T) *directory.Storage {
	t.Helper()
	dir := filepath.Join(t.TempDir(), testDumpId)
	require.NoError(t, os.Mkdir(dir, 0o750))
	st, err := directory.NewStorage(&directory.Config{Path: dir})
	require.NoError(t, err)
	return st
}

func putObject(t *testing.T, ctx context.Context, st *directory.Storage, name, content string) {
	t.Helper()
	require.NoError(t, st.PutObject(ctx, name, strings.NewReader(content)))
}

func strPtr(v string) *string {
	return &v
}

func tableToc(fileName string) *toc.Toc {
	desc := toc.TableDataDesc
	tag := "users"
	namespace := "public"
	return &toc.Toc{
		Header: &toc.Header{},
		Entries: []*toc.Entry{
			{
				DumpId:    5,
				Section:   toc.SectionData,
				Desc:      &desc,
				Tag:       &tag,
				Namespace: &namespace,
				FileName:  &fileName,
			},
		},
	}
}

func tableMetadata(fileName string) *storage.Metadata {
	return &storage.Metadata{
		Header: storage.Header{
			TocEntriesCount: 1,
		},
		Entries: []*storage.Entry{
			{
				DumpId:   5,
				FileName: fileName,
			},
		},
	}
}

// writeFinishedDump writes a consistent minimal backup into st and returns
// the (toc, metadata) pair matching it.
func writeFinishedDump(t *testing.T, ctx context.Context, st *directory.Storage) (*toc.Toc, *storage.Metadata) {
	t.Helper()
	tocObj := tableToc("5.dat.gz")
	md := tableMetadata("5.dat.gz")
	putObject(t, ctx, st, MetadataObjectName, "{}")
	putObject(t, ctx, st, TocObjectName, "TOC-CONTENT")
	putObject(t, ctx, st, HeartbeatObjectName, HeartbeatDoneContent)
	putObject(t, ctx, st, "5.dat.gz", "data")
	return tocObj, md
}

func TestBuild_PinsCommonGeneration(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)
	tocObj, md := writeFinishedDump(t, ctx, st)

	identity, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)
	require.NotEmpty(t, identity.Fingerprint())
	require.Equal(t, []string{"5.dat.gz", HeartbeatObjectName, MetadataObjectName, TocObjectName},
		identity.ObjectNames())

	// Rebuilding without modifications yields the same generation.
	same, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)
	require.Equal(t, identity.Fingerprint(), same.Fingerprint())

	// Restore lock objects are no part of the generation.
	putObject(t, ctx, st, restorelock.LocksDirName+"/deadbeef.json", "{}")
	withLocks, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)
	require.Equal(t, identity.Fingerprint(), withLocks.Fingerprint())
	for _, name := range withLocks.ObjectNames() {
		require.False(t, restorelock.IsLockPath(name))
	}
}

func TestBuild_RejectsMissingRequiredObject(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)
	tocObj, md := writeFinishedDump(t, ctx, st)
	require.NoError(t, os.Remove(filepath.Join(st.GetCwd(), "5.dat.gz")))

	_, err := Build(ctx, st, testDumpId, tocObj, md)
	require.ErrorIs(t, err, ErrObjectMissing)
}

func TestBuild_RejectsMetadataTocMismatch(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)
	tocObj, md := writeFinishedDump(t, ctx, st)

	md.Header.TocEntriesCount = 2
	_, err := Build(ctx, st, testDumpId, tocObj, md)
	require.ErrorIs(t, err, ErrBackupCorrupted)

	md.Header.TocEntriesCount = 1
	md.Entries = nil
	_, err = Build(ctx, st, testDumpId, tocObj, md)
	require.ErrorIs(t, err, ErrBackupCorrupted)
}

func TestBuild_PinsIndividualBlobObjects(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)

	desc := toc.BlobsDesc
	tag := toc.BlobsDesc
	tocObj := &toc.Toc{
		Header: &toc.Header{},
		Entries: []*toc.Entry{
			{
				DumpId:   7,
				Section:  toc.SectionData,
				Desc:     &desc,
				Tag:      &tag,
				FileName: strPtr(BlobsTocObjectName),
			},
		},
	}
	md := &storage.Metadata{
		Header:  storage.Header{TocEntriesCount: 1},
		Entries: []*storage.Entry{{DumpId: 7, FileName: BlobsTocObjectName}},
	}
	putObject(t, ctx, st, MetadataObjectName, "{}")
	putObject(t, ctx, st, TocObjectName, "TOC-CONTENT")
	putObject(t, ctx, st, HeartbeatObjectName, HeartbeatDoneContent)
	putObject(t, ctx, st, BlobsTocObjectName, "100 blob_100.dat.gz\n101 blob_101.dat.gz\n")
	putObject(t, ctx, st, "blob_100.dat.gz", "lo1")
	putObject(t, ctx, st, "blob_101.dat.gz", "lo2")

	identity, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)
	require.True(t, identity.HasObject("blob_100.dat.gz"))
	require.True(t, identity.HasObject("blob_101.dat.gz"))

	// A missing individual large object dump corrupts the accepted generation.
	require.NoError(t, os.Remove(filepath.Join(st.GetCwd(), "blob_101.dat.gz")))
	_, err = Build(ctx, st, testDumpId, tocObj, md)
	require.ErrorIs(t, err, ErrObjectMissing)
}

func TestIdentity_VerifyAnchorsAndObject(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)
	tocObj, md := writeFinishedDump(t, ctx, st)

	identity, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)

	require.NoError(t, identity.VerifyAnchors(ctx, st))
	require.NoError(t, identity.VerifyObject(ctx, st, "5.dat.gz"))

	// Replace a data object with content of a different generation.
	future := time.Now().Add(time.Hour)
	putObject(t, ctx, st, "5.dat.gz", "completely different data from another dump")
	require.NoError(t, os.Chtimes(filepath.Join(st.GetCwd(), "5.dat.gz"), future, future))
	err = identity.VerifyObject(ctx, st, "5.dat.gz")
	require.ErrorIs(t, err, ErrGenerationChanged)

	// Rewriting toc.dat moves the anchors.
	putObject(t, ctx, st, TocObjectName, "NEW-TOC-CONTENT")
	require.NoError(t, os.Chtimes(filepath.Join(st.GetCwd(), TocObjectName), future, future))
	err = identity.VerifyAnchors(ctx, st)
	require.ErrorIs(t, err, ErrGenerationChanged)

	// A removed object is reported as missing.
	require.NoError(t, os.Remove(filepath.Join(st.GetCwd(), "5.dat.gz")))
	_, err = Build(ctx, st, testDumpId, tocObj, md)
	require.ErrorIs(t, err, ErrObjectMissing)
}

func TestPinnedStorage_GetObjectGuardsGeneration(t *testing.T) {
	ctx := context.Background()
	st := newDumpStorage(t)
	tocObj, md := writeFinishedDump(t, ctx, st)

	identity, err := Build(ctx, st, testDumpId, tocObj, md)
	require.NoError(t, err)
	pinned := NewPinnedStorage(st, identity)

	// Pinned object can be read.
	r, err := pinned.GetObject(ctx, "5.dat.gz")
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, "data", string(data))

	// Object replaced by another generation is rejected before opening it.
	future := time.Now().Add(2 * time.Hour)
	putObject(t, ctx, st, "5.dat.gz", "foreign generation payload")
	require.NoError(t, os.Chtimes(filepath.Join(st.GetCwd(), "5.dat.gz"), future, future))
	_, err = pinned.GetObject(ctx, "5.dat.gz")
	require.ErrorIs(t, err, ErrGenerationChanged)

	// Object deleted while pinned is rejected as missing.
	require.NoError(t, os.Remove(filepath.Join(st.GetCwd(), "5.dat.gz")))
	_, err = pinned.GetObject(ctx, "5.dat.gz")
	require.ErrorIs(t, err, ErrObjectMissing)

	// Unknown names are not part of the pin and are passed through to the
	// backend (missing there yields the backend's not-found error).
	_, err = pinned.GetObject(ctx, "not-pinned.txt")
	require.Error(t, err)
}
