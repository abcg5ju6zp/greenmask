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

package delete

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/backupidentity"
	"github.com/greenmaskio/greenmask/internal/utils/restorelock"
)

const testDumpId = "1700000000000"

func newRootWithDump(t *testing.T) (*directory.Storage, string) {
	t.Helper()
	ctx := context.Background()
	root, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	dumpSt := root.SubStorage(testDumpId, true)
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.HeartbeatObjectName,
		strings.NewReader(backupidentity.HeartbeatDoneContent)))
	require.NoError(t, dumpSt.PutObject(ctx, backupidentity.MetadataObjectName, strings.NewReader("{}")))
	require.NoError(t, dumpSt.PutObject(ctx, "5.dat.gz", strings.NewReader("data")))
	return root, filepath.Join(root.GetCwd(), testDumpId)
}

func TestGuardedDelete_WithoutHoldDeletesDump(t *testing.T) {
	ctx := context.Background()
	root, dumpPath := newRootWithDump(t)

	require.NoError(t, guardedDeleteDump(ctx, root, testDumpId, true))
	_, err := os.Stat(dumpPath)
	require.True(t, os.IsNotExist(err))
}

func TestGuardedDelete_ActiveHoldBlocksExplicitAndRetention(t *testing.T) {
	ctx := context.Background()
	root, dumpPath := newRootWithDump(t)

	hold, err := restorelock.Acquire(ctx, root, testDumpId, "identity-123", time.Minute)
	require.NoError(t, err)
	defer func() { _ = hold.Release() }()

	// Explicit deletion is refused and nothing is removed.
	err = guardedDeleteDump(ctx, root, testDumpId, true)
	require.ErrorIs(t, err, restorelock.ErrDeletionBlocked)
	_, err = os.Stat(dumpPath)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(dumpPath, "5.dat.gz"))
	require.NoError(t, err)

	// Retention cleanup skips the dump and continues its plan.
	require.NoError(t, guardedDeleteDump(ctx, root, testDumpId, false))
	_, err = os.Stat(dumpPath)
	require.NoError(t, err)
}

func TestGuardedDelete_ExpiredHoldOfCrashedRestoreDoesNotBlock(t *testing.T) {
	ctx := context.Background()
	root, dumpPath := newRootWithDump(t)

	// Simulate a restore process that died: the lock object stayed behind but
	// its expiry is in the past.
	staleLock := restorelock.Lock{
		Token:     "stale-token",
		DumpId:    testDumpId,
		Identity:  "identity-stale",
		Host:      "dead-host",
		Pid:       4242,
		StartedAt: time.Now().Add(-2 * time.Hour),
		RenewedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	buf, err := json.Marshal(staleLock)
	require.NoError(t, err)
	locksSt := root.SubStorage(testDumpId, true).SubStorage(restorelock.LocksDirName, true)
	require.NoError(t, locksSt.PutObject(ctx, staleLock.Token+".json", strings.NewReader(string(buf))))

	// The cleanup request recognizes the stale hold, removes it and performs
	// the originally planned deletion.
	require.NoError(t, guardedDeleteDump(ctx, root, testDumpId, true))
	_, err = os.Stat(dumpPath)
	require.True(t, os.IsNotExist(err))
}

func TestGuardedDelete_ReleasedHoldAllowsDeletion(t *testing.T) {
	ctx := context.Background()
	root, dumpPath := newRootWithDump(t)

	hold, err := restorelock.Acquire(ctx, root, testDumpId, "identity-456", time.Minute)
	require.NoError(t, err)
	require.NoError(t, hold.Release())

	require.NoError(t, guardedDeleteDump(ctx, root, testDumpId, false))
	_, err = os.Stat(dumpPath)
	require.True(t, errors.Is(err, os.ErrNotExist))
}
