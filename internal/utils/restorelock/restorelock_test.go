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

package restorelock

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/storages/directory"
)

const testDumpId = "1700000000000"

func newRootStorage(t *testing.T) *directory.Storage {
	t.Helper()
	st, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)
	return st
}

func TestAcquireCheckRelease(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)
	now := time.Now()

	hold, err := Acquire(ctx, root, testDumpId, "identity-abc", time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, hold.Token)
	require.Equal(t, testDumpId, hold.DumpId)
	require.Equal(t, "identity-abc", hold.Identity)
	require.True(t, hold.ExpiresAt.After(now))

	// The lock object exists.
	lockPath := filepath.Join(root.GetCwd(), testDumpId, LocksDirName, hold.TokenFileName())
	_, err = os.Stat(lockPath)
	require.NoError(t, err)

	active, err := CheckActive(ctx, root, testDumpId, now)
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, hold.Token, active[0].Token)
	require.Equal(t, "identity-abc", active[0].Identity)

	// Releasing removes the lock and the empty locks directory.
	require.NoError(t, hold.Release())
	active, err = CheckActive(ctx, root, testDumpId, now)
	require.NoError(t, err)
	require.Empty(t, active)
	_, err = os.Stat(filepath.Join(root.GetCwd(), testDumpId, LocksDirName))
	require.True(t, os.IsNotExist(err))
}

func TestCheckActive_RemovesExpiredLockOfCrashedRestore(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)
	now := time.Now()

	hold, err := Acquire(ctx, root, testDumpId, "identity-old", time.Minute)
	require.NoError(t, err)
	// Simulate a restore that died without releasing the hold.

	// After expiry the cleanup request recognizes the hold as stale, removes
	// it and proceeds with its plan.
	active, err := CheckActive(ctx, root, testDumpId, now.Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, active)
	lockPath := filepath.Join(root.GetCwd(), testDumpId, LocksDirName, hold.TokenFileName())
	_, err = os.Stat(lockPath)
	require.True(t, os.IsNotExist(err))
}

func TestCheckActive_RemovesUnreadableLock(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)

	locksSt := root.SubStorage(testDumpId, true).SubStorage(LocksDirName, true)
	require.NoError(t, locksSt.PutObject(ctx, "broken.json", strings.NewReader("not-json")))

	active, err := CheckActive(ctx, root, testDumpId, time.Now())
	require.NoError(t, err)
	require.Empty(t, active)

	names, _, err := locksSt.ListDir(ctx)
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestRenewExtendsHold(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)
	ttl := time.Minute
	now := time.Now()

	hold, err := Acquire(ctx, root, testDumpId, "identity-x", ttl)
	require.NoError(t, err)
	initialExpiry := hold.ExpiresAt

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, hold.Renew(ctx))
	require.True(t, hold.ExpiresAt.After(initialExpiry))

	// At a point in time where the original lock would have been valid, the
	// renewed lock is still seen as active.
	active, err := CheckActive(ctx, root, testDumpId, now.Add(ttl-2*time.Second))
	require.NoError(t, err)
	require.Len(t, active, 1)

	require.NoError(t, hold.Release())
}

func TestCheckActive_NoLocksDirectory(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)

	active, err := CheckActive(ctx, root, "never-restored", time.Now())
	require.NoError(t, err)
	require.Empty(t, active)
}

func TestRelease_DoesNotClobberConcurrentHold(t *testing.T) {
	ctx := context.Background()
	root := newRootStorage(t)

	holdA, err := Acquire(ctx, root, testDumpId, "identity-a", time.Minute)
	require.NoError(t, err)
	holdB, err := Acquire(ctx, root, testDumpId, "identity-b", time.Minute)
	require.NoError(t, err)

	// A finishes first: its own lock disappears but B's hold must survive.
	require.NoError(t, holdA.Release())
	active, err := CheckActive(ctx, root, testDumpId, time.Now())
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, holdB.Token, active[0].Token)

	// B finishes last and removes the now-empty locks directory.
	require.NoError(t, holdB.Release())
	active, err = CheckActive(ctx, root, testDumpId, time.Now())
	require.NoError(t, err)
	require.Empty(t, active)
	_, err = os.Stat(filepath.Join(root.GetCwd(), testDumpId, LocksDirName))
	require.True(t, os.IsNotExist(err))
}
