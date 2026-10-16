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

// Package restorelock coordinates restore tasks and dump deletion across
// processes and storage backends.
//
// While a restore is in progress it holds a lock object under
// "<dumpId>/.restore.locks/". The lock carries an expiry timestamp and is
// renewed periodically; both explicit deletion and retention cleanup consult
// the locks before deleting a dump directory and skip (or refuse) dumps held
// by a live restore. Locks left behind by a restore process that died without
// releasing its hold stop being renewed, expire, and are recognized as stale by
// the next cleanup, which then proceeds with its original plan.
package restorelock

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/storages"
)

const (
	// LocksDirName is the per-dump directory holding restore locks. It is
	// created inside the backup directory so that every storage backend that
	// can host a dump can host the locks without any extra configuration.
	LocksDirName = ".restore.locks"
	// lockFileSuffix is the suffix of lock object names.
	lockFileSuffix = ".json"
	// DefaultTTL is how long a lock stays valid after the last renewal.
	DefaultTTL = 5 * time.Minute
	// releaseTimeout bounds lock release when the caller context is already
	// canceled.
	releaseTimeout = 10 * time.Second
)

// ErrDeletionBlocked is returned (explicit delete) or logged (retention
// cleanup) when a dump directory is still held by an active restore.
var ErrDeletionBlocked = errors.New("dump deletion is blocked by an active restore task")

// Lock is the serialized content of a restore hold.
type Lock struct {
	Token     string    `json:"token"`
	DumpId    string    `json:"dumpId"`
	Identity  string    `json:"identity"`
	Host      string    `json:"host"`
	Pid       int       `json:"pid"`
	StartedAt time.Time `json:"startedAt"`
	RenewedAt time.Time `json:"renewedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// Hold is a live restore hold on a dump directory.
type Hold struct {
	rootSt storages.Storager
	ttl    time.Duration
	Lock
}

// IsLockPath reports whether an object path inside a backup directory belongs
// to the restore lock directory (and therefore is not part of the backup
// generation).
func IsLockPath(objectPath string) bool {
	return objectPath == LocksDirName ||
		len(objectPath) > len(LocksDirName) && objectPath[:len(LocksDirName)+1] == LocksDirName+"/"
}

// Acquire establishes a restore hold for the given dump and pinned backup
// identity. Failing to establish the hold must abort the restore: without it
// deletion could reclaim the objects while they are being read.
func Acquire(
	ctx context.Context, rootSt storages.Storager, dumpId string, identity string, ttl time.Duration,
) (*Hold, error) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("cannot generate restore lock token: %w", err)
	}

	now := time.Now()
	h := &Hold{
		rootSt: rootSt,
		ttl:    ttl,
		Lock: Lock{
			Token:     token,
			DumpId:    dumpId,
			Identity:  identity,
			Host:      host,
			Pid:       os.Getpid(),
			StartedAt: now,
			RenewedAt: now,
			ExpiresAt: now.Add(ttl),
		},
	}

	if err := h.write(ctx); err != nil {
		return nil, fmt.Errorf("cannot acquire restore lock for dump %s: %w", dumpId, err)
	}
	return h, nil
}

// TokenFileName returns the object name of the lock within LocksDirName.
func (h *Hold) TokenFileName() string {
	return h.Token + lockFileSuffix
}

// Renew extends the hold. It is safe to call on a best-effort timer: a single
// failed renewal is tolerated as long as the hold is refreshed before ExpiresAt.
func (h *Hold) Renew(ctx context.Context) error {
	now := time.Now()
	h.RenewedAt = now
	h.ExpiresAt = now.Add(h.ttl)
	if err := h.write(ctx); err != nil {
		return fmt.Errorf("cannot renew restore lock %s: %w", h.Token, err)
	}
	return nil
}

// KeepAlive renews the hold until the context is canceled. Renewal failures
// are logged but do not stop the restore immediately: a single missed renewal
// is tolerated while ttl/3 gives three retries before expiry.
func (h *Hold) KeepAlive(ctx context.Context) {
	interval := h.ttl / 3
	if interval <= 0 {
		interval = h.ttl
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.Renew(ctx); err != nil {
				log.Warn().
					Err(err).
					Str("DumpId", h.DumpId).
					Str("BackupIdentity", h.Identity).
					Str("Token", h.Token).
					Time("ExpiresAt", h.ExpiresAt).
					Msg("cannot renew restore lock")
			}
		}
	}
}

// Release removes the hold. It uses an independent, bounded context because it
// usually runs in a deferred call after the request context was canceled.
func (h *Hold) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	locksSt := h.locksStorage()
	if err := locksSt.Delete(ctx, h.TokenFileName()); err != nil {
		return fmt.Errorf("cannot remove restore lock %s: %w", h.Token, err)
	}
	// Remove the marker directory only when no other restore still holds a
	// lock in it. On object stores this is a harmless no-op; on the directory
	// backend it avoids leaving an empty directory behind without clobbering a
	// concurrent restore's hold.
	files, dirs, listErr := locksSt.ListDir(ctx)
	if listErr != nil {
		if errors.Is(listErr, os.ErrNotExist) {
			return nil
		}
		log.Debug().
			Err(listErr).
			Str("DumpId", h.DumpId).
			Msg("cannot list restore lock directory after release")
		return nil
	}
	if len(files) == 0 && len(dirs) == 0 {
		if err := h.dumpStorage().Delete(ctx, LocksDirName); err != nil {
			log.Debug().
				Err(err).
				Str("DumpId", h.DumpId).
				Msg("cannot remove restore lock directory")
		}
	}
	return nil
}

func (h *Hold) dumpStorage() storages.Storager {
	return h.rootSt.SubStorage(h.DumpId, true)
}

func (h *Hold) locksStorage() storages.Storager {
	return h.dumpStorage().SubStorage(LocksDirName, true)
}

func (h *Hold) write(ctx context.Context) error {
	buf := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(h.Lock); err != nil {
		return fmt.Errorf("cannot encode restore lock: %w", err)
	}
	return h.locksStorage().PutObject(ctx, h.TokenFileName(), buf)
}

// CheckActive returns the currently valid (non-expired) restore holds on a
// dump. Expired or unreadable locks are treated as stale: they are deleted so
// that a crashed restore cannot block cleanup forever. The current time is
// injected to keep the logic deterministic and testable.
func CheckActive(ctx context.Context, rootSt storages.Storager, dumpId string, now time.Time) ([]Lock, error) {
	locksSt := rootSt.SubStorage(dumpId, true).SubStorage(LocksDirName, true)

	files, _, err := locksSt.ListDir(ctx)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot list restore locks for dump %s: %w", dumpId, err)
	}

	var active []Lock
	for _, file := range files {
		if path.Ext(file) != lockFileSuffix {
			continue
		}
		lock, err := readLock(ctx, locksSt, file)
		if err != nil {
			log.Warn().
				Err(err).
				Str("DumpId", dumpId).
				Str("LockFile", file).
				Msg("removing unreadable restore lock as stale")
			if delErr := locksSt.Delete(ctx, file); delErr != nil {
				log.Warn().
					Err(delErr).
					Str("DumpId", dumpId).
					Str("LockFile", file).
					Msg("cannot remove stale restore lock")
			}
			continue
		}

		if !now.Before(lock.ExpiresAt) {
			log.Warn().
				Str("DumpId", dumpId).
				Str("Token", lock.Token).
				Str("BackupIdentity", lock.Identity).
				Str("Host", lock.Host).
				Time("StartedAt", lock.StartedAt).
				Time("ExpiresAt", lock.ExpiresAt).
				Msg("removing expired restore lock left by a restore that did not release it")
			if err := locksSt.Delete(ctx, file); err != nil {
				log.Warn().
					Err(err).
					Str("DumpId", dumpId).
					Str("LockFile", file).
					Msg("cannot remove expired restore lock")
			}
			continue
		}

		active = append(active, *lock)
	}

	slices.SortFunc(active, func(a, b Lock) int {
		if a.StartedAt.Before(b.StartedAt) {
			return -1
		}
		if a.StartedAt.After(b.StartedAt) {
			return 1
		}
		return 0
	})
	return active, nil
}

func readLock(ctx context.Context, locksSt storages.Storager, fileName string) (*Lock, error) {
	f, err := locksSt.GetObject(ctx, fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open restore lock: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing restore lock file")
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("cannot read restore lock: %w", err)
	}
	var lock Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, fmt.Errorf("cannot parse restore lock: %w", err)
	}
	if lock.Token == "" || lock.DumpId == "" || lock.ExpiresAt.IsZero() {
		return nil, errors.New("restore lock is missing mandatory fields")
	}
	return &lock, nil
}

func newToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
