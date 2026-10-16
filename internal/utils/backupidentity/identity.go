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

// Package backupidentity pins the generation of a backup directory.
//
// A backup lives in a single storage directory and consists of metadata.json,
// toc.dat and a set of data objects (table dumps, blobs.toc and large object
// dumps). Retention cleanup or a re-run of the dump command may delete that
// directory or write a new generation under the very same name while a restore
// is reading it.
//
// To prevent workers of an already accepted restore from feeding missing or
// foreign-generation objects into the target database, the restore pins an
// Identity at acceptance time: a fingerprint of the object set together with
// the size and modification time of every object. The identity is re-checked
// before each object is claimed and before every object is opened.
package backupidentity

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/domains"
	"github.com/greenmaskio/greenmask/internal/utils/restorelock"
)

const (
	// MetadataObjectName is the name of the backup metadata descriptor.
	MetadataObjectName = "metadata.json"
	// TocObjectName is the name of the table of contents binary file.
	TocObjectName = "toc.dat"
	// HeartbeatObjectName is the dump lifecycle marker.
	HeartbeatObjectName = "heartbeat"
	// BlobsTocObjectName is the descriptor enumerating dumped large objects.
	BlobsTocObjectName = "blobs.toc"

	// HeartbeatDoneContent marks a completely finished dump.
	HeartbeatDoneContent = "done"
	// HeartbeatInProgressContent marks a running dump.
	HeartbeatInProgressContent = "in-progress"
	// HeartbeatWriteInterval is how often a running dump refreshes the marker.
	HeartbeatWriteInterval = 15 * time.Minute
)

const shortFingerprintLength = 12

var (
	// ErrBackupIncomplete means the backup has not finished (or has failed)
	// and must not be restored.
	ErrBackupIncomplete = errors.New("backup is not complete")
	// ErrBackupCorrupted means metadata.json, toc.dat and the data objects do
	// not describe the same backup.
	ErrBackupCorrupted = errors.New("backup integrity check failed")
	// ErrObjectMissing means a pinned object disappeared from the storage.
	ErrObjectMissing = errors.New("backup object is missing")
	// ErrGenerationChanged means a pinned object was replaced by another
	// generation (size or modification time differ from the pinned one).
	ErrGenerationChanged = errors.New("backup generation has changed")
)

// ObjectFingerprint is the immutable part of an object stat used to tell
// generations apart.
type ObjectFingerprint struct {
	Size         int64
	LastModified time.Time
}

// Identity is the pinned generation of one backup directory.
type Identity struct {
	DumpId string

	objects     map[string]ObjectFingerprint
	fingerprint string
}

// Fingerprint returns the full hex digest of the pinned generation.
func (i *Identity) Fingerprint() string {
	return i.fingerprint
}

// ShortFingerprint returns a short, log-friendly digest prefix.
func (i *Identity) ShortFingerprint() string {
	if len(i.fingerprint) <= shortFingerprintLength {
		return i.fingerprint
	}
	return i.fingerprint[:shortFingerprintLength]
}

// ObjectNames returns the sorted list of pinned object paths.
func (i *Identity) ObjectNames() []string {
	names := make([]string, 0, len(i.objects))
	for name := range i.objects {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// HasObject reports whether the name belongs to the pinned generation.
func (i *Identity) HasObject(name string) bool {
	_, ok := i.objects[name]
	return ok
}

// Build verifies that metadata, toc.dat and every data object describe the
// same finished generation and pins it. Missing or inconsistent objects make
// it return ErrObjectMissing / ErrBackupCorrupted without touching the target
// database: callers run this at acceptance time, before any DB connection is
// opened.
func Build(
	ctx context.Context, st storages.Storager, dumpId string, tocObj *toc.Toc, md *storage.Metadata,
) (*Identity, error) {
	if tocObj == nil {
		return nil, fmt.Errorf("%w: toc is empty", ErrBackupCorrupted)
	}
	if md == nil {
		return nil, fmt.Errorf("%w: metadata is empty", ErrBackupCorrupted)
	}

	required, err := collectRequiredObjects(tocObj, md)
	if err != nil {
		return nil, err
	}

	// Large object dumps are enumerated by blobs.toc at runtime rather than
	// by toc.dat, so resolve and pin them explicitly.
	if required[BlobsTocObjectName] {
		blobObjects, err := collectBlobObjects(ctx, st)
		if err != nil {
			return nil, err
		}
		for _, name := range blobObjects {
			required[name] = true
		}
	}

	present, err := listBackupObjects(ctx, st)
	if err != nil {
		return nil, err
	}

	objects := make(map[string]ObjectFingerprint, len(present))
	for _, name := range present {
		stat, err := statObject(ctx, st, name)
		if err != nil {
			return nil, fmt.Errorf("cannot stat backup object %q: %w", name, err)
		}
		if !stat.Exist {
			// The object vanished between listing and stat-ing it.
			return nil, fmt.Errorf("%w: %s", ErrObjectMissing, name)
		}
		objects[name] = ObjectFingerprint{
			Size:         stat.Size,
			LastModified: stat.LastModified,
		}
	}

	var missing []string
	for name := range required {
		if _, ok := objects[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return nil, fmt.Errorf("%w: missing objects: %s", ErrObjectMissing, strings.Join(missing, ", "))
	}

	fp, err := computeFingerprint(dumpId, objects)
	if err != nil {
		return nil, err
	}

	return &Identity{
		DumpId:      dumpId,
		objects:     objects,
		fingerprint: fp,
	}, nil
}

// VerifyAll re-stats every pinned object. It returns the names of the missing
// and the replaced objects. Storage errors are returned as-is.
func (i *Identity) VerifyAll(ctx context.Context, st storages.Storager) (missing, changed []string, err error) {
	for _, name := range i.ObjectNames() {
		switch err := i.VerifyObject(ctx, st, name); {
		case err == nil:
			continue
		case errors.Is(err, ErrObjectMissing):
			missing = append(missing, name)
		case errors.Is(err, ErrGenerationChanged):
			changed = append(changed, name)
		default:
			return nil, nil, err
		}
	}
	slices.Sort(missing)
	slices.Sort(changed)
	return missing, changed, nil
}

// VerifyAnchors re-stats the objects every generation rewrite must touch:
// metadata.json and toc.dat (plus the heartbeat when the dump format has one).
// This is the cheap per-task generation check.
func (i *Identity) VerifyAnchors(ctx context.Context, st storages.Storager) error {
	anchors := []string{MetadataObjectName, TocObjectName}
	if i.HasObject(HeartbeatObjectName) {
		anchors = append(anchors, HeartbeatObjectName)
	}
	for _, name := range anchors {
		if err := i.VerifyObject(ctx, st, name); err != nil {
			return err
		}
	}
	return nil
}

// VerifyObject re-stats a single object against the pinned generation. Names
// that do not belong to the pinned backup are allowed through.
func (i *Identity) VerifyObject(ctx context.Context, st storages.Storager, name string) error {
	pinned, ok := i.objects[name]
	if !ok {
		return nil
	}
	stat, err := statObject(ctx, st, name)
	if err != nil {
		return fmt.Errorf("cannot stat backup object %q: %w", name, err)
	}
	if !stat.Exist {
		return fmt.Errorf("%w: %s", ErrObjectMissing, name)
	}
	current := ObjectFingerprint{
		Size:         stat.Size,
		LastModified: stat.LastModified,
	}
	if current != pinned {
		return fmt.Errorf(
			"%w: object %s changed since the backup was accepted (pinned size=%d mtime=%s, got size=%d mtime=%s)",
			ErrGenerationChanged, name,
			pinned.Size, pinned.LastModified.Format(time.RFC3339Nano),
			current.Size, current.LastModified.Format(time.RFC3339Nano),
		)
	}
	return nil
}

// collectRequiredObjects builds the set of objects that must exist for a
// consistent restore and cross-validates metadata against the TOC.
func collectRequiredObjects(tocObj *toc.Toc, md *storage.Metadata) (map[string]bool, error) {
	required := map[string]bool{
		MetadataObjectName: true,
		TocObjectName:      true,
	}
	tocFiles := map[string]bool{}

	for _, entry := range tocObj.Entries {
		if entry.FileName != nil && *entry.FileName != "" {
			name := *entry.FileName
			required[name] = true
			tocFiles[name] = true
		}
	}

	// Common identity of metadata, toc.dat and the data objects: the metadata
	// descriptor must describe exactly the TOC it is shipped with.
	var problems []string
	if md.Header.TocEntriesCount != len(tocObj.Entries) {
		problems = append(problems, fmt.Sprintf(
			"toc entries count mismatch: metadata header says %d, toc.dat has %d",
			md.Header.TocEntriesCount, len(tocObj.Entries),
		))
	}
	if len(md.Entries) != len(tocObj.Entries) {
		problems = append(problems, fmt.Sprintf(
			"entries count mismatch: metadata has %d, toc.dat has %d",
			len(md.Entries), len(tocObj.Entries),
		))
	}
	metaFiles := map[string]bool{}
	for _, entry := range md.Entries {
		if entry.FileName != "" {
			metaFiles[entry.FileName] = true
		}
	}
	for name := range tocFiles {
		if !metaFiles[name] {
			problems = append(problems, fmt.Sprintf("data object %q is present in toc.dat but missing in metadata", name))
		}
	}
	for name := range metaFiles {
		if !tocFiles[name] {
			problems = append(problems, fmt.Sprintf("data object %q is present in metadata but missing in toc.dat", name))
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		return nil, fmt.Errorf("%w: %s", ErrBackupCorrupted, strings.Join(problems, "; "))
	}

	return required, nil
}

// collectBlobObjects resolves the individual large object dumps listed in
// blobs.toc.
func collectBlobObjects(ctx context.Context, st storages.Storager) ([]string, error) {
	f, err := st.GetObject(ctx, BlobsTocObjectName)
	if err != nil {
		return nil, fmt.Errorf("cannot open %s: %w", BlobsTocObjectName, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing blobs.toc")
		}
	}()

	var objects []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		oidToken := strings.Fields(line)[0]
		oid, err := strconv.ParseInt(oidToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot parse large object oid %q in %s: %w",
				ErrBackupCorrupted, oidToken, BlobsTocObjectName, err)
		}
		objects = append(objects, fmt.Sprintf("blob_%d.dat.gz", oid))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", BlobsTocObjectName, err)
	}
	return objects, nil
}

// listBackupObjects enumerates every regular object of the backup directory.
// Restore lock markers live inside the backup directory but are no part of the
// backup generation, so they are excluded.
func listBackupObjects(ctx context.Context, st storages.Storager) ([]string, error) {
	all, err := storages.Walk(ctx, st, "")
	if err != nil {
		return nil, fmt.Errorf("cannot list backup objects: %w", err)
	}
	objects := make([]string, 0, len(all))
	for _, name := range all {
		if restorelock.IsLockPath(name) {
			continue
		}
		objects = append(objects, name)
	}
	slices.Sort(objects)
	return objects, nil
}

// statObject normalizes the not-found behaviour of the different backends.
func statObject(ctx context.Context, st storages.Storager, name string) (*domains.ObjectStat, error) {
	_ = ctx
	stat, err := st.Stat(name)
	if err != nil {
		if errors.Is(err, storages.ErrFileNotFound) {
			return &domains.ObjectStat{Exist: false}, nil
		}
		return nil, err
	}
	return stat, nil
}

func computeFingerprint(dumpId string, objects map[string]ObjectFingerprint) (string, error) {
	names := make([]string, 0, len(objects))
	for name := range objects {
		names = append(names, name)
	}
	slices.Sort(names)

	h := sha256.New()
	if _, err := fmt.Fprintf(h, "dumpId=%s\n", dumpId); err != nil {
		return "", err
	}
	for _, name := range names {
		fp := objects[name]
		if _, err := fmt.Fprintf(h, "%s|%d|%s\n",
			name, fp.Size, fp.LastModified.UTC().Format(time.RFC3339Nano)); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
