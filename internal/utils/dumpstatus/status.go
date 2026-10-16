package dumpstatus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/domains"
)

const (
	DoneStatusName            = "done"
	UnknownOrFailedStatusName = "unknown or failed"
	FailedStatusName          = "failed"
	InProgressStatusName      = "in progress"
)

// Backup layout and heartbeat markers. These are the canonical definitions;
// the cmd package re-exports them for backward compatibility.
const (
	MetadataJsonFileName = "metadata.json"
	HeartBeatFileName    = "heartbeat"
)

const HeartBeatWriteInterval = 15 * time.Minute

const (
	HeartBeatDoneContent       = "done"
	HeartBeatInProgressContent = "in-progress"
)

const failedTimeoutMultiplayer time.Duration = 2

// ErrCorruptMetadata marks a backup whose metadata exists but cannot be parsed.
// Such a backup is broken and callers must skip it instead of failing the whole
// procedure or using its (unreadable) data as a baseline.
var ErrCorruptMetadata = errors.New("backup metadata is corrupt")

func GetDumpStatusAndMetadata(ctx context.Context, st storages.Storager) (string, *storage.Metadata, error) {
	objectInfo, err := st.Stat(cmd.HeartBeatFileName)
	if err != nil {
		return "", nil, err
	}
	if !objectInfo.Exist {
		// The logic for legacy
		// 1. Check metadata exist
		// 2. Open if exist and return MD and status Done
		exist, err := isMetadataExist(st)
		if err != nil {
			return "", nil, err
		}
		if !exist {
			return UnknownOrFailedStatusName, nil, nil
		}
		md, err := getMetadata(ctx, st)
		if err != nil {
			if errors.Is(err, storages.ErrFileNotFound) {
				return UnknownOrFailedStatusName, nil, nil
			}
			return "", nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		return heartBeatDoneContent, md, nil
	}

	data, err := getHeartbeat(ctx, st)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get heart beat file: %w", err)
	}
	if len(data) == 0 {
		return FailedStatusName, nil, nil
	}
	switch string(data) {
	case cmd.HeartBeatDoneContent:
		// if done - read and parse metadata
		md, err := getMetadata(ctx, st)
		if err != nil {
			if errors.Is(err, storages.ErrFileNotFound) {
				return UnknownOrFailedStatusName, nil, nil
			}
			return "", nil, fmt.Errorf("failed to get metadata: %w", err)
		}
		return heartBeatDoneContent, md, nil
	case cmd.HeartBeatInProgressContent:
		if time.Now().After(objectInfo.LastModified.Add(cmd.HeartBeatWriteInterval * failedTimeoutMultiplayer)) {
			return FailedStatusName, nil, nil
		}
		return InProgressStatusName, nil, nil
	}
	return UnknownOrFailedStatusName, nil, nil
}

// LookupDoneMetadata returns the parsed metadata of a completed backup bound to
// the storage location st. It performs a single metadata read and never mixes
// data from several reads of candidates that share the same name.
//
// Return semantics:
//   - md != nil, err == nil: the backup is completed and the metadata returned
//     was confirmed to belong to the same backup instance throughout the read.
//   - md == nil, err == nil: the backup is not a usable baseline: it is still
//     in progress, failed, has no metadata, or it disappeared/was replaced
//     while being read. The caller is expected to try the next candidate.
//   - error wrapping ErrCorruptMetadata: the backup is marked completed but its
//     metadata cannot be parsed. The caller must skip this candidate.
//   - any other error: a transient storage error. The caller must stop instead
//     of guessing, since the backup state cannot be determined.
func LookupDoneMetadata(ctx context.Context, st storages.Storager) (*storage.Metadata, error) {
	hbStat, err := st.Stat(cmd.HeartBeatFileName)
	if err != nil {
		return nil, fmt.Errorf("cannot stat heartbeat: %w", err)
	}

	if hbStat.Exist {
		data, err := getHeartbeat(ctx, st)
		if err != nil {
			if isNotFoundError(err) {
				// The heartbeat vanished between the stat and the read: the
				// backup is being modified right now. Pick another candidate.
				return nil, nil
			}
			return nil, fmt.Errorf("failed to read heartbeat: %w", err)
		}
		switch string(data) {
		case cmd.HeartBeatDoneContent:
			// Completed marker: proceed to the metadata.
		case cmd.HeartBeatInProgressContent:
			// The upload or cleanup is still running, even if the marker is
			// stale. Such a backup must never be used as a baseline.
			return nil, nil
		default:
			// Empty or unknown content: failed or unknown backup.
			return nil, nil
		}
	}

	mdStat, err := st.Stat(cmd.MetadataJsonFileName)
	if err != nil {
		return nil, fmt.Errorf("cannot stat metadata: %w", err)
	}
	if !mdStat.Exist {
		// Backups without a heartbeat are considered completed only when the
		// metadata exists. A done heartbeat without metadata means the backup
		// is still in the finalization window (or has failed).
		return nil, nil
	}

	md, readErr := getMetadata(ctx, st)
	if readErr != nil {
		if isNotFoundError(readErr) {
			// The metadata disappeared while it was being read.
			return nil, nil
		}
		if errors.Is(readErr, ErrCorruptMetadata) {
			// Completed backup with unreadable metadata: corrupt candidate.
			return nil, readErr
		}
		// A stream read may fail because the object was deleted mid-transfer;
		// a final stat distinguishes a mutated/vanished candidate from a
		// transient I/O failure.
		mutated, statErr := backupMutated(st, hbStat, mdStat)
		if statErr != nil {
			return nil, fmt.Errorf("failed to read metadata: %w", readErr)
		}
		if mutated {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata: %w", readErr)
	}

	mutated, err := backupMutated(st, hbStat, mdStat)
	if err != nil {
		return nil, fmt.Errorf("cannot confirm backup state: %w", err)
	}
	if mutated {
		// The candidate changed during reading: never bind this metadata to a
		// different backup instance. Reselect another candidate.
		return nil, nil
	}

	return md, nil
}

// backupMutated verifies that the completion marker and the metadata object
// still exist and have not been modified since they were inspected. It returns
// mutated=true when the candidate disappeared or was replaced while read.
func backupMutated(
	st storages.Storager, hbStat, mdStat *domains.ObjectStat,
) (bool, error) {
	currentMdStat, err := st.Stat(cmd.MetadataJsonFileName)
	if err != nil {
		return false, err
	}
	if !currentMdStat.Exist || statChanged(mdStat, currentMdStat) {
		return true, nil
	}

	if !hbStat.Exist {
		// Legacy backup without a heartbeat: nothing else to pin.
		return false, nil
	}

	currentHbStat, err := st.Stat(cmd.HeartBeatFileName)
	if err != nil {
		return false, err
	}
	if !currentHbStat.Exist || statChanged(hbStat, currentHbStat) {
		return true, nil
	}

	return false, nil
}

// statChanged compares two object snapshots. Backends with coarse timestamp
// granularity (S3 reports seconds) may report equal modification times for
// rapid rewrites, but the single-read binding of LookupDoneMetadata already
// prevents mixing two reads; this check is an additional safety net.
func statChanged(before, after *domains.ObjectStat) bool {
	if before.Exist != after.Exist {
		return true
	}
	return !before.LastModified.IsZero() &&
		!after.LastModified.IsZero() &&
		!before.LastModified.Equal(after.LastModified)
}

func isMetadataExist(st storages.Storager) (bool, error) {
	stat, err := st.Stat(cmd.MetadataJsonFileName)
	if err != nil {
		return false, err
	}
	return stat.Exist, nil
}

func getHeartbeat(ctx context.Context, st storages.Storager) ([]byte, error) {
	f, err := st.GetObject(ctx, cmd.HeartBeatFileName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close heart beat file")
		}
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func getMetadata(ctx context.Context, st storages.Storager) (*storage.Metadata, error) {
	mf, err := st.GetObject(ctx, cmd.MetadataJsonFileName)
	if err != nil {
		return nil, fmt.Errorf("get metadata from storage: %w", err)
	}
	defer func() {
		if err := mf.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close metadata file")
		}
	}()

	data, err := io.ReadAll(mf)
	if err != nil {
		return nil, fmt.Errorf("unable to read metadata: %w", err)
	}

	metadata := &storage.Metadata{}
	if err = json.Unmarshal(data, metadata); err != nil {
		// A parse error means the stored content is broken: the candidate is
		// corrupt rather than temporarily unavailable.
		return nil, fmt.Errorf("unable to parse metadata: %w: %w", ErrCorruptMetadata, err)
	}
	return metadata, nil
}

// isNotFoundError reports whether err means that the object does not exist,
// regardless of the storage backend in use.
func isNotFoundError(err error) bool {
	if errors.Is(err, storages.ErrFileNotFound) {
		return true
	}
	return os.IsNotExist(err)
}
