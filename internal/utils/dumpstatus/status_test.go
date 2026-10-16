package dumpstatus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/domains"
)

type fakeObject struct {
	data         []byte
	lastModified time.Time
}

type fakeFS struct {
	mu         sync.Mutex
	objects    map[string]*fakeObject
	statErrors map[string]error
	streamErr  map[string]error
	beforeGet  map[string]func()
}

func newFakeFS() *fakeFS {
	return &fakeFS{
		objects:    map[string]*fakeObject{},
		statErrors: map[string]error{},
		streamErr:  map[string]error{},
		beforeGet:  map[string]func(){},
	}
}

type fakeStorage struct {
	fs  *fakeFS
	cwd string
}

func newFakeStorage(fs *fakeFS, cwd string) *fakeStorage {
	return &fakeStorage{fs: fs, cwd: cwd}
}

func (s *fakeStorage) key(fileName string) string {
	return path.Join(s.cwd, fileName)
}

func (s *fakeStorage) put(t *testing.T, fileName string, data []byte, lastModified time.Time) {
	t.Helper()
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	s.fs.objects[s.key(fileName)] = &fakeObject{data: data, lastModified: lastModified}
}

func (s *fakeStorage) GetCwd() string { return s.cwd }

func (s *fakeStorage) Dirname() string { return path.Base(s.cwd) }

func (s *fakeStorage) ListDir(_ context.Context) ([]string, []storages.Storager, error) {
	return nil, nil, nil
}

func (s *fakeStorage) GetObject(_ context.Context, fileName string) (io.ReadCloser, error) {
	key := s.key(fileName)
	s.fs.mu.Lock()
	if hook, ok := s.fs.beforeGet[key]; ok {
		hook()
	}
	if _, ok := s.fs.streamErr[key]; ok {
		s.fs.mu.Unlock()
		return errReadCloser{}, nil
	}
	obj, ok := s.fs.objects[key]
	s.fs.mu.Unlock()
	if !ok {
		return nil, storages.ErrFileNotFound
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (s *fakeStorage) PutObject(_ context.Context, _ string, _ io.Reader) error {
	return errors.New("not implemented in test")
}

func (s *fakeStorage) Delete(_ context.Context, _ ...string) error { return nil }

func (s *fakeStorage) DeleteAll(_ context.Context, _ string) error { return nil }

func (s *fakeStorage) Exists(_ context.Context, fileName string) (bool, error) {
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	_, ok := s.fs.objects[s.key(fileName)]
	return ok, nil
}

func (s *fakeStorage) SubStorage(subPath string, relative bool) storages.Storager {
	cwd := subPath
	if relative {
		cwd = path.Join(s.cwd, subPath)
	}
	return &fakeStorage{fs: s.fs, cwd: cwd}
}

func (s *fakeStorage) Stat(fileName string) (*domains.ObjectStat, error) {
	key := s.key(fileName)
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	if err, ok := s.fs.statErrors[key]; ok {
		return nil, err
	}
	obj, ok := s.fs.objects[key]
	if !ok {
		return &domains.ObjectStat{Name: key, Exist: false}, nil
	}
	return &domains.ObjectStat{Name: key, Exist: true, LastModified: obj.lastModified}, nil
}

func (s *fakeStorage) Close() error { return nil }

// errReadCloser fails during streaming, simulating a mid-transfer failure.
type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }
func (errReadCloser) Close() error             { return nil }

var validMetadata = mustEncodeMetadata(&storage.Metadata{
	Header: storage.Header{DbName: "appdb"},
})

func mustEncodeMetadata(md *storage.Metadata) []byte {
	data, err := jsonMarshal(md)
	if err != nil {
		panic(err)
	}
	return data
}

func jsonMarshal(md *storage.Metadata) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(md); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestLookupDoneMetadata(t *testing.T) {
	now := time.Now()

	type testCase struct {
		name      string
		setup     func(t *testing.T, fs *fakeFS, backup *fakeStorage)
		wantMD    bool
		wantError func(t *testing.T, err error)
	}

	transientErr := errors.New("storage unavailable")

	cases := []testCase{
		{
			name: "done heartbeat with valid metadata",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now.Add(-time.Minute))
				backup.put(t, MetadataJsonFileName, validMetadata, now.Add(-time.Minute))
			},
			wantMD: true,
		},
		{
			name: "fresh in-progress heartbeat is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatInProgressContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
			},
			wantMD: false,
		},
		{
			name: "stale in-progress heartbeat is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatInProgressContent), now.Add(-time.Hour))
			},
			wantMD: false,
		},
		{
			name: "empty heartbeat means failed and is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte{}, now.Add(-time.Hour))
			},
			wantMD: false,
		},
		{
			name: "unknown heartbeat content is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte("garbage"), now)
			},
			wantMD: false,
		},
		{
			name: "legacy backup without heartbeat but with metadata",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, MetadataJsonFileName, validMetadata, now.Add(-24*time.Hour))
			},
			wantMD: true,
		},
		{
			name: "empty backup directory is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
			},
			wantMD: false,
		},
		{
			name: "done heartbeat without metadata is in the finalization window",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
			},
			wantMD: false,
		},
		{
			name: "corrupt metadata under done heartbeat",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, []byte("{not-json"), now)
			},
			wantMD: false,
			wantError: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrCorruptMetadata)
			},
		},
		{
			name: "transient heartbeat stat error stops with a non-corrupt error",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				fs.statErrors[backup.key(HeartBeatFileName)] = transientErr
			},
			wantMD: false,
			wantError: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.NotErrorIs(t, err, ErrCorruptMetadata)
				assert.ErrorIs(t, err, transientErr)
			},
		},
		{
			name: "transient metadata stat error stops",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				fs.statErrors[backup.key(MetadataJsonFileName)] = transientErr
			},
			wantMD: false,
			wantError: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.ErrorIs(t, err, transientErr)
			},
		},
		{
			name: "heartbeat vanishing between stat and read is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				fs.beforeGet[backup.key(HeartBeatFileName)] = func() {
					delete(fs.objects, backup.key(HeartBeatFileName))
				}
			},
			wantMD: false,
		},
		{
			name: "metadata vanishing while being read is skipped",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
				fs.beforeGet[backup.key(MetadataJsonFileName)] = func() {
					delete(fs.objects, backup.key(MetadataJsonFileName))
				}
				// GetObject happens after the mutation hook, so it reports not-found.
			},
			wantMD: false,
		},
		{
			name: "metadata stream failure and concurrent deletion is reselected",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
				fs.beforeGet[backup.key(MetadataJsonFileName)] = func() {
					delete(fs.objects, backup.key(MetadataJsonFileName))
				}
				// GetObject returns a stream that fails while the object is
				// already gone: the post-read stat detects the mutation.
				fs.streamErr[backup.key(MetadataJsonFileName)] = errors.New("stream failed")
			},
			wantMD: false,
		},
		{
			name: "metadata stream failure while object stays is a transient error",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
				fs.streamErr[backup.key(MetadataJsonFileName)] = errors.New("stream failed")
			},
			wantMD: false,
			wantError: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.NotErrorIs(t, err, ErrCorruptMetadata)
			},
		},
		{
			name: "metadata replaced during read is discarded",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
				fs.beforeGet[backup.key(MetadataJsonFileName)] = func() {
					fs.objects[backup.key(MetadataJsonFileName)] = &fakeObject{
						data:         validMetadata,
						lastModified: now.Add(time.Second),
					}
				}
			},
			wantMD: false,
		},
		{
			name: "heartbeat replaced during metadata read is discarded",
			setup: func(t *testing.T, fs *fakeFS, backup *fakeStorage) {
				backup.put(t, HeartBeatFileName, []byte(HeartBeatDoneContent), now)
				backup.put(t, MetadataJsonFileName, validMetadata, now)
				fs.beforeGet[backup.key(MetadataJsonFileName)] = func() {
					fs.objects[backup.key(HeartBeatFileName)] = &fakeObject{
						data:         []byte(HeartBeatInProgressContent),
						lastModified: now.Add(time.Second),
					}
				}
			},
			wantMD: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeFS()
			backup := newFakeStorage(fs, "/backups/1700000000000")

			tc.setup(t, fs, backup)

			md, err := LookupDoneMetadata(context.Background(), backup)
			if tc.wantError != nil {
				tc.wantError(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.wantMD {
				require.NotNil(t, md)
				assert.Equal(t, "appdb", md.Header.DbName)
			} else {
				assert.Nil(t, md)
			}
		})
	}
}

func TestGetDumpStatusAndMetadataLegacyBehavior(t *testing.T) {
	fs := newFakeFS()
	backup := newFakeStorage(fs, "/backups/legacy")

	backup.put(t, MetadataJsonFileName, validMetadata, time.Now())

	status, md, err := GetDumpStatusAndMetadata(context.Background(), backup)
	require.NoError(t, err)
	assert.Equal(t, DoneStatusName, status)
	require.NotNil(t, md)
}

func TestGetDumpStatusAndMetadataStaleInProgress(t *testing.T) {
	fs := newFakeFS()
	backup := newFakeStorage(fs, "/backups/stale")
	backup.put(t, HeartBeatFileName, []byte(HeartBeatInProgressContent), time.Now().Add(-time.Hour))

	status, md, err := GetDumpStatusAndMetadata(context.Background(), backup)
	require.NoError(t, err)
	assert.Equal(t, FailedStatusName, status)
	assert.Nil(t, md)
}

func TestGetDumpStatusAndMetadataFreshInProgress(t *testing.T) {
	fs := newFakeFS()
	backup := newFakeStorage(fs, "/backups/running")
	backup.put(t, HeartBeatFileName, []byte(HeartBeatInProgressContent), time.Now())

	status, _, err := GetDumpStatusAndMetadata(context.Background(), backup)
	require.NoError(t, err)
	assert.Equal(t, InProgressStatusName, status)
}

func TestCorruptMetadataErrorIsSentinel(t *testing.T) {
	// Ensure callers can detect the corrupt class with errors.Is and format it.
	err := fmt.Errorf("wrap: %w", ErrCorruptMetadata)
	assert.True(t, errors.Is(err, ErrCorruptMetadata))
}
