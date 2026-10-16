package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	storageDomains "github.com/greenmaskio/greenmask/internal/storages/domains"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/dumpstatus"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// baselineFakeFS is a shared, thread-safe in-memory storage backend used for
// schema baseline resolution tests.
type baselineFakeFS struct {
	mu         sync.Mutex
	objects    map[string]*baselineFakeObject
	statErrors map[string]error
	listErrors int
	listCalls  int
}

type baselineFakeObject struct {
	data         []byte
	lastModified time.Time
}

func newBaselineFakeFS() *baselineFakeFS {
	return &baselineFakeFS{
		objects:    map[string]*baselineFakeObject{},
		statErrors: map[string]error{},
	}
}

type baselineFakeStorage struct {
	fs    *baselineFakeFS
	cwd   string
	onGet map[string]func()
}

func newBaselineRoot(fs *baselineFakeFS) *baselineFakeStorage {
	return &baselineFakeStorage{fs: fs, cwd: "/backups", onGet: map[string]func(){}}
}

func (s *baselineFakeStorage) sub(dir string) *baselineFakeStorage {
	return &baselineFakeStorage{fs: s.fs, cwd: path.Join(s.cwd, dir), onGet: s.onGet}
}

func (s *baselineFakeStorage) key(fileName string) string {
	return path.Join(s.cwd, fileName)
}

func (s *baselineFakeStorage) put(t *testing.T, fileName string, data []byte) {
	t.Helper()
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	s.fs.objects[s.key(fileName)] = &baselineFakeObject{data: data, lastModified: time.Now()}
}

func (s *baselineFakeStorage) GetCwd() string { return s.cwd }

func (s *baselineFakeStorage) Dirname() string { return path.Base(s.cwd) }

func (s *baselineFakeStorage) ListDir(_ context.Context) ([]string, []storages.Storager, error) {
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	s.fs.listCalls++
	if s.fs.listErrors > 0 {
		s.fs.listErrors--
		return nil, nil, errors.New("listing failed")
	}

	dirSet := map[string]bool{}
	prefix := s.cwd + "/"
	for key := range s.fs.objects {
		if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
			continue
		}
		rest := key[len(prefix):]
		for i := 0; i < len(rest); i++ {
			if rest[i] == '/' {
				dirSet[rest[:i]] = true
				break
			}
		}
	}
	dirs := make([]string, 0, len(dirSet))
	for d := range dirSet {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	res := make([]storages.Storager, 0, len(dirs))
	for _, d := range dirs {
		res = append(res, s.sub(d))
	}
	return nil, res, nil
}

func (s *baselineFakeStorage) GetObject(_ context.Context, fileName string) (io.ReadCloser, error) {
	key := s.key(fileName)
	s.fs.mu.Lock()
	if hook, ok := s.onGet[key]; ok {
		hook()
	}
	obj, ok := s.fs.objects[key]
	s.fs.mu.Unlock()
	if !ok {
		return nil, storages.ErrFileNotFound
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (s *baselineFakeStorage) PutObject(_ context.Context, _ string, _ io.Reader) error {
	return errors.New("not implemented")
}

func (s *baselineFakeStorage) Delete(_ context.Context, _ ...string) error { return nil }
func (s *baselineFakeStorage) DeleteAll(_ context.Context, _ string) error { return nil }
func (s *baselineFakeStorage) Close() error                                { return nil }

func (s *baselineFakeStorage) Exists(_ context.Context, fileName string) (bool, error) {
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	_, ok := s.fs.objects[s.key(fileName)]
	return ok, nil
}

func (s *baselineFakeStorage) SubStorage(subPath string, relative bool) storages.Storager {
	cwd := subPath
	if relative {
		cwd = path.Join(s.cwd, subPath)
	}
	return &baselineFakeStorage{fs: s.fs, cwd: cwd, onGet: s.onGet}
}

func (s *baselineFakeStorage) Stat(fileName string) (*storageDomains.ObjectStat, error) {
	key := s.key(fileName)
	s.fs.mu.Lock()
	defer s.fs.mu.Unlock()
	if err, ok := s.fs.statErrors[key]; ok {
		return nil, err
	}
	obj, ok := s.fs.objects[key]
	if !ok {
		return &storageDomains.ObjectStat{Name: key, Exist: false}, nil
	}
	return &storageDomains.ObjectStat{Name: key, Exist: true, LastModified: obj.lastModified}, nil
}

func mustMetadata(t *testing.T, dbName, description string) []byte {
	t.Helper()
	md := &storageDto.Metadata{
		Header:         storageDto.Header{DbName: dbName},
		Description:    description,
		DatabaseSchema: toolkit.DatabaseSchema{},
	}
	data, err := json.Marshal(md)
	require.NoError(t, err)
	return data
}

func addDoneBackup(t *testing.T, root *baselineFakeStorage, dir, dbName string) {
	t.Helper()
	st := root.sub(dir)
	st.put(t, dumpstatus.HeartBeatFileName, []byte(dumpstatus.HeartBeatDoneContent))
	st.put(t, dumpstatus.MetadataJsonFileName, mustMetadata(t, dbName, dir))
}

func addLegacyBackup(t *testing.T, root *baselineFakeStorage, dir, dbName string) {
	t.Helper()
	root.sub(dir).put(t, dumpstatus.MetadataJsonFileName, mustMetadata(t, dbName, dir))
}

func addInProgressBackup(t *testing.T, root *baselineFakeStorage, dir string) {
	t.Helper()
	st := root.sub(dir)
	st.put(t, dumpstatus.HeartBeatFileName, []byte(dumpstatus.HeartBeatInProgressContent))
	st.put(t, dumpstatus.MetadataJsonFileName, mustMetadata(t, "appdb", dir))
}

func newBaselineValidate(dbName string) *Validate {
	return &Validate{
		Dump: &Dump{dbName: dbName},
	}
}

// mustPin inventories the same way Run does: a single listing sorted newest
// first and stored on the Validate instance.
func mustPin(t *testing.T, ctx context.Context, v *Validate, root storages.Storager) {
	t.Helper()
	v.remoteSt = root
	v.config = domains.NewConfig()
	v.config.Validate.Schema = true
	require.NoError(t, v.pinBackupInventory(ctx))
}

func TestResolveSchemaBaseline(t *testing.T) {
	ctx := context.Background()

	t.Run("newest completed comparable backup is selected and bound", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addDoneBackup(t, root, "200", "appdb")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "200", baseline.dumpId)
		// The metadata is bound to the selected backup identity.
		assert.Equal(t, "200", baseline.md.Description)
		// Resolution never re-lists the storage: inventory stays pinned.
		assert.Equal(t, 1, fs.listCalls)
	})

	t.Run("newest in progress is skipped for an older completed backup", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addInProgressBackup(t, root, "200")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "100", baseline.dumpId)
	})

	t.Run("corrupt highest candidate is skipped", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		corrupt := root.sub("200")
		corrupt.put(t, dumpstatus.HeartBeatFileName, []byte(dumpstatus.HeartBeatDoneContent))
		corrupt.put(t, dumpstatus.MetadataJsonFileName, []byte("{broken"))

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "100", baseline.dumpId)
	})

	t.Run("transient status error stops resolution", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addDoneBackup(t, root, "200", "appdb")
		fs.statErrors[root.sub("200").key(dumpstatus.HeartBeatFileName)] = errors.New("service unavailable")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.Error(t, err)
		assert.Nil(t, baseline)
	})

	t.Run("selected candidate disappearing during read triggers reselection", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addDoneBackup(t, root, "200", "appdb")
		root.onGet[root.sub("200").key(dumpstatus.MetadataJsonFileName)] = func() {
			delete(fs.objects, root.sub("200").key(dumpstatus.MetadataJsonFileName))
		}

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "100", baseline.dumpId)
	})

	t.Run("backup of another database is not comparable", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addDoneBackup(t, root, "200", "otherdb")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "100", baseline.dumpId)
	})

	t.Run("legacy backup without heartbeat is eligible", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addLegacyBackup(t, root, "100", "appdb")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		require.NotNil(t, baseline)
		assert.Equal(t, "100", baseline.dumpId)
	})

	t.Run("no usable history means no baseline", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addInProgressBackup(t, root, "200")

		v := newBaselineValidate("appdb")
		mustPin(t, ctx, v, root)

		baseline, err := v.resolveSchemaBaseline(ctx)
		require.NoError(t, err)
		assert.Nil(t, baseline)
	})
}

// TestResolveSchemaBaselineWithDirectoryBackend exercises the whole flow
// against the real filesystem storage backend.
func TestResolveSchemaBaselineWithDirectoryBackend(t *testing.T) {
	ctx := context.Background()

	putFile := func(t *testing.T, st storages.Storager, name string, content string) {
		t.Helper()
		require.NoError(t, st.PutObject(ctx, name, strings.NewReader(content)))
	}

	root, err := directory.NewStorage(&directory.Config{Path: t.TempDir()})
	require.NoError(t, err)

	// 300: complete backup, but corrupt metadata -> skipped.
	st300 := root.SubStorage("300", true)
	putFile(t, st300, dumpstatus.HeartBeatFileName, dumpstatus.HeartBeatDoneContent)
	putFile(t, st300, dumpstatus.MetadataJsonFileName, "{broken-json")

	// 200: still uploading -> skipped.
	st200 := root.SubStorage("200", true)
	putFile(t, st200, dumpstatus.HeartBeatFileName, dumpstatus.HeartBeatInProgressContent)
	putFile(t, st200, dumpstatus.MetadataJsonFileName, string(mustMetadata(t, "appdb", "200")))

	// 100: complete legacy backup without heartbeat, same database -> selected.
	st100 := root.SubStorage("100", true)
	putFile(t, st100, dumpstatus.MetadataJsonFileName, string(mustMetadata(t, "appdb", "100")))

	v := newBaselineValidate("appdb")
	mustPin(t, ctx, v, root)

	baseline, err := v.resolveSchemaBaseline(ctx)
	require.NoError(t, err)
	require.NotNil(t, baseline)
	assert.Equal(t, "100", baseline.dumpId)
	assert.Equal(t, "appdb", baseline.md.Header.DbName)
}

func TestPinBackupInventory(t *testing.T) {
	ctx := context.Background()

	t.Run("inventory is sorted newest first and listed once", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)
		addDoneBackup(t, root, "100", "appdb")
		addDoneBackup(t, root, "300", "appdb")
		addDoneBackup(t, root, "200", "appdb")

		v := newBaselineValidate("appdb")
		v.remoteSt = root
		v.config = domains.NewConfig()
		v.config.Validate.Schema = true

		require.NoError(t, v.pinBackupInventory(ctx))
		require.Len(t, v.pinnedBackups, 3)
		assert.Equal(t, "300", v.pinnedBackups[0].Dirname())
		assert.Equal(t, "100", v.pinnedBackups[2].Dirname())
		assert.Equal(t, 1, fs.listCalls)
	})

	t.Run("schema diff disabled pins nothing and lists nothing", func(t *testing.T) {
		fs := newBaselineFakeFS()
		root := newBaselineRoot(fs)

		v := newBaselineValidate("appdb")
		v.remoteSt = root
		v.config = domains.NewConfig()
		v.config.Validate.Schema = false

		require.NoError(t, v.pinBackupInventory(ctx))
		assert.Nil(t, v.pinnedBackups)
		assert.Zero(t, fs.listCalls)
	})

	t.Run("listing error is propagated", func(t *testing.T) {
		fs := newBaselineFakeFS()
		fs.listErrors = 1
		root := newBaselineRoot(fs)

		v := newBaselineValidate("appdb")
		v.remoteSt = root
		v.config = domains.NewConfig()
		v.config.Validate.Schema = true

		require.Error(t, v.pinBackupInventory(ctx))
	})
}

func TestDiffWithPreviousSchema(t *testing.T) {
	ctx := context.Background()

	baselineMD := func(schema toolkit.DatabaseSchema) *storageDto.Metadata {
		return &storageDto.Metadata{
			Header:         storageDto.Header{DbName: "appdb"},
			DatabaseSchema: schema,
		}
	}

	newConfiguredValidate := func() *Validate {
		v := newBaselineValidate("appdb")
		v.config = domains.NewConfig()
		v.config.Validate.Schema = true
		v.config.Validate.Format = TextFormat
		v.exitCode = zeroExitCode
		return v
	}

	t.Run("schema check disabled is a no-op", func(t *testing.T) {
		v := newConfiguredValidate()
		v.config.Validate.Schema = false
		require.NoError(t, v.diffWithPreviousSchema(ctx))
	})

	t.Run("identical schema reports no diff", func(t *testing.T) {
		schema := toolkit.DatabaseSchema{
			{Oid: 1, Schema: "public", Name: "users", Kind: "r"},
		}
		v := newConfiguredValidate()
		v.context = &runtimeContext.RuntimeContext{DatabaseSchema: schema}
		v.pinnedBackups = []storages.Storager{
			newPinnedBaseline(t, "100", baselineMD(schema)),
		}

		require.NoError(t, v.diffWithPreviousSchema(ctx))
		assert.Equal(t, zeroExitCode, v.exitCode)
	})

	t.Run("schema change sets non-zero exit code but returns no error", func(t *testing.T) {
		previous := toolkit.DatabaseSchema{
			{Oid: 1, Schema: "public", Name: "users", Kind: "r"},
		}
		current := toolkit.DatabaseSchema{
			{Oid: 1, Schema: "public", Name: "users", Kind: "r"},
			{Oid: 2, Schema: "public", Name: "orders", Kind: "r"},
		}
		v := newConfiguredValidate()
		v.context = &runtimeContext.RuntimeContext{DatabaseSchema: current}
		v.pinnedBackups = []storages.Storager{
			newPinnedBaseline(t, "100", baselineMD(previous)),
		}

		require.NoError(t, v.diffWithPreviousSchema(ctx))
		assert.Equal(t, nonZeroExitCode, v.exitCode)
	})

	t.Run("no baseline means no diff and no failure", func(t *testing.T) {
		v := newConfiguredValidate()
		v.context = &runtimeContext.RuntimeContext{DatabaseSchema: toolkit.DatabaseSchema{}}
		v.pinnedBackups = nil
		require.NoError(t, v.diffWithPreviousSchema(ctx))
		assert.Equal(t, zeroExitCode, v.exitCode)
	})
}

// pinnedBaseline is an in-memory completed-backup stub serving its metadata
// directly, used when diffing must run without touching a storage backend.
type pinnedBaseline struct {
	dir      string
	md       *storageDto.Metadata
	mdData   []byte
	frozenAt time.Time
}

func newPinnedBaseline(t *testing.T, dir string, md *storageDto.Metadata) pinnedBaseline {
	t.Helper()
	data, err := json.Marshal(md)
	require.NoError(t, err)
	return pinnedBaseline{dir: dir, md: md, mdData: data, frozenAt: time.Now()}
}

func (p pinnedBaseline) GetCwd() string  { return p.dir }
func (p pinnedBaseline) Dirname() string { return p.dir }
func (p pinnedBaseline) ListDir(context.Context) ([]string, []storages.Storager, error) {
	return nil, nil, nil
}
func (p pinnedBaseline) GetObject(_ context.Context, fileName string) (io.ReadCloser, error) {
	switch fileName {
	case dumpstatus.MetadataJsonFileName:
		return io.NopCloser(bytes.NewReader(p.mdData)), nil
	case dumpstatus.HeartBeatFileName:
		return io.NopCloser(bytes.NewReader([]byte(dumpstatus.HeartBeatDoneContent))), nil
	default:
		return nil, storages.ErrFileNotFound
	}
}
func (p pinnedBaseline) PutObject(context.Context, string, io.Reader) error { return nil }
func (p pinnedBaseline) Delete(context.Context, ...string) error            { return nil }
func (p pinnedBaseline) DeleteAll(context.Context, string) error            { return nil }
func (p pinnedBaseline) Exists(context.Context, string) (bool, error)       { return true, nil }
func (p pinnedBaseline) SubStorage(string, bool) storages.Storager          { return p }
func (p pinnedBaseline) Stat(fileName string) (*storageDomains.ObjectStat, error) {
	switch fileName {
	case dumpstatus.MetadataJsonFileName, dumpstatus.HeartBeatFileName:
		return &storageDomains.ObjectStat{Name: fileName, Exist: true, LastModified: p.frozenAt}, nil
	default:
		return &storageDomains.ObjectStat{Name: fileName, Exist: false}, nil
	}
}
func (p pinnedBaseline) Close() error { return nil }
