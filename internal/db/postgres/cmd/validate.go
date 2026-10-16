package cmd

import (
	"bufio"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd/validate_utils"
	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/dumpstatus"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	JsonFormat string = "json"
	TextFormat string = "text"
)

const (
	VerticalTableFormat   = "vertical"
	HorizontalTableFormat = "horizontal"
)

const (
	nonZeroExitCode = 1
	zeroExitCode    = 0
)

type closeFunc func()

type Validate struct {
	*Dump
	tmpDir   string
	remoteSt storages.Storager
	exitCode int
	// pinnedBackups holds the remote storage inventory captured once at the
	// validation start. Baseline resolution must never re-list the storage:
	// otherwise a backup that is still uploading or being cleaned up could
	// appear, disappear or be replaced while it is being inspected.
	pinnedBackups []storages.Storager
}

func NewValidate(cfg *domains.Config, registry *utils.TransformerRegistry, st storages.Storager, remoteSt storages.Storager) (*Validate, error) {
	tmpDirName := strconv.FormatInt(time.Now().UnixMilli(), 10)
	st = st.SubStorage(tmpDirName, true)

	d := NewDump(cfg, st, registry)
	d.dumpIdSequence = toc.NewDumpSequence(0)

	d.validate = true
	d.validateRowsLimit = cfg.Validate.RowsLimit
	return &Validate{
		Dump:     d,
		tmpDir:   tmpDirName,
		exitCode: zeroExitCode,
		remoteSt: remoteSt,
	}, nil
}

func (v *Validate) Run(ctx context.Context) (int, error) {

	// The temporary directory must be removed on every exit path, including
	// failures. Its name looks like a backup directory id, therefore leaving
	// it behind could make a later validation mistake partial validation data
	// for a historical backup. Cleanup is unconditional and tolerant of a
	// directory that was never created.
	defer func() {
		log.Debug().Str("TmpDir", v.tmpDir).Msg("cleaning up temporary validation directory")
		if err := v.st.DeleteAll(ctx, ""); err != nil && !isObjectNotFoundError(err) {
			log.Warn().Err(err).Msg("error deleting temporary directory")
		}
	}()
	defer func() {
		v.prune()
	}()
	if err := custom.BootstrapCustomTransformers(ctx, v.registry, v.config.CustomTransformers); err != nil {
		return nonZeroExitCode, fmt.Errorf("error bootstraping custom transformers: %w", err)
	}

	dsn, err := v.pgDumpOptions.GetPgDSN()
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := v.connect(ctx, dsn)
	if err != nil {
		return nonZeroExitCode, err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tx, err := v.startMainTx(ctx, conn)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	if err = v.gatherPgFacts(ctx, tx); err != nil {
		return nonZeroExitCode, fmt.Errorf("error gathering facts: %w", err)
	}

	// Pin the remote inventory at the validation start. All later baseline
	// decisions use this snapshot and never see a backup that appears,
	// disappears or is replaced while the validation is running.
	if err = v.pinBackupInventory(ctx); err != nil {
		return nonZeroExitCode, fmt.Errorf("error pinning backup inventory: %w", err)
	}

	// Get list of tables to validate
	tablesToValidate, err := v.getTablesToValidate()
	if err != nil {
		return nonZeroExitCode, err
	}
	v.config.Dump.Transformation = tablesToValidate

	v.context, err = runtimeContext.NewRuntimeContext(
		ctx, tx, &v.config.Dump, v.registry,
		v.config.Dump.VirtualReferences, v.version,
	)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("unable to build runtime context: %w", err)
	}

	err = toolkit.PrintValidationWarnings(
		v.context.Warnings, v.config.Validate.ResolvedWarnings,
		v.config.Validate.Warnings || v.config.Validate.Strict,
	)
	if err != nil {
		return nonZeroExitCode, err
	}
	if v.context.IsFatal() {
		return nonZeroExitCode, fmt.Errorf("fatal validation error")
	}
	if v.config.Validate.Strict && v.context.Warnings.HasUnresolved(v.config.Validate.ResolvedWarnings) {
		v.exitCode = nonZeroExitCode
	}

	if err = v.diffWithPreviousSchema(ctx); err != nil {
		return nonZeroExitCode, err
	}

	if !v.config.Validate.Data {
		return v.exitCode, nil
	}

	if err = v.dataDump(ctx); err != nil {
		return nonZeroExitCode, err
	}

	if err = v.print(ctx); err != nil {
		return nonZeroExitCode, err
	}

	return v.exitCode, nil
}

func (v *Validate) print(ctx context.Context) error {
	for _, e := range v.dataEntries {
		idx := slices.IndexFunc(v.context.DataSectionObjectsToValidate, func(entry entries.Entry) bool {
			t, ok := entry.(*entries.Table)
			if !ok {
				return false
			}
			return t.DumpId == e.DumpId
		})

		if idx == -1 {
			// skip if not in DataSectionObjectsToValidate
			continue
		}

		t, ok := v.context.DataSectionObjectsToValidate[idx].(*entries.Table)
		if !ok {
			return fmt.Errorf("table %d not found", e.DumpId)
		}
		doc, err := v.createDocument(ctx, t)
		if err != nil {
			return fmt.Errorf("unable to create validation document: %w", err)
		}

		if err = doc.Print(os.Stdout); err != nil {
			return fmt.Errorf("unable to print validation document: %w", err)
		}
	}
	return nil
}

func (v *Validate) getDocument(table *entries.Table) validate_utils.Documenter {
	switch v.config.Validate.Format {
	case JsonFormat:
		return validate_utils.NewJsonDocument(table, v.config.Validate.Diff, v.config.Validate.OnlyTransformed)
	case TextFormat:
		return validate_utils.NewTextDocument(
			table, v.config.Validate.Diff, v.config.Validate.OnlyTransformed, v.config.Validate.TableFormat,
		)
	default:
		panic(fmt.Sprintf("unknown format %s", v.config.Validate.Format))
	}
}

func (v *Validate) getReader(ctx context.Context, table *entries.Table) (closeFunc, *bufio.Reader, error) {
	tableData, err := v.st.GetObject(ctx, fmt.Sprintf("%d.dat.gz", table.DumpId))
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get object from storage: %w", err)
	}

	gz, err := gzip.NewReader(tableData)
	if err != nil {
		if err := tableData.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing table data")
		}
		return nil, nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	f := func() {
		if err := tableData.Close(); err != nil {
			log.Warn().Err(err).Msg("caused error when closing reader object")
		}
		if err := gz.Close(); err != nil {
			log.Warn().Err(err).Msg("caused error when closing gzip reader")
		}
	}

	return f, bufio.NewReader(gz), nil
}

func (v *Validate) readRecords(r *bufio.Reader, t *entries.Table) (original, transformed *pgcopy.Row, err error) {
	var originalLine, transformedLine []byte
	var originalRow, transformedRow *pgcopy.Row

	originalRow = pgcopy.NewRow(len(t.Columns))
	transformedRow = pgcopy.NewRow(len(t.Columns))

	originalLine, err = reader.ReadLine(r, nil)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("unable to read line: %w", err)
	}
	// Handle end of dump_objects file seq
	if validate_utils.LineIsEndOfData(originalLine) {
		return nil, nil, io.EOF
	}

	transformedLine, err = reader.ReadLine(r, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to read line: %w", err)
	}

	if err = originalRow.Decode(originalLine); err != nil {
		return nil, nil, fmt.Errorf("error decoding copy line: %w", err)
	}
	if err = transformedRow.Decode(transformedLine); err != nil {
		return nil, nil, fmt.Errorf("error decoding copy line: %w", err)
	}
	return originalRow, transformedRow, nil
}

func (v *Validate) createDocument(ctx context.Context, t *entries.Table) (validate_utils.Documenter, error) {
	doc := v.getDocument(t)

	closeReader, r, err := v.getReader(ctx, t)
	if err != nil {
		return nil, err
	}
	defer closeReader()

	var line int
	for {

		original, transformed, err := v.readRecords(r, t)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if err := doc.Append(original, transformed); err != nil {
			return nil, fmt.Errorf("unable to append line %d to document: %w", line, err)
		}

		line++
	}

	return doc, nil
}

func (v *Validate) getTablesToValidate() ([]*domains.Table, error) {
	var tablesToValidate []*domains.Table
	for _, tv := range v.config.Validate.Tables {

		schemaName, tableName, err := parseTableName(tv)
		if err != nil {
			return nil, err
		}

		foundTable, err := findTableBySchemaAndName(v.config.Dump.Transformation, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		if foundTable != nil {
			tablesToValidate = append(tablesToValidate, foundTable)
		}
	}

	if len(tablesToValidate) == 0 {
		return v.config.Dump.Transformation, nil
	}

	return tablesToValidate, nil
}

// schemaBaseline binds a backup identity to exactly one metadata read. The
// pair is produced atomically by resolveSchemaBaseline and must never be
// assembled from different candidates.
type schemaBaseline struct {
	dumpId string
	md     *storageDto.Metadata
}

func (v *Validate) diffWithPreviousSchema(ctx context.Context) error {
	if !v.config.Validate.Schema {
		return nil
	}

	// Resolve and confirm the baseline before the current schema snapshot is
	// compared with anything. No diff is computed or printed until both the
	// live schema snapshot and a single, bound baseline metadata are known.
	baseline, err := v.resolveSchemaBaseline(ctx)
	if err != nil {
		return fmt.Errorf("cannot resolve schema diff baseline: %w", err)
	}
	if baseline == nil {
		return nil
	}

	diff := baseline.md.DatabaseSchema.Diff(v.context.DatabaseSchema)
	if len(diff) > 0 {
		v.exitCode = nonZeroExitCode

		if err = v.printSchemaDiff(diff, baseline.dumpId); err != nil {
			return fmt.Errorf("cannot print schema diff: %w", err)
		}
	}

	return nil
}

func (v *Validate) printSchemaDiff(diff []*toolkit.DiffNode, previousDumpId string) error {

	if v.config.Validate.Format == JsonFormat {
		data, err := json.Marshal(diff)
		if err != nil {
			return fmt.Errorf("cannot encode diff node: %w", err)
		}
		log.Warn().
			Str("PreviousDumpId", previousDumpId).
			RawJSON("Diff", data).
			Str("Hint", "Check schema changes before making new dump").
			Msg("Database schema has been changed")
		return nil
	}
	log.Warn().
		Str("PreviousDumpId", previousDumpId).
		Str("Hint", "Check schema changes before making new dump").
		Msg("Database schema has been changed")

	for _, node := range diff {
		log.Warn().
			Str("Event", node.Event).
			Any("Signature", node.Signature).
			Msg(toolkit.DiffEventMsgs[node.Event])
	}

	return nil
}

// pinBackupInventory captures the remote storage inventory once. It is called
// at the validation start, before any backup state is read.
func (v *Validate) pinBackupInventory(ctx context.Context) error {
	if !v.config.Validate.Schema || v.remoteSt == nil {
		return nil
	}

	_, dirs, err := v.remoteSt.ListDir(ctx)
	if err != nil {
		return fmt.Errorf("cannot list backups: %w", err)
	}

	// Sort the pinned snapshot once, newest backup first.
	slices.SortFunc(dirs, func(a, b storages.Storager) int {
		return cmp.Compare(b.Dirname(), a.Dirname())
	})
	v.pinnedBackups = dirs
	return nil
}

// resolveSchemaBaseline selects the most recent backup that can safely be used
// as a schema diff baseline. It iterates only over the inventory pinned at the
// validation start and never re-lists the storage.
//
// Outcomes for every candidate are deterministic:
//   - completed, readable and comparable backup: used as the baseline;
//   - corrupt backup (status is done but metadata is unreadable): skipped with
//     a warning so older backups can still provide a baseline;
//   - transient storage error while reading the status: validation stops;
//   - candidate that disappears or is replaced while being read: skipped and
//     another candidate from the pinned inventory is selected.
func (v *Validate) resolveSchemaBaseline(ctx context.Context) (*schemaBaseline, error) {
	for _, candidate := range v.pinnedBackups {
		baseline, err := v.inspectBaselineCandidate(ctx, candidate)
		if err != nil {
			// The backup state cannot be determined right now: stop instead
			// of guessing and reporting a schema change that may not exist.
			return nil, err
		}
		if baseline != nil {
			return baseline, nil
		}
	}
	return nil, nil
}

func (v *Validate) inspectBaselineCandidate(ctx context.Context, candidate storages.Storager) (*schemaBaseline, error) {
	dumpId := candidate.Dirname()

	md, err := dumpstatus.LookupDoneMetadata(ctx, candidate)
	if err != nil {
		if errors.Is(err, dumpstatus.ErrCorruptMetadata) {
			// A corrupt highest candidate must not abort validation: skip it
			// and continue with older backups from the pinned inventory.
			log.Warn().
				Err(err).
				Str("DumpId", dumpId).
				Msg("skipping corrupt backup while looking for schema diff baseline")
			return nil, nil
		}
		return nil, fmt.Errorf("cannot determine backup %q state: %w", dumpId, err)
	}
	if md == nil {
		// In progress, failed, completed for another purpose, or vanished
		// while being read: another candidate may be usable.
		log.Debug().
			Str("DumpId", dumpId).
			Msg("backup is not a usable schema diff baseline, trying the next one")
		return nil, nil
	}

	if !v.isMetadataComparable(md) {
		log.Debug().
			Str("DumpId", dumpId).
			Str("BackupDatabase", md.Header.DbName).
			Str("CurrentDatabase", v.dbName).
			Msg("backup belongs to another database, skipping it as schema diff baseline")
		return nil, nil
	}

	return &schemaBaseline{
		dumpId: dumpId,
		md:     md,
	}, nil
}

// isMetadataComparable reports whether a backup can be diffed against the
// current PostgreSQL database. Table OIDs are only unique inside a database,
// therefore a backup taken from another database must never be compared here.
func (v *Validate) isMetadataComparable(md *storageDto.Metadata) bool {
	return md.Header.DbName != "" && md.Header.DbName == v.dbName
}

// isObjectNotFoundError reports whether err means that the object does not
// exist, regardless of the storage backend in use. Removing a temporary
// directory that was never created must not be treated as a failure.
func isObjectNotFoundError(err error) bool {
	if errors.Is(err, storages.ErrFileNotFound) {
		return true
	}
	return os.IsNotExist(err)
}

func findTableBySchemaAndName(Transformations []*domains.Table, schemaName, tableName string) (*domains.Table, error) {
	var foundTable *domains.Table
	for _, t := range Transformations {
		if t.Schema == schemaName && t.Name == tableName {
			foundTable = t
			break
		}
		if schemaName == "" && t.Name == tableName {
			if foundTable != nil {
				return nil, fmt.Errorf("wrong \"validate_table\" value: unable uniqually identify table \"%s\": sepcify schema name", tableName)
			}
			foundTable = t
		}
	}
	if foundTable == nil {
		errMsg := fmt.Sprintf("table %s is not found in transformation config", tableName)
		if schemaName != "" && tableName != "" {
			errMsg = fmt.Sprintf("table %s.%s is not found in transformation config", schemaName, tableName)
		}
		return nil, fmt.Errorf("unable to find table from \"validate_table\" parameter: %s", errMsg)
	}
	return foundTable, nil
}

func parseTableName(name string) (schemaName string, tableName string, err error) {
	parts := strings.Split(name, ".")

	if len(parts) > 2 {
		return "", "", fmt.Errorf("wrong \"validate_table\" format \"%s\": value has %d coma symbols (.)", name, len(parts))
	} else if len(parts) == 2 {
		schemaName = parts[0]
		tableName = parts[1]
	} else {
		tableName = parts[0]
	}
	return
}
