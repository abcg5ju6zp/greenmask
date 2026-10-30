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

package custom

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/greenmaskio/greenmask/pkg/toolkit/testutils"
)

// Common body of POSIX-shell transformers used in the tests. Consumes the metadata line,
// answers the ready handshake and echoes every envelope unchanged (same seq and payload).
const echoTransformerScript = `
read meta
printf '{"type":"ready"}\n'
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
done
`

func newTestCmdTransformer(t *testing.T, script string, rowTimeout time.Duration, expectedExitCode int) *CmdTransformer {
	t.Helper()
	table := &toolkit.Table{
		Schema: "public",
		Name:   "custom_cmd_test",
		Oid:    100500,
		Columns: []*toolkit.Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
		},
	}
	driver, _, err := toolkit.NewDriver(table, nil)
	require.NoError(t, err)

	params := &toolkit.DriverParams{
		Name:                 toolkit.JsonModeName,
		JsonDataFormat:       toolkit.JsonTextDataFormatName,
		JsonAttributesFormat: toolkit.JsonAttributesNamesFormatName,
	}
	api, err := toolkit.NewApi(params, table.Columns, table.Columns, driver)
	require.NoError(t, err)

	if rowTimeout == 0 {
		rowTimeout = 2 * time.Second
	}
	ctd := &TransformerDefinition{
		Name:                     "TestCustomCmd",
		ExpectedExitCode:         expectedExitCode,
		RowTransformationTimeout: rowTimeout,
		ValidationTimeout:        10 * time.Second,
		Driver:                   params,
	}
	cct := utils.NewCmdTransformerBase(ctd.Name, expectedExitCode, rowTimeout, driver, api)

	return &CmdTransformer{
		CmdTransformerBase: cct,
		executable:         "/bin/sh",
		args:               []string{"-c", script},
		driver:             driver,
		name:               ctd.Name,
		ctd:                ctd,
	}
}

func newTestRecord(ct *CmdTransformer, id string) *toolkit.Record {
	record := toolkit.NewRecord(ct.driver)
	record.SetRow(testutils.NewTestRowDriver([]string{id}))
	return record
}

func transformTestRecord(t *testing.T, ct *CmdTransformer, ctx context.Context, id string) {
	t.Helper()
	record := newTestRecord(ct, id)
	_, err := ct.Transform(ctx, record)
	require.NoError(t, err)
	v, err := record.GetRawColumnValueByName("id")
	require.NoError(t, err)
	assert.False(t, v.IsNull)
	assert.Equal(t, id, string(v.Data))
}

func TestCustomCmdTransformer_HappyPath(t *testing.T) {
	ct := newTestCmdTransformer(t, echoTransformerScript, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	for i := 0; i < 5; i++ {
		transformTestRecord(t, ct, ctx, fmt.Sprintf("%d", i+1))
	}
	require.NoError(t, ct.Done(ctx))
	assert.Equal(t, int64(5), ct.Seq())
	assert.True(t, ct.Cmd.ProcessState.Exited())
	assert.Equal(t, 0, ct.Cmd.ProcessState.ExitCode())
}

func TestCustomCmdTransformer_HandshakeFails_ProcessExitsEarly(t *testing.T) {
	// The command exits after receiving metadata without sending the ready message and
	// reports the reason via stderr.
	script := `
read meta
echo "boom: cannot initialize" >&2
exit 1
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	err := ct.Init(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handshake")
	assert.Contains(t, err.Error(), "boom: cannot initialize")
}

func TestCustomCmdTransformer_HandshakeFails_GarbageAck(t *testing.T) {
	script := `
read meta
printf '{"type":"nope"}\n'
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)

	err := ct.Init(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handshake")
}

func TestCustomCmdTransformer_ExitAfterSomeRecords(t *testing.T) {
	// Answers two records, writes stderr and exits before the third one.
	script := `
read meta
printf '{"type":"ready"}\n'
n=0
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
	n=$((n+1))
	if [ $n -ge 2 ]; then
		echo "transformer exploded mid-table" >&2
		exit 1
	fi
done
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")
	transformTestRecord(t, ct, ctx, "2")

	_, err := ct.Transform(ctx, newTestRecord(ct, "3"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seq=2")
	assert.Contains(t, err.Error(), "transformer exploded mid-table")

	// The dead session must reject further records immediately.
	_, err = ct.Transform(ctx, newTestRecord(ct, "4"))
	require.Error(t, err)

	doneErr := ct.Done(ctx)
	require.Error(t, doneErr)
	assert.True(t, ct.Cmd.ProcessState.Exited())
}

func TestCustomCmdTransformer_HalfLineOutput(t *testing.T) {
	// Replies to the second record with a truncated line without newline and exits.
	script := `
read meta
printf '{"type":"ready"}\n'
n=0
while IFS= read -r line; do
	n=$((n+1))
	if [ $n -ge 2 ]; then
		printf '{"seq":1,"paylo'
		echo "half json" >&2
		exit 1
	fi
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
done
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")

	record := newTestRecord(ct, "2")
	_, err := ct.Transform(ctx, record)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seq=1")

	require.Error(t, ct.Done(ctx))
}

func TestCustomCmdTransformer_WrongSequenceOutput(t *testing.T) {
	// Always answers with seq+1.
	script := `
read meta
printf '{"type":"ready"}\n'
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$((seq+1))" "$payload"
done
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	record := newTestRecord(ct, "1")
	_, err := ct.Transform(ctx, record)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out-of-order")
	assert.Contains(t, err.Error(), "seq=0")

	require.Error(t, ct.Done(ctx))
}

func TestCustomCmdTransformer_MissingOutput(t *testing.T) {
	// Exits without replying to the second record.
	script := `
read meta
printf '{"type":"ready"}\n'
n=0
while IFS= read -r line; do
	n=$((n+1))
	if [ $n -ge 2 ]; then
		echo "gone" >&2
		exit 1
	fi
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
done
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")

	record := newTestRecord(ct, "2")
	_, err := ct.Transform(ctx, record)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seq=1")
	assert.Contains(t, err.Error(), "gone")

	require.Error(t, ct.Done(ctx))
}

func TestCustomCmdTransformer_ExtraOutputFailsTable(t *testing.T) {
	// Emits an extra unconsumed line after the last reply.
	script := `
read meta
printf '{"type":"ready"}\n'
n=0
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
	n=$((n+1))
	if [ $n -ge 2 ]; then
		printf 'unexpected-extra-output\n'
	fi
done
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")
	transformTestRecord(t, ct, ctx, "2")

	err := ct.Done(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extra output")
}

func TestCustomCmdTransformer_NonZeroExitAfterAllRecordsFails(t *testing.T) {
	// Every record is processed, but the command exits 7 on stdin EOF. The table must
	// still be treated as failed.
	script := `
read meta
printf '{"type":"ready"}\n'
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
done
exit 7
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")
	transformTestRecord(t, ct, ctx, "2")

	err := ct.Done(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exit code")
	assert.True(t, ct.Cmd.ProcessState.Exited())
	assert.Equal(t, 7, ct.Cmd.ProcessState.ExitCode())
}

func TestCustomCmdTransformer_ExpectedExitCodeAny(t *testing.T) {
	script := `
read meta
printf '{"type":"ready"}\n'
while IFS= read -r line; do
	seq=$(printf '%s' "$line" | sed 's/^{"seq"://; s/,"payload".*//')
	payload=$(printf '%s' "$line" | sed 's/^{"seq":[0-9-]*,"payload"://; s/}$//')
	printf '{"seq":%s,"payload":%s}\n' "$seq" "$payload"
done
exit 7
`
	// expected_exit_code=-1 accepts any graceful exit code.
	ct := newTestCmdTransformer(t, script, time.Second, -1)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	transformTestRecord(t, ct, ctx, "1")
	require.NoError(t, ct.Done(ctx))
}

func TestCustomCmdTransformer_TimeoutKillsProcessGroup(t *testing.T) {
	// Ignores SIGTERM and keeps holding the pipes: the row timeout must fire, the whole
	// process group must be SIGKILLed and Done must return instead of hanging.
	script := `
trap '' TERM
read meta
printf '{"type":"ready"}\n'
sleep 300
`
	ct := newTestCmdTransformer(t, script, 300*time.Millisecond, 0)
	ctx := context.Background()

	require.NoError(t, ct.Init(ctx))
	record := newTestRecord(ct, "1")

	start := time.Now()
	_, err := ct.Transform(ctx, record)
	require.Error(t, err)
	assert.ErrorIs(t, err, utils.ErrRowTransformationTimeout)

	done := make(chan error, 1)
	go func() { done <- ct.Done(ctx) }()
	select {
	case doneErr := <-done:
		require.Error(t, doneErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Done hung after the transformation timeout")
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 8*time.Second, "the stuck process group was not reaped in time")
	// SIGKILLed processes are reaped by Wait but do not report Exited()=true (WIFSIGNALED).
	require.NotNil(t, ct.Cmd.ProcessState)
	assert.True(t, ct.Cmd.ProcessState.Exited() || ct.Cmd.ProcessState.Sys() != nil)
}

func TestCustomCmdTransformer_CancelledContextStopsSession(t *testing.T) {
	script := `
trap '' TERM
read meta
printf '{"type":"ready"}\n'
sleep 300
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, ct.Init(ctx))
	record := newTestRecord(ct, "1")

	cancel()
	_, err := ct.Transform(ctx, record)
	require.Error(t, err)

	require.Error(t, ct.Done(context.Background()))
	require.NotNil(t, ct.Cmd.ProcessState)
}

func TestCustomCmdTransformer_Validate(t *testing.T) {
	script := `
read meta
printf '{"msg":"test validation warning","severity":"warning"}\n'
exit 0
`
	ct := newTestCmdTransformer(t, script, time.Second, 0)

	warnings, err := ct.Validate(context.Background())
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Equal(t, "test validation warning", warnings[0].Msg)
}
