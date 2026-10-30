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
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/greenmaskio/greenmask/pkg/toolkit/testutils"
)

const passthroughBinaryPackage = "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom/testdata/passthrough"

// buildPassthroughBinary compiles the scratch passthrough transformer built on the
// pkg/toolkit SDK. It is used to verify that the two sides of the custom command
// protocol (handshake and sequenced envelopes) interoperate end-to-end.
func buildPassthroughBinary(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain is not available")
	}
	binaryPath := filepath.Join(t.TempDir(), "passthrough")
	cmd := exec.Command(goBin, "build", "-o", binaryPath, passthroughBinaryPackage)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot build passthrough transformer fixture: %s\n%s", err, string(out))
	}
	return binaryPath
}

// TestCustomCmdTransformer_EndToEndWithSdkBinary drives a real transformer binary built
// on the patched toolkit SDK through the full lifecycle: handshake, strictly ordered
// record exchange and graceful shutdown.
func TestCustomCmdTransformer_EndToEndWithSdkBinary(t *testing.T) {
	binaryPath := buildPassthroughBinary(t)

	table := &toolkit.Table{
		Schema: "public",
		Name:   "e2e",
		Oid:    200200,
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

	colDef := toolkit.MustNewParameterDefinition("column", "affected column").
		SetIsColumn(toolkit.NewColumnProperties().
			SetAffected(true).
			SetAllowedColumnTypes("int2", "int4"),
		).
		SetRequired(true)
	staticParam := toolkit.NewStaticParameter(colDef, driver, false)
	_, err = staticParam.Init(nil, []byte(`"id"`))
	require.NoError(t, err)

	ctd := &TransformerDefinition{
		Name:                     "Passthrough",
		ExpectedExitCode:         0,
		RowTransformationTimeout: 5 * time.Second,
		ValidationTimeout:        10 * time.Second,
		Driver:                   params,
	}
	cct := utils.NewCmdTransformerBase("Passthrough", 0, 5*time.Second, driver, api)
	ct := &CmdTransformer{
		CmdTransformerBase: cct,
		executable:         binaryPath,
		driver:             driver,
		name:               "Passthrough",
		ctd:                ctd,
		parameters: map[string]toolkit.Parameterizer{
			"column": staticParam,
		},
	}

	ctx := context.Background()
	require.NoError(t, ct.Init(ctx))
	for i := 0; i < 5; i++ {
		rec := toolkit.NewRecord(driver)
		rec.SetRow(testutils.NewTestRowDriver([]string{string(rune('0' + i))}))
		_, err = ct.Transform(ctx, rec)
		require.NoError(t, err)
		v, err := rec.GetRawColumnValueByName("id")
		require.NoError(t, err)
		require.False(t, v.IsNull)
		require.Equal(t, string(rune('0'+i)), string(v.Data))
	}
	require.NoError(t, ct.Done(ctx))
	require.Equal(t, int64(5), ct.Seq())
}
