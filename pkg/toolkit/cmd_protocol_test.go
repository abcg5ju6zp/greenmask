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

package toolkit

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestProtocolReadyMessage(t *testing.T) {
	line := ReadyMessageLine()
	require.Equal(t, "{\"type\":\"ready\"}\n", string(line))
	require.NoError(t, ParseReadyMessage(line[:len(line)-1]))
	require.NoError(t, ParseReadyMessage(line)) // trailing newline tolerated
	require.Error(t, ParseReadyMessage([]byte(`{"type":"nope"}`)))
	require.Error(t, ParseReadyMessage([]byte(`{bad json`)))
	require.Error(t, ParseReadyMessage(nil))
}

func TestProtocolRecordMessageRoundtrip(t *testing.T) {
	payload := []byte(`{"id":{"d":"42","n":false}}`)
	line, err := NewRecordMessageLine(7, payload)
	require.NoError(t, err)
	require.Equal(t, "{\"seq\":7,\"payload\":\"{\\\"id\\\":{\\\"d\\\":\\\"42\\\",\\\"n\\\":false}}\"}\n",
		string(line))

	seq, gotPayload, err := ParseRecordMessage(line[:len(line)-1])
	require.NoError(t, err)
	require.Equal(t, int64(7), seq)
	require.Equal(t, string(payload), string(gotPayload))
}

func TestProtocolParseRecordErrors(t *testing.T) {
	// Ready message is not a valid record envelope.
	_, _, err := ParseRecordMessage([]byte(`{"type":"ready"}`))
	require.Error(t, err)

	// Missing payload.
	_, _, err = ParseRecordMessage([]byte(`{"seq":3}`))
	require.Error(t, err)

	// Broken json.
	_, _, err = ParseRecordMessage([]byte(`{"seq":3,"payl`))
	require.Error(t, err)
}

func TestJsonApiRowRoundtrip(t *testing.T) {
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1,
		Columns: []*Column{
			{Name: "id", TypeName: "int4", TypeOid: pgtype.Int4OID, Num: 1, NotNull: true, Length: -1},
		},
	}
	driver, _, err := NewDriver(table, nil)
	require.NoError(t, err)

	params := &DriverParams{
		Name:                 JsonModeName,
		JsonDataFormat:       JsonTextDataFormatName,
		JsonAttributesFormat: JsonAttributesNamesFormatName,
	}
	api, err := NewApi(params, table.Columns, table.Columns, driver)
	require.NoError(t, err)

	record := NewRecord(driver)
	record.SetRow(newTestRowDriver([]string{"1"}))
	require.NoError(t, record.SetColumnValueByName("id", 42))
	rd, err := api.GetRowDriverFromRecord(record)
	require.NoError(t, err)

	line, err := api.EncodeRow(rd)
	require.NoError(t, err)
	require.NotContains(t, string(line), "\n")

	decoded, err := api.DecodeRow(line)
	require.NoError(t, err)
	require.NoError(t, api.SetRowDriverToRecord(decoded, record))

	v, err := record.GetRawColumnValueByIdx(0)
	require.NoError(t, err)
	require.False(t, v.IsNull)
	require.Equal(t, "42", string(v.Data))
}

func TestCsvApiRowRoundtrip(t *testing.T) {
	table := &Table{
		Schema: "public", Name: "test", Oid: 1,
		Columns: []*Column{{Name: "id", TypeName: "int4", TypeOid: pgtype.Int4OID, Num: 1, NotNull: true, Length: -1}},
	}
	driver, _, err := NewDriver(table, nil)
	require.NoError(t, err)

	params := &DriverParams{
		Name:                CsvModeName,
		CsvAttributesFormat: CsvAttributesDirectNumeratingFormatName,
	}
	api := NewCsvApi(table.Columns, table.Columns, driver, params)

	record := NewRecord(driver)
	record.SetRow(newTestRowDriver([]string{"7"}))
	rd, err := api.GetRowDriverFromRecord(record)
	require.NoError(t, err)

	line, err := api.EncodeRow(rd)
	require.NoError(t, err)
	require.Equal(t, `7`, string(line))

	decoded, err := api.DecodeRow(line)
	require.NoError(t, err)
	require.NoError(t, api.SetRowDriverToRecord(decoded, record))
	v, err := record.GetRawColumnValueByIdx(0)
	require.NoError(t, err)
	require.Equal(t, "7", string(v.Data))
}

func TestTextApiRowRoundtrip(t *testing.T) {
	api, err := NewTextApi(0, false)
	require.NoError(t, err)
	record := NewRecord(&Driver{Table: &Table{}})
	record.SetRow(newTestRowDriver([]string{"hello"}))
	rd, err := api.GetRowDriverFromRecord(record)
	require.NoError(t, err)
	line, err := api.EncodeRow(rd)
	require.NoError(t, err)
	require.Equal(t, "hello", string(line))
	decoded, err := api.DecodeRow(line)
	require.NoError(t, err)
	got, err := decoded.Encode()
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestJsonApiDecodeRowRejectsDoubledRecords(t *testing.T) {
	table := &Table{
		Schema: "public", Name: "test", Oid: 1,
		Columns: []*Column{{Name: "id", TypeName: "int4", TypeOid: pgtype.Int4OID, Num: 1, NotNull: true, Length: -1}},
	}
	driver, _, err := NewDriver(table, nil)
	require.NoError(t, err)

	params := &DriverParams{
		Name:                 JsonModeName,
		JsonDataFormat:       JsonTextDataFormatName,
		JsonAttributesFormat: JsonAttributesNamesFormatName,
	}
	api, err := NewApi(params, table.Columns, table.Columns, driver)
	require.NoError(t, err)

	// Two JSON objects on one line must be rejected: one input may produce only one output.
	_, err = api.DecodeRow([]byte(`{"id":{"d":"1","n":false}}{"id":{"d":"2","n":false}}`))
	require.Error(t, err)

	// Truncated JSON is a parse error.
	_, err = api.DecodeRow([]byte(`{"id":{"d":"1"`))
	require.Error(t, err)
}
