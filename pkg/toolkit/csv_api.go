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
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
)

const (
	CsvAttributesDirectNumeratingFormatName = "direct"
	CsvAttributesConfigNumeratingFormatName = "config"
)

type CsvApi struct {
	transferringColumns []*Column
	affectedColumns     []*Column
	w                   *csv.Writer
	stream              io.Writer
	r                   *csv.Reader
	record              *RawRecordCsv
}

func NewCsvApi(transferringColumns []*Column, affectedColumns []*Column, driver *Driver, params *DriverParams) *CsvApi {
	var record *RawRecordCsv

	switch params.CsvAttributesFormat {
	case CsvAttributesDirectNumeratingFormatName:
		record = NewRawRecordCsv(len(driver.Table.Columns), nil)
	case CsvAttributesConfigNumeratingFormatName:
		allColumns := make([]*Column, len(transferringColumns))
		copy(allColumns, transferringColumns)

		for _, c := range affectedColumns {
			if slices.IndexFunc(allColumns, func(col *Column) bool {
				return col.Name == c.Name
			}) == -1 {
				allColumns = append(allColumns, c)
			}
		}

		record = NewRawRecordCsv(len(driver.Table.Columns), allColumns)
	}

	return &CsvApi{
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
		record:              record,
	}
}

func (ca *CsvApi) SetWriter(w io.Writer) {
	ca.stream = w
	ca.w = csv.NewWriter(w)
}

func (ca *CsvApi) SetReader(r io.Reader) {
	ca.r = csv.NewReader(r)
}

func (ca *CsvApi) GetRowDriverFromRecord(r *Record) (RowDriver, error) {
	for _, c := range ca.transferringColumns {

		v, err := r.GetRawColumnValueByIdx(c.Idx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		if err = ca.record.SetColumn(c.Idx, v); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}
	return ca.record, nil
}

func (ca *CsvApi) SetRowDriverToRecord(rd RowDriver, r *Record) error {
	for _, c := range ca.affectedColumns {
		v, err := rd.GetColumn(c.Idx)
		if err != nil {
			return fmt.Errorf(`error getting column %d value: %w`, c.Idx, err)
		}
		err = r.SetRawColumnValueByIdx(c.Idx, v)
		if err != nil {
			return fmt.Errorf(`error setting column %d value to record: %w`, c.Idx, err)
		}
	}
	return nil
}

func (ca *CsvApi) Encode(ctx context.Context, row RowDriver) (err error) {
	line, err := ca.EncodeRow(row)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err = ca.stream.Write(line); err != nil {
		return fmt.Errorf("error encoding row: %w", err)
	}
	if ca.record != nil {
		ca.record.Clean()
	}
	return nil
}

func (ca *CsvApi) Decode(ctx context.Context) (RowDriver, error) {
	ca.record.Clean()
	data, err := ca.r.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading csv record: %w", err)
	}
	ca.record.Data = data

	return ca.record, nil
}

func (ca *CsvApi) EncodeRow(row RowDriver) ([]byte, error) {
	csvRow, ok := row.(*RawRecordCsv)
	if !ok {
		return nil, fmt.Errorf("expected RawRecordCsv but received another driver")
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(csvRow.Data); err != nil {
		return nil, fmt.Errorf("error encoding row: %w", err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("error encoding row: %w", err)
	}
	if ca.record != nil {
		ca.record.Clean()
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func (ca *CsvApi) DecodeRow(line []byte) (RowDriver, error) {
	ca.record.Clean()
	r := csv.NewReader(bytes.NewReader(line))
	data, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading csv record: %w", err)
	}
	// Exactly one record per line: any trailing CSV record means the external command
	// emitted more than one output for a single input.
	if _, err = r.Read(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("error reading csv record: unexpected extra record on the same line")
		}
		return nil, fmt.Errorf("error reading csv record: %w", err)
	}
	ca.record.Data = data

	return ca.record, nil
}

func (ca *CsvApi) Clean() {
	ca.record.Clean()
}
