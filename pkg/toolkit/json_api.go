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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

const (
	JsonAttributesIndexesFormatName = "indexes"
	JsonAttributesNamesFormatName   = "names"
	JsonBytesDataFormatName         = "bytes"
	JsonTextDataFormatName          = "text"
)

var emptyJson = []byte("{}\n")

type JsonApi struct {
	transferringColumns []*Column
	affectedColumns     []*Column
	tupleLength         int
	w                   io.Writer
	r                   io.Reader
	encoder             *json.Encoder
	decoder             *json.Decoder
	record              RowDriver
	reader              *bufio.Reader
}

func NewJsonApi(
	transferringColumns []*Column, affectedColumns []*Column, params *DriverParams,
) (*JsonApi, error) {

	var record RowDriver

	if err := params.Validate(); err != nil {
		return nil, fmt.Errorf("error validating driver params: %w", err)
	}

	switch params.JsonAttributesFormat {
	case JsonAttributesIndexesFormatName:
		switch params.JsonDataFormat {
		case JsonBytesDataFormatName:
			r := make(RawRecord, len(transferringColumns))
			record = &r
		case JsonTextDataFormatName:
			r := make(RawRecordStr, len(transferringColumns))
			record = &r
		}
	case JsonAttributesNamesFormatName:
		allColumns := make([]*Column, len(transferringColumns))
		copy(allColumns, transferringColumns)

		for _, c := range affectedColumns {
			if slices.IndexFunc(allColumns, func(col *Column) bool {
				return col.Name == c.Name
			}) == -1 {
				allColumns = append(allColumns, c)
			}
		}

		switch params.JsonDataFormat {
		case JsonBytesDataFormatName:
			record = NewJsonRecordWithAttrNamesBinary(allColumns)
		case JsonTextDataFormatName:
			record = NewJsonRecordWithAttrNamesText(allColumns)
		}
	}

	if record == nil {
		panic("bug: record is nil")
	}

	return &JsonApi{
		transferringColumns: transferringColumns,
		affectedColumns:     affectedColumns,
		tupleLength:         len(transferringColumns),
		record:              record,
	}, nil
}

func (j *JsonApi) SetWriter(w io.Writer) {
	j.w = w
	j.encoder = json.NewEncoder(w)
}

func (j *JsonApi) SetReader(r io.Reader) {
	j.r = r
	j.decoder = json.NewDecoder(r)
	j.reader = bufio.NewReader(r)
}

func (j *JsonApi) GetRowDriverFromRecord(r *Record) (RowDriver, error) {
	for _, c := range j.transferringColumns {

		v, err := r.GetRawColumnValueByIdx(c.Idx)
		if err != nil {
			return nil, fmt.Errorf("error getting raw atribute value: %w", err)
		}
		if err = j.record.SetColumn(c.Idx, v); err != nil {
			return nil, fmt.Errorf("unable to set new value: %w", err)
		}
	}
	return j.record, nil
}

func (j *JsonApi) SetRowDriverToRecord(rd RowDriver, r *Record) error {
	for _, c := range j.affectedColumns {
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

func (j *JsonApi) Encode(ctx context.Context, row RowDriver) (err error) {
	line, err := j.EncodeRow(row)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err = j.w.Write(line); err != nil {
		return err
	}

	return nil
}

func (j *JsonApi) Decode(ctx context.Context) (RowDriver, error) {

	j.record.Clean()
	if err := j.decoder.Decode(j.record); err != nil {
		return nil, fmt.Errorf("error unmarshalling json: %w", err)
	}
	return j.record, nil
}

func (j *JsonApi) EncodeRow(row RowDriver) ([]byte, error) {
	if row.Length() == 0 {
		return bytes.Clone(emptyJson[:len(emptyJson)-1]), nil
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(row); err != nil {
		return nil, fmt.Errorf("error marshaling row driver: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func (j *JsonApi) DecodeRow(line []byte) (RowDriver, error) {

	j.record.Clean()
	dec := json.NewDecoder(bytes.NewReader(line))
	if err := dec.Decode(j.record); err != nil {
		return nil, fmt.Errorf("error unmarshalling json: %w", err)
	}
	// A line must carry exactly one JSON value. Any trailing data means the external
	// command violated the one-line-one-record protocol (doubled or glued records).
	if dec.More() {
		return nil, fmt.Errorf("error unmarshalling json: unexpected data after the record")
	}
	return j.record, nil
}

func (j *JsonApi) Clean() {
	j.record.Clean()
}
