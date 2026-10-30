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
	"encoding/json"
	"fmt"
)

// Custom command transformer framing protocol (--transform mode).
//
// After the process has started Greenmask sends the table metadata JSON line via stdin
// (this part is kept unchanged for backward compatibility with the metadata exchange).
// Right after a successful initialisation the external command must answer with a single
// ready acknowledgement line - this is the handshake: Greenmask does not start reading
// the table before it is completed.
//
// Afterwards each table record is exchanged as a single JSON envelope line:
//
//	{"seq":0,"payload":"<one raw driver line>"}
//
// The payload carries exactly one line of the underlying row driver (json, csv or text)
// encoded as a JSON string. The sequence number is zero-based, monotonically increasing
// and owned by the table: the response to the record with seq=N must carry the same
// seq=N. A missing, duplicated, reordered, truncated or otherwise unparsable response
// aborts the whole table.
const (
	// ProtocolMessageTypeReady - handshake acknowledgement sent by the external command
	// after a successful initialisation, right before the records exchange starts.
	ProtocolMessageTypeReady = "ready"
)

var (
	readyMessageLine = []byte(`{"type":"ready"}`)
	newLineSeq       = []byte("\n")
)

// ProtocolMessage - JSON envelope of the custom command interaction protocol.
// Field order is part of the wire format: seq precedes payload. The seq field is always
// present (including seq=0), so omitempty must not be used here.
type ProtocolMessage struct {
	Type    string `json:"type,omitempty"`
	Seq     int64  `json:"seq"`
	Payload string `json:"payload,omitempty"`
}

// ReadyMessageLine - returns the ready acknowledgement line including the trailing newline.
func ReadyMessageLine() []byte {
	line := make([]byte, 0, len(readyMessageLine)+1)
	line = append(line, readyMessageLine...)
	line = append(line, newLineSeq...)
	return line
}

// NewRecordMessageLine - encodes one record envelope line with the provided sequence and
// raw driver payload (without trailing newline). The returned line includes "\n".
func NewRecordMessageLine(seq int64, payload []byte) ([]byte, error) {
	msg := ProtocolMessage{
		Seq:     seq,
		Payload: string(payload),
	}
	data, err := json.Marshal(&msg)
	if err != nil {
		return nil, fmt.Errorf("error marshalling record protocol message: %w", err)
	}
	data = append(data, newLineSeq...)
	return data, nil
}

// ParseReadyMessage - parses and validates the handshake acknowledgement line.
func ParseReadyMessage(line []byte) error {
	var msg ProtocolMessage
	if err := json.Unmarshal(bytes.TrimSpace(line), &msg); err != nil {
		return fmt.Errorf("error unmarshalling handshake message: %w", err)
	}
	if msg.Type != ProtocolMessageTypeReady {
		return fmt.Errorf("unexpected handshake message %q: expected type=%q", string(line), ProtocolMessageTypeReady)
	}
	return nil
}

// ParseRecordMessage - parses a record envelope line and returns its sequence and raw payload.
func ParseRecordMessage(line []byte) (seq int64, payload []byte, err error) {
	var msg ProtocolMessage
	if err = json.Unmarshal(bytes.TrimSpace(line), &msg); err != nil {
		return 0, nil, fmt.Errorf("error unmarshalling record protocol message: %w", err)
	}
	if msg.Type != "" {
		return 0, nil, fmt.Errorf("unexpected protocol message type %q in record stream", msg.Type)
	}
	if msg.Payload == "" {
		return 0, nil, fmt.Errorf("empty record payload for seq=%d", msg.Seq)
	}
	return msg.Seq, []byte(msg.Payload), nil
}
