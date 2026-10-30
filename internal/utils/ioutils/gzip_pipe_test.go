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

package ioutils

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGzipPipe_CloseWithErrorAbortsReader verifies that a failure on the writer side is
// observed by the storage-reading side as an error instead of a successful stream end.
// This is what keeps a truncated transformed table object from being published.
func TestGzipPipe_CloseWithErrorAbortsReader(t *testing.T) {
	errSentinel := errors.New("dump aborted: transformation failure")

	w, r := NewGzipPipe(false)
	require.Implements(t, (*ErrorWriteCloser)(nil), w)

	// The storage reader consumes the stream concurrently (as PutObject does).
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(r)
		readDone <- readErr
	}()

	_, err := w.Write([]byte("partial table data"))
	require.NoError(t, err)

	require.NoError(t, w.(ErrorWriteCloser).CloseWithError(errSentinel))

	readErr := <-readDone
	require.Error(t, readErr)
	require.ErrorIs(t, readErr, errSentinel)
}

func TestGzipPipe_SuccessDeliversEOF(t *testing.T) {
	w, r := NewGzipPipe(false)

	go func() {
		_, _ = w.Write([]byte("complete data"))
		_ = w.Close()
	}()

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}
