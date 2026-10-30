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

package dumpers

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type asyncContext struct {
	tc *utils.TransformerContext
	// ch receives a buffered result channel per record that has to be transformed.
	ch chan chan error
}

type transformationWindow struct {
	affectedColumns map[string]struct{}
	window          []*asyncContext
	done            chan struct{}
	eg              *errgroup.Group
	r               *toolkit.Record
	ctx             context.Context
	cancel          context.CancelFunc
	// collectMu guards r and the in-flight result channels.
	collectMu sync.Mutex
}

func newTransformationWindow(ctx context.Context, eg *errgroup.Group) *transformationWindow {
	wctx, cancel := context.WithCancel(ctx)
	return &transformationWindow{
		affectedColumns: map[string]struct{}{},
		done:            make(chan struct{}, 1),
		eg:              eg,
		ctx:             wctx,
		cancel:          cancel,
	}
}

func (tw *transformationWindow) tryAdd(table *entries.Table, t *utils.TransformerContext) bool {

	affectedColumn := t.Transformer.GetAffectedColumns()
	if len(affectedColumn) == 0 {
		if len(tw.window) == 0 {
			for _, c := range table.Columns {
				tw.affectedColumns[c.Name] = struct{}{}
			}
		} else {
			return false
		}
	} else {
		for _, name := range affectedColumn {
			if _, ok := tw.affectedColumns[name]; ok {
				return false
			}
		}
		for _, name := range affectedColumn {
			tw.affectedColumns[name] = struct{}{}
		}
	}

	tw.window = append(tw.window, &asyncContext{
		tc: t,
		ch: make(chan chan error, 1),
	})

	return true
}

// init - runs all transformers in goroutines and waits for the signal channel to transform a record.
// A transformer error cancels the whole window, so no other record can be dispatched into a
// session that is already dead.
func (tw *transformationWindow) init() {
	for _, ac := range tw.window {
		func(ac *asyncContext) {
			tw.eg.Go(func() error {
				for {
					// Normal shutdown (close(done)) must be reported as nil even when the
					// window context is cancelled alongside it.
					select {
					case <-tw.done:
						return nil
					default:
					}
					select {
					case <-tw.done:
						return nil
					case <-tw.ctx.Done():
						select {
						case <-tw.done:
							return nil
						default:
							return tw.ctx.Err()
						}
					case errCh := <-ac.ch:
						_, err := ac.tc.Transformer.Transform(tw.ctx, tw.r)
						// Buffered channel: the goroutine never blocks even if the collector
						// has already abandoned the record due to cancellation.
						errCh <- err
						if err != nil {
							tw.cancel()
							return err
						}
					}
				}
			})
		}(ac)
	}
}

// close - cancels the window context and closes the done channel to stop the transformer goroutines
func (tw *transformationWindow) close() {
	tw.cancel()
	close(tw.done)
}

// Transform - runs the transformation for the record in the window. This function checks the when
// condition of the transformer and if true signals the transformer goroutine to run the transformation.
// Every per-transformer error is collected synchronously: this call returns only after all signalled
// transformers have reported, and the first error aborts the window and (via the pipeline) the table
// reading loop.
func (tw *transformationWindow) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	tw.collectMu.Lock()
	defer tw.collectMu.Unlock()

	tw.r = r
	var calls []chan error
	for _, ac := range tw.window {
		needTransform, err := ac.tc.EvaluateWhen(r)
		if err != nil {
			tw.cancel()
			return nil, fmt.Errorf("error evaluating when condition: %w", err)
		}
		if !needTransform {
			continue
		}

		errCh := make(chan error, 1)
		select {
		case <-ctx.Done():
			tw.cancel()
			return nil, ctx.Err()
		case <-tw.ctx.Done():
			tw.cancel()
			return nil, tw.ctx.Err()
		case ac.ch <- errCh:
			calls = append(calls, errCh)
		}
	}

	var firstErr error
	for _, errCh := range calls {
		select {
		case err := <-errCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			tw.cancel()
			return nil, ctx.Err()
		case <-tw.ctx.Done():
			// Drain the result channel if the transformer still reports, then propagate
			// the window cancellation unless a concrete transformation error arrives.
			if err := <-errCh; err != nil && firstErr == nil {
				firstErr = err
			}
			if firstErr != nil {
				break
			}
			tw.cancel()
			return nil, tw.ctx.Err()
		}
	}

	if firstErr != nil {
		tw.cancel()
		return nil, firstErr
	}
	return r, nil
}
