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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	ErrValidationTimeout = errors.New("validation timeout")
)

const (
	ValidateArgName        = "--validate"
	PrintDefinitionArgName = "--print-definition"
	TransformArgName       = "--transform"
	// DefaultHandshakeTimeout - time to wait for the transformer ready acknowledgement
	// after sending the table metadata.
	DefaultHandshakeTimeout = 20 * time.Second
)

func ProduceNewCmdTransformerFunction(ctd *TransformerDefinition) utils.NewTransformerFunc {
	return func(
		ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer,
	) (utils.Transformer, toolkit.ValidationWarnings, error) {
		return NewCustomCmdTransformer(ctx, driver, parameters, ctd)
	}
}

type CmdTransformer struct {
	*utils.CmdTransformerBase

	name            string
	executable      string
	args            []string
	driver          *toolkit.Driver
	parameters      map[string]toolkit.Parameterizer
	affectedColumns map[int]string
	ctd             *TransformerDefinition
}

func NewCustomCmdTransformer(
	ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer,
	ctd *TransformerDefinition,
) (*CmdTransformer, toolkit.ValidationWarnings, error) {
	affectedColumns := make(map[int]string)
	affectedColumnsIdx, transferringColumnsIdx, err := toolkit.GetAffectedAndTransferringColumns(parameters, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting affeected and transferring columns: %w", err)
	}
	for _, c := range affectedColumnsIdx {
		affectedColumns[c.Idx] = c.Name
	}

	api, err := toolkit.NewApi(ctd.Driver, transferringColumnsIdx, affectedColumnsIdx, driver)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating InteractionApi: %w", err)
	}

	cct := utils.NewCmdTransformerBase(ctd.Name, ctd.ExpectedExitCode, ctd.RowTransformationTimeout, driver, api)

	ct := &CmdTransformer{
		CmdTransformerBase: cct,
		executable:         ctd.Executable,
		args:               ctd.Args,
		driver:             driver,
		parameters:         parameters,
		affectedColumns:    affectedColumns,
		name:               ctd.Name,
		ctd:                ctd,
	}

	var warnings toolkit.ValidationWarnings
	if ctd.Validate {
		warnings, err = ct.Validate(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("error validating transformer: %w", err)
		}
	}

	return ct, warnings, nil
}

func (ct *CmdTransformer) GetAffectedColumns() map[int]string {
	return ct.affectedColumns
}

func (ct *CmdTransformer) Init(ctx context.Context) (err error) {

	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, TransformArgName)
	if err = ct.BaseInit(ct.executable, args); err != nil {
		return err
	}

	ct.initStderrHandler()
	ct.StartSupervision(ctx)

	// If the handshake or any initialisation step fails, the process must be reaped
	// before the table pipeline is created: a dead transformer session must never be
	// exposed to the table reading stage.
	defer func() {
		if err != nil {
			if doneErr := ct.BaseDoneWithContext(ctx); doneErr != nil {
				log.Debug().Err(doneErr).Msg("error cleaning up transformer after failed init")
			}
		}
	}()

	if err = ct.sendMetadata(ctx); err != nil {
		return fmt.Errorf("error sending metadata: %w", err)
	}

	// The table processing starts only after the command has acknowledged its readiness,
	// so the per-table process ownership and the record sequence are fixed upfront.
	if err = ct.ReadHandshake(ctx, DefaultHandshakeTimeout); err != nil {
		return err
	}

	return nil
}

// Transform - performs one strict request/response roundtrip over the framed protocol.
// The table-owned monotonic sequence is fixed by CmdTransformerBase.ExchangeEnvelope.
func (ct *CmdTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	rd, err := ct.Api.GetRowDriverFromRecord(r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error getting dto: %w", err)
	}

	line, err := ct.Api.EncodeRow(rd)
	if err != nil {
		return nil, fmt.Errorf("interaction api error: cannot encode tuple for transformer: %w", err)
	}

	respLine, err := ct.ExchangeEnvelope(ctx, line)
	if err != nil {
		return nil, err
	}

	rd, err = ct.Api.DecodeRow(respLine)
	if err != nil {
		return nil, ct.Fail(fmt.Errorf("interaction api error: cannot parse transformed tuple: %w", err))
	}

	if err = ct.Api.SetRowDriverToRecord(rd, r); err != nil {
		return nil, ct.Fail(fmt.Errorf("interaction api error: error setting transformed data to record: %w", err))
	}

	ct.Api.Clean()

	return r, nil
}

func (ct *CmdTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {

	args := make([]string, len(ct.args))
	copy(args, ct.args)
	args = append(args, ValidateArgName)

	ctx, cancel := context.WithTimeout(ctx, ct.ctd.ValidationTimeout)
	defer cancel()
	err := ct.BaseInitWithContext(ctx, ct.executable, args)
	if err != nil {
		return nil, fmt.Errorf("transformer initialisation error: %w", err)
	}

	ct.initStderrHandler()
	ct.StartSupervision(ctx)

	if err = ct.sendMetadata(ctx); err != nil {
		if doneErr := ct.BaseDoneWithContext(ctx); doneErr != nil {
			log.Debug().Err(doneErr).Msg("error cleaning up transformer after failed metadata send")
		}
		return nil, fmt.Errorf("error sending metadata: %w", err)
	}

	var warnings toolkit.ValidationWarnings

	for {
		line, err := ct.ReceiveStdoutLine(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				break
			}
			log.Debug().Err(err).Msg("line reader error")
			if doneErr := ct.BaseDoneWithContext(ctx); doneErr != nil {
				log.Debug().Err(doneErr).Msg("error cleaning up transformer after validation failure")
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, ErrValidationTimeout
			}
			return nil, err
		}

		vw := toolkit.NewValidationWarning()
		if err := json.Unmarshal(line, &vw); err != nil {
			log.Warn().
				Err(err).
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Str("Data", string(line)).
				Msg("error unmarshalling ValidationWarning")
			vw = toolkit.NewValidationWarning().
				AddMeta("Payload", string(line)).
				SetSeverity(toolkit.ErrorValidationSeverity).
				SetMsg("error unmarshalling validation warning")
		}
		warnings = append(warnings, vw)
	}

	cleanupErr := ct.BaseDoneWithContext(ctx)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		log.Warn().
			Dur("ValidationTimeout", ct.ctd.ValidationTimeout).
			Str("TableSchema", ct.driver.Table.Schema).
			Str("TableName", ct.driver.Table.Name).
			Str("TransformerName", ct.name).
			Msg("validation timeout")
		return nil, ErrValidationTimeout
	}

	var cause error
	if err := ct.ExitError(); err != nil {
		cause = fmt.Errorf("transformer exited with error: %w", err)
	}
	if cause == nil && cleanupErr != nil {
		cause = cleanupErr
	}
	if cause != nil {
		return nil, ct.WithStderr(cause)
	}

	return warnings, nil
}

// Done - finishes the transformer session. Even when every record has been processed
// successfully a non-zero unexpected exit status or extra unconsumed output still fails
// the table and prevents TOC/metadata from being published. Cleanup errors never replace
// the first recorded failure.
func (ct *CmdTransformer) Done(ctx context.Context) error {
	cleanupErr := ct.BaseDoneWithContext(ctx)

	var cause error
	if err := ct.FirstError(); err != nil {
		cause = err
	}
	if err := ct.ExitError(); err != nil {
		cause = errors.Join(cause, fmt.Errorf("transformer terminated with error: %w", err))
	}
	if tail := ct.TrailingOutput(); len(tail) > 0 {
		cause = errors.Join(cause,
			fmt.Errorf("transformer produced unexpected extra output after the last consumed record: %q",
				string(tail)))
	}
	if cause == nil && cleanupErr != nil {
		cause = fmt.Errorf("error terminating custom transformer: %w", cleanupErr)
	} else if cleanupErr != nil {
		cause = errors.Join(cause, fmt.Errorf("cleanup error: %w", cleanupErr))
	}

	return ct.WithStderr(cause)
}

func (ct *CmdTransformer) getMetadata() ([]byte, error) {
	staticParamValues := make(toolkit.StaticParameters)
	dynamicParamValues := make(map[string]*toolkit.DynamicParamValue)
	for name, p := range ct.parameters {
		switch v := p.(type) {
		case *toolkit.StaticParameter:
			rawValue, err := p.RawValue()
			if err != nil {
				return nil, fmt.Errorf("error getting raw value from parameter \"%s\": %w", p.GetDefinition().Name, err)
			}
			staticParamValues[name] = rawValue
		case *toolkit.DynamicParameter:
			dynamicParamValues[p.GetDefinition().Name] = v.DynamicValue
		}
	}
	meta := &toolkit.Meta{
		Table: ct.driver.Table,
		Parameters: &toolkit.Parameters{
			Static:  staticParamValues,
			Dynamic: dynamicParamValues,
		},
		Types: ct.driver.CustomTypes,
	}
	res, err := json.Marshal(&meta)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal metadata: %w", err)
	}
	return res, nil
}

// initStderrHandler - forwards stderr lines to the log while the base supervision pump
// retains them for the failure report. This is required for convenient verbosity of
// output: print "stderr forwarding" message at most once per 500ms, otherwise each line
// would be covered with the message and tracebacks or multiline messages would be hard
// to recognize.
func (ct *CmdTransformer) initStderrHandler() {
	t := time.NewTicker(500 * time.Millisecond)
	lineNum := 0
	var mx sync.Mutex
	ct.StderrHandler = func(line []byte) {
		mx.Lock()
		defer mx.Unlock()

		if lineNum == 0 {
			log.Warn().
				Str("TableSchema", ct.driver.Table.Schema).
				Str("TableName", ct.driver.Table.Name).
				Str("TransformerName", ct.name).
				Int("TransformerPid", ct.Cmd.Process.Pid).
				Msg("stderr forwarding")
		}
		fmt.Printf("\tDATA: %s\n", string(line))

		select {
		case <-t.C:
			lineNum = 0
		default:
		}
		lineNum++
	}
}

func (ct *CmdTransformer) sendMetadata(ctx context.Context) error {
	meta, err := ct.getMetadata()
	if err != nil {
		return err
	}
	log.Debug().
		RawJSON("Meta", meta).
		Str("TableSchema", ct.driver.Table.Schema).
		Str("TableName", ct.driver.Table.Name).
		Str("TransformerName", ct.name).
		Msg("running transformer with metadata")

	meta = append(meta, '\n')
	if err = ct.WriteLine(ctx, meta); err != nil {
		return fmt.Errorf("error writing metadata to stdin: %w", err)
	}
	return nil
}
