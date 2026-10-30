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

package utils

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	// shutdownTimeout - time to wait for a graceful process exit after stdin EOF before
	// signalling the process group.
	shutdownTimeout = 5 * time.Second
	// sigKillTimeout - grace period between SIGTERM and SIGKILL of the whole process group.
	sigKillTimeout = 2 * time.Second
	// maxStderrBytes - maximum amount of stderr kept for error reporting.
	maxStderrBytes = 64 * 1024
)

var (
	ErrRowTransformationTimeout = errors.New("row transformation timeout")
	ErrTransformerSessionClosed = errors.New("transformer session is closed")
)

type CancelFunction func() error

type CmdTransformerBase struct {
	Name                     string
	Cmd                      *exec.Cmd
	ExpectedExitCode         int
	Driver                   *toolkit.Driver
	Api                      toolkit.InteractionApi
	ProcessedLines           int
	RowTransformationTimeout time.Duration

	StdoutReader *bufio.Reader
	StderrReader *bufio.Reader
	StdinWriter  io.Writer
	Cancel       CancelFunction

	// StderrHandler - optional callback invoked for every line received from the process
	// stderr while the session is supervised.
	StderrHandler func(line []byte)

	stdinCloser io.Closer
	pipeStdout  io.Closer
	pipeStderr  io.Closer

	eg            *errgroup.Group
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
	exited        chan struct{}
	opWg          sync.WaitGroup
	shutdownOnce  sync.Once

	mu               sync.Mutex
	supervisionReady bool
	terminated       bool
	shutdownDone     bool
	waitErr          error
	firstErr         error
	stderrData       bytes.Buffer
	trailingOutput   []byte

	seq int64
}

func NewCmdTransformerBase(
	name string,
	expectedExitCode int,
	rowTransformationTimeout time.Duration,
	driver *toolkit.Driver,
	api toolkit.InteractionApi,
) *CmdTransformerBase {
	if api == nil {
		panic("api is nil")
	}

	return &CmdTransformerBase{
		Name:                     name,
		ExpectedExitCode:         expectedExitCode,
		Driver:                   driver,
		Api:                      api,
		ProcessedLines:           -1,
		RowTransformationTimeout: rowTransformationTimeout,
	}
}

// BaseDone - graceful shutdown with a background context. See BaseDoneWithContext.
func (ctb *CmdTransformerBase) BaseDone() error {
	return ctb.BaseDoneWithContext(context.Background())
}

// BaseDoneWithContext - closes stdin, waits for the process to exit and reaps all the
// supervision goroutines and in-flight operations. If the process keeps running after
// stdin closure, gets cancelled by the context or the grace timeout expires, the whole
// process group is terminated (SIGTERM followed by SIGKILL).
// Errors returned from this method describe only the cleanup itself (pipe/signal errors);
// business failures (transform errors, unexpected exit code, protocol errors) must be
// obtained via FirstError/ExitError/WithStderr so that cleanup never masks the cause.
func (ctb *CmdTransformerBase) BaseDoneWithContext(ctx context.Context) error {
	var cleanupErr error
	ctb.shutdownOnce.Do(func() {
		tailCh := make(chan []byte, 1)
		if ctb.StdoutReader != nil && ctb.supervisionReady {
			// Drain any output produced after the last consumed record, so that a command
			// emitting extra lines cannot hide it. The drain ends when stdout is closed.
			go func() {
				data, readErr := io.ReadAll(ctb.StdoutReader)
				if readErr != nil {
					log.Debug().Err(readErr).Msg("error draining transformer stdout during shutdown")
				}
				tailCh <- data
			}()
		} else {
			close(tailCh)
		}

		if ctb.stdinCloser != nil && !ctb.isTerminated() {
			// Signal end of input first: well-behaved commands finish and exit with their
			// real exit code, which must not be masked by an externally sent signal.
			if err := ctb.stdinCloser.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("error closing transformer stdin: %w", err))
			}
		}

		if ctb.exited != nil {
			timer := time.NewTimer(shutdownTimeout)
			select {
			case <-ctb.exited:
			case <-ctx.Done():
				ctb.terminate()
			case <-timer.C:
				log.Debug().
					Str("TableSchema", ctb.Driver.Table.Schema).
					Str("TableName", ctb.Driver.Table.Name).
					Str("TransformerName", ctb.Name).
					Msg("transformer has not exited after stdin closure, terminating the process")
				ctb.terminate()
			}
			timer.Stop()
			<-ctb.exited
		}

		// All in-flight pipe operations are unblocked by the process termination.
		ctb.opWg.Wait()
		if ctb.eg != nil {
			if err := ctb.eg.Wait(); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		data := <-tailCh
		ctb.mu.Lock()
		ctb.trailingOutput = bytes.TrimSpace(data)
		ctb.shutdownDone = true
		ctb.mu.Unlock()

		if ctb.stdinCloser != nil {
			if err := ctb.stdinCloser.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("error closing transformer stdin: %w", err))
			}
		}
		if ctb.pipeStdout != nil {
			if err := ctb.pipeStdout.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				log.Debug().Err(err).Msg("error closing transformer stdout pipe")
			}
		}
		if ctb.pipeStderr != nil {
			if err := ctb.pipeStderr.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				log.Debug().Err(err).Msg("error closing transformer stderr pipe")
			}
		}
	})
	return cleanupErr
}

// Transform - perform one synchronous raw line request/response roundtrip. Used by the
// built-in Cmd transformer, whose wire protocol is one raw line in - one raw line out.
func (ctb *CmdTransformerBase) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	ctb.ProcessedLines++
	ctx, cancel := context.WithTimeout(ctx, ctb.RowTransformationTimeout)
	defer cancel()

	rd, err := ctb.Api.GetRowDriverFromRecord(r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error getting dto: %w", err)
	}

	line, err := ctb.Api.EncodeRow(rd)
	if err != nil {
		return nil, fmt.Errorf("interaction api error: cannot encode tuple for transformer: %w", err)
	}
	line = append(line, '\n')
	if err = ctb.writeLine(ctx, line); err != nil {
		return nil, ctb.Fail(fmt.Errorf("interaction api error: cannot send tuple to transformer: %w", err))
	}

	respLine, err := ctb.readLine(ctx)
	if err != nil {
		return nil, ctb.Fail(fmt.Errorf("interaction api error: cannot receive transformed tuple from transformer: %w", err))
	}

	rd, err = ctb.Api.DecodeRow(respLine)
	if err != nil {
		return nil, ctb.Fail(fmt.Errorf("interaction api error: cannot parse transformed tuple: %w", err))
	}

	if err = ctb.Api.SetRowDriverToRecord(rd, r); err != nil {
		return nil, ctb.Fail(fmt.Errorf("interaction api error: error setting transformed data to record: %w", err))
	}

	ctb.Api.Clean()

	return r, nil
}

// ExchangeEnvelope - send one framed record with the table-owned monotonic sequence and
// accept exactly one reply carrying the same sequence. Any mismatch, truncated line or
// process death fails the whole table session.
func (ctb *CmdTransformerBase) ExchangeEnvelope(ctx context.Context, payload []byte) (respPayload []byte, err error) {
	ctx, cancel := context.WithTimeout(ctx, ctb.RowTransformationTimeout)
	defer cancel()

	if err = ctb.sessionError(); err != nil {
		return nil, ctb.Fail(err)
	}

	seq := ctb.seq
	line, err := toolkit.NewRecordMessageLine(seq, payload)
	if err != nil {
		return nil, ctb.Fail(err)
	}
	if err = ctb.writeLine(ctx, line); err != nil {
		return nil, ctb.Fail(fmt.Errorf("error sending record seq=%d to transformer: %w", seq, err))
	}

	respLine, err := ctb.readLine(ctx)
	if err != nil {
		return nil, ctb.Fail(fmt.Errorf("error receiving transformed record seq=%d from transformer: %w", seq, err))
	}

	respSeq, respPayload, err := toolkit.ParseRecordMessage(respLine)
	if err != nil {
		return nil, ctb.Fail(fmt.Errorf("protocol error for record seq=%d: %w", seq, err))
	}
	if respSeq != seq {
		return nil, ctb.Fail(fmt.Errorf("protocol error for record seq=%d: received out-of-order response with seq=%d", seq, respSeq))
	}

	ctb.seq++
	return respPayload, nil
}

// ReadHandshake - wait for the ready acknowledgement of the external command.
func (ctb *CmdTransformerBase) ReadHandshake(ctx context.Context, timeout time.Duration) error {
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	line, err := ctb.readLine(hctx)
	if err != nil {
		return ctb.Fail(fmt.Errorf("transformer handshake error: %w", err))
	}
	if err = toolkit.ParseReadyMessage(line); err != nil {
		return ctb.Fail(fmt.Errorf("transformer handshake error: %w", err))
	}
	return nil
}

// StartSupervision - launch the process reaper and stderr pump. The session context is
// cancelled as soon as the process exits, so every table stage stops promptly.
func (ctb *CmdTransformerBase) StartSupervision(ctx context.Context) {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	if ctb.supervisionReady {
		return
	}
	ctb.exited = make(chan struct{})
	ctb.sessionCtx, ctb.sessionCancel = context.WithCancel(ctx)
	ctb.eg = &errgroup.Group{}
	ctb.eg.Go(ctb.waitPump)
	ctb.eg.Go(ctb.stderrPump)
	ctb.supervisionReady = true
}

// FirstError - the first failure recorded for this transformer session.
func (ctb *CmdTransformerBase) FirstError() error {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	return ctb.firstErr
}

// Seq - current record sequence number (number of successfully acknowledged records).
func (ctb *CmdTransformerBase) Seq() int64 {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	return ctb.seq
}

// ExitError - classifies the process exit status. A process that exited while the table
// was still being processed is always an error, even when the exit code matches the
// expected one (expected_exit_code=-1 accepts any code during shutdown).
func (ctb *CmdTransformerBase) ExitError() error {
	ctb.mu.Lock()
	waitErr := ctb.waitErr
	firstErr := ctb.firstErr
	terminated := ctb.terminated
	shutdownDone := ctb.shutdownDone
	ctb.mu.Unlock()

	if waitErr == nil {
		return nil
	}

	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		code := exitErr.ExitCode()
		// If the session failed or was terminated before the normal shutdown, the process
		// had no chance to report its graceful exit code: surface the status as context.
		if firstErr != nil || (terminated && !shutdownDone) {
			return fmt.Errorf("transformer process exited with code %d during the failed session", code)
		}
		if ctb.ExpectedExitCode != -1 && code != ctb.ExpectedExitCode {
			return fmt.Errorf("unexpected transformer exit code: expected %d received %d",
				ctb.ExpectedExitCode, code)
		}
		return nil
	}

	return fmt.Errorf("transformer exited with error: %w", waitErr)
}

// TrailingOutput - bytes received after the last consumed record.
func (ctb *CmdTransformerBase) TrailingOutput() []byte {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	return ctb.trailingOutput
}

// Stderr - the captured stderr content preserved for diagnostics.
func (ctb *CmdTransformerBase) Stderr() string {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	return strings.TrimSpace(ctb.stderrData.String())
}

// WithStderr - appends the captured stderr to the error unless it is already present.
func (ctb *CmdTransformerBase) WithStderr(err error) error {
	if err == nil {
		return nil
	}
	stderr := ctb.Stderr()
	if stderr == "" || strings.Contains(err.Error(), "stderr:") {
		return err
	}
	return fmt.Errorf("%w\nstderr:\n%s", err, stderr)
}

func (ctb *CmdTransformerBase) BaseInitWithContext(ctx context.Context, executable string, args []string) error {
	log.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("initializing transformer")

	ctb.Cmd = ctb.newCommand(ctx, executable, args)
	if err := ctb.init(); err != nil {
		return err
	}
	if err := ctb.Cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("custom transformer exited with error")

		return fmt.Errorf("external command runtime error: %w", err)
	}
	return nil
}

func (ctb *CmdTransformerBase) BaseInit(executable string, args []string) error {
	log.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("initializing transformer")

	ctb.Cmd = ctb.newCommand(context.Background(), executable, args)
	if err := ctb.init(); err != nil {
		return err
	}
	if err := ctb.Cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("custom transformer exited with error")

		return fmt.Errorf("external command runtime error: %w", err)
	}
	return nil
}

// WriteLine - write a single line (caller controls timeout context), aborting the session
// on failure.
func (ctb *CmdTransformerBase) WriteLine(ctx context.Context, line []byte) error {
	if err := ctb.writeLine(ctx, line); err != nil {
		return ctb.Fail(err)
	}
	return nil
}

// ReceiveStdoutLine - read one stdout line honouring the context. Used by the validation
// protocol only.
func (ctb *CmdTransformerBase) ReceiveStdoutLine(ctx context.Context) (line []byte, err error) {
	type result struct {
		line []byte
		err  error
	}
	ch := make(chan result, 1)
	ctb.opWg.Add(1)
	go func() {
		defer ctb.opWg.Done()
		l, e := reader.ReadLine(ctb.StdoutReader, nil)
		ch <- result{line: l, err: e}
	}()
	select {
	case <-ctx.Done():
		ctb.terminate()
		r := <-ch
		return r.line, ctx.Err()
	case r := <-ch:
		return r.line, r.err
	}
}

func (ctb *CmdTransformerBase) newCommand(ctx context.Context, executable string, args []string) *exec.Cmd {
	var cmd *exec.Cmd
	if ctx != nil {
		cmd = exec.CommandContext(ctx, executable, args...)
	} else {
		cmd = exec.Command(executable, args...)
	}
	// Run the command in its own process group so that the whole process tree can be
	// signalled: terminating only the direct leader leaves grandchildren holding the
	// stdin/stdout pipes and deadlocks the shutdown.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		ctb.terminate()
		return os.ErrProcessDone
	}
	cmd.WaitDelay = sigKillTimeout
	return cmd
}

func (ctb *CmdTransformerBase) init() error {

	var err error
	stderr, err := ctb.Cmd.StderrPipe()
	if err != nil {
		return err
	}
	stdout, err := ctb.Cmd.StdoutPipe()
	if err != nil {
		if err := stderr.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing stderr pipe")
		}
		return err
	}

	stdin, err := ctb.Cmd.StdinPipe()
	if err != nil {
		if err := stderr.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing stderr pipe")
		}
		if err := stdout.Close(); err != nil {
			log.Warn().Err(err).Msg("error closing stdout pipe")
		}
		return err
	}
	ctb.pipeStderr = stderr
	ctb.pipeStdout = stdout
	ctb.stdinCloser = stdin
	ctb.StderrReader = bufio.NewReader(stderr)
	ctb.StdoutReader = bufio.NewReader(stdout)
	ctb.StdinWriter = stdin

	ctb.Api.SetReader(stdout)
	ctb.Api.SetWriter(stdin)

	ctb.Cancel = func() error {
		ctb.terminate()
		return nil
	}

	return nil
}

// fail - records the first session failure (all subsequent failures are ignored as
// consequences), tears down the process so no further record can enter the dead session
// and returns the failure enriched with the process exit status and captured stderr.
func (ctb *CmdTransformerBase) Fail(err error) error {
	if err == nil {
		return nil
	}

	ctb.mu.Lock()
	if ctb.firstErr == nil {
		ctb.firstErr = err
	} else {
		err = ctb.firstErr
	}
	waitErr := ctb.waitErr
	ctb.mu.Unlock()

	// Stop the table session immediately. Other goroutines observe the cancelled
	// session context and the closed pipes.
	ctb.terminate()

	enriched := err
	if waitErr != nil {
		enriched = fmt.Errorf("%w: transformer process ended: %s", err, waitErr.Error())
	}
	if stderr := ctb.Stderr(); stderr != "" && !strings.Contains(enriched.Error(), "stderr:") {
		enriched = fmt.Errorf("%w\nstderr:\n%s", enriched, stderr)
	}
	return enriched
}

func (ctb *CmdTransformerBase) waitPump() error {
	err := ctb.Cmd.Wait()
	ctb.mu.Lock()
	ctb.waitErr = err
	ctb.mu.Unlock()
	if err != nil {
		log.Debug().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Int("TransformerPid", ctb.Cmd.Process.Pid).
			Msg("transformer process has exited")
	} else {
		log.Debug().
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Int("TransformerPid", ctb.Cmd.Process.Pid).
			Msg("transformer exited normally")
	}
	// Cancel the session before signalling waiters so transformers stop before the
	// next record is dispatched.
	ctb.sessionCancel()
	close(ctb.exited)
	return nil
}

func (ctb *CmdTransformerBase) stderrPump() error {
	for {
		line, _, err := ctb.StderrReader.ReadLine()
		if err != nil {
			// EOF means the process closed stderr; any other read error ends the pump as
			// well. This must never overwrite the original transformation failure.
			return nil
		}
		ctb.appendStderr(line)
		if ctb.StderrHandler != nil {
			ctb.StderrHandler(line)
		}
	}
}

func (ctb *CmdTransformerBase) appendStderr(line []byte) {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	if ctb.stderrData.Len() >= maxStderrBytes {
		return
	}
	remaining := maxStderrBytes - ctb.stderrData.Len()
	if len(line) > remaining {
		line = line[:remaining]
	}
	ctb.stderrData.Write(line)
	ctb.stderrData.WriteByte('\n')
}

func (ctb *CmdTransformerBase) sessionError() error {
	if ctb.sessionCtx == nil {
		return fmt.Errorf("%w: supervision is not started", ErrTransformerSessionClosed)
	}
	select {
	case <-ctb.sessionCtx.Done():
		ctb.mu.Lock()
		waitErr := ctb.waitErr
		ctb.mu.Unlock()
		if waitErr != nil {
			return fmt.Errorf("%w: transformer process has exited: %s",
				ErrTransformerSessionClosed, waitErr.Error())
		}
		return ErrTransformerSessionClosed
	default:
		return nil
	}
}

func (ctb *CmdTransformerBase) isTerminated() bool {
	ctb.mu.Lock()
	defer ctb.mu.Unlock()
	return ctb.terminated
}

// terminate - closes stdin and terminates the whole process group (SIGTERM, then SIGKILL
// after the grace period). Idempotent and safe to call from any goroutine.
func (ctb *CmdTransformerBase) terminate() {
	ctb.mu.Lock()
	if ctb.terminated {
		ctb.mu.Unlock()
		return
	}
	ctb.terminated = true
	alreadyExited := ctb.waitErr != nil
	ctb.mu.Unlock()

	if ctb.sessionCancel != nil {
		ctb.sessionCancel()
	}

	if ctb.stdinCloser != nil {
		if err := ctb.stdinCloser.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			log.Debug().
				Err(err).
				Str("TableSchema", ctb.Driver.Table.Schema).
				Str("TableName", ctb.Driver.Table.Name).
				Str("TransformerName", ctb.Name).
				Msg("error closing transformer stdin")
		}
	}

	if alreadyExited || ctb.Cmd == nil || ctb.Cmd.Process == nil {
		return
	}

	pid := ctb.Cmd.Process.Pid
	log.Debug().
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Int("TransformerPid", pid).
		Msg("sending SIGTERM to the transformer process group")
	if err := ctb.signalProcessGroup(syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Debug().Err(err).Int("TransformerPid", pid).Msg("error sending SIGTERM to process group")
	}

	go func() {
		timer := time.NewTimer(sigKillTimeout)
		defer timer.Stop()
		if ctb.exited == nil {
			<-timer.C
			if err := ctb.signalProcessGroup(syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				log.Warn().Err(err).Int("TransformerPid", pid).Msg("error sending SIGKILL to process group")
			}
			return
		}
		select {
		case <-ctb.exited:
		case <-timer.C:
			log.Warn().
				Str("TableSchema", ctb.Driver.Table.Schema).
				Str("TableName", ctb.Driver.Table.Name).
				Str("TransformerName", ctb.Name).
				Int("TransformerPid", pid).
				Msg("transformer process group has not exited, sending SIGKILL")
			if err := ctb.signalProcessGroup(syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				log.Warn().Err(err).Int("TransformerPid", pid).Msg("error sending SIGKILL to process group")
			}
		}
	}()
}

func (ctb *CmdTransformerBase) signalProcessGroup(sig syscall.Signal) error {
	if ctb.Cmd == nil || ctb.Cmd.Process == nil {
		return nil
	}
	// With Setpgid the child becomes the leader of a new process group whose id equals
	// its pid; negative id addresses every process in the group.
	return syscall.Kill(-ctb.Cmd.Process.Pid, sig)
}

func (ctb *CmdTransformerBase) writeLine(ctx context.Context, p []byte) error {
	return ctb.runOp(ctx, func() error {
		_, err := ctb.StdinWriter.Write(p)
		return err
	})
}

func (ctb *CmdTransformerBase) readLine(ctx context.Context) ([]byte, error) {
	var line []byte
	err := ctb.runOp(ctx, func() error {
		var e error
		line, e = ctb.readOneLine()
		return e
	})
	return line, err
}

func (ctb *CmdTransformerBase) readOneLine() ([]byte, error) {
	data, err := ctb.StdoutReader.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) {
			if len(data) > 0 {
				return bytes.TrimSpace(data), fmt.Errorf("received truncated line without newline: %w", io.ErrUnexpectedEOF)
			}
			return nil, io.EOF
		}
		if errors.Is(err, os.ErrClosed) {
			return nil, err
		}
		return nil, fmt.Errorf("error reading line from transformer: %w", err)
	}
	return bytes.TrimSuffix(data, []byte("\n")), nil
}

// runOp - runs a pipe operation with context/timeout while watching the process session.
// On timeout, cancellation or process death it terminates the whole process group to
// unblock the operation goroutine and waits for it to finish - no goroutine leaks.
func (ctb *CmdTransformerBase) runOp(ctx context.Context, op func() error) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrRowTransformationTimeout
		}
		return err
	}
	if err := ctb.sessionError(); err != nil {
		return err
	}

	done := make(chan error, 1)
	ctb.opWg.Add(1)
	go func() {
		defer ctb.opWg.Done()
		done <- op()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		ctb.terminate()
		<-done
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrRowTransformationTimeout
		}
		return ctx.Err()
	case <-ctb.sessionCtx.Done():
		err := <-done
		if err != nil {
			return err
		}
		return ctb.sessionError()
	}
}
