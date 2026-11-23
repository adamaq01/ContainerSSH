package kubernetes

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"go.containerssh.io/containerssh/config"
	"go.containerssh.io/containerssh/internal/metrics"
	"go.containerssh.io/containerssh/log"
	"go.containerssh.io/containerssh/message"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/exec"
)

type kubernetesExecutionImpl struct {
	pod                   *kubernetesPodImpl
	exec                  remotecommand.Executor
	terminalSizeQueue     pushSizeQueue
	logger                log.Logger
	tty                   bool
	pid                   int
	env                   map[string]string
	backendRequestsMetric metrics.SimpleCounter
	backendFailuresMetric metrics.SimpleCounter
	doneChan              chan struct{}
	exited                bool
	lock                  *sync.Mutex
}

func (k *kubernetesExecutionImpl) term(ctx context.Context) {
	select {
	case <-k.done():
		return
	default:
	}
	_ = k.signal(ctx, "TERM")
}

func (k *kubernetesExecutionImpl) kill() {
	select {
	case <-k.done():
		return
	default:
	}
	_ = k.signal(context.Background(), "KILL")
}

func (k *kubernetesExecutionImpl) done() <-chan struct{} {
	return k.doneChan
}

func (k *kubernetesExecutionImpl) signal(ctx context.Context, sig string) error {
	if k.pod.config.Pod.DisableAgent {
		err := message.UserMessage(
			message.EKubernetesCannotSendSignalNoAgent,
			"Cannot send signal to process.",
			"Cannot send signal %s to process because the ContainerSSH agent is disabled",
			sig,
		).Label("signal", sig)
		k.logger.Debug(err)
		return err
	}
	k.lock.Lock()

	if k.pid <= 0 {
		k.lock.Unlock()
		return message.UserMessage(message.EKubernetesFailedSignalNoPID, "Cannot send signal to process", "could not send signal to exec, process ID not found")
	}
	if k.exited {
		k.lock.Unlock()
		return message.UserMessage(message.EKubernetesFailedSignalExited, "Cannot send signal to process", "could not send signal to exec, process already exited")
	}

	if k.pod.shutdown {
		err := message.UserMessage(
			message.EKubernetesFailedExecSignal,
			"Cannot send signal to process.",
			"Not sending signal to process, pod is already shutting down.",
		).Label("signal", sig)
		k.logger.Debug(err)
		return err
	}
	k.pod.wg.Add(1)
	pid := k.pid
	k.lock.Unlock()
	if pid < 1 {
		k.pod.wg.Done()
		return k.logAndReturnNonPositivePidOnSignal(sig)
	}

	k.logger.Debug(
		message.NewMessage(
			message.MKubernetesExecSignal,
			"Using the exec facility to send signal %s to pid %d...",
			sig,
			pid,
		).Label("signal", sig),
	)
	err := k.processSignalExec(ctx, sig, pid)
	if err != nil {
		err = message.Wrap(
			err,
			message.EKubernetesFailedExecSignal,
			"Cannot send %s signal to pod %s pid %d",
			sig, k.pod.pod.Name, pid,
		).Label("signal", sig)
		k.logger.Error(
			err,
		)
	} else {
		k.logger.Debug(
			message.NewMessage(
				message.MKubernetesExecSignalSuccessful,
				"Sent %s signal to pod %s pid %d",
				sig, k.pod.pod.Name, pid,
			).Label("signal", sig),
		)
	}
	return err
}

func (k *kubernetesExecutionImpl) logAndReturnNonPositivePidOnSignal(sig string) error {
	err := message.UserMessage(
		message.EKubernetesFailedExecSignal,
		"Cannot send signal to process.",
		"No process ID recorded, not sending signal.",
	).Label("signal", sig)
	k.logger.Debug(err)
	return err
}

func (k *kubernetesExecutionImpl) processSignalExec(ctx context.Context, sig string, pid int) error {
	podExec, err := k.pod.createExecLocked(
		ctx, []string{
			k.pod.config.Pod.AgentPath,
			"signal",
			"--pid",
			strconv.Itoa(pid),
			"--signal",
			sig,
		}, map[string]string{}, false,
	)
	if err != nil {
		k.pod.wg.Done()
		return err
	}
	var stdoutBytes bytes.Buffer
	var stderrBytes bytes.Buffer
	stdin, stdinWriter := io.Pipe()
	done := make(chan struct{})
	podExec.run(
		stdin, &stdoutBytes, &stderrBytes, func() error {
			return nil
		},
		func(exitStatus int) {
			if exitStatus != 0 {
				k.backendFailuresMetric.Increment()
				err = fmt.Errorf("non-zero exit status (%d)", exitStatus)
			}
			done <- struct{}{}
		},
	)
	<-done
	_ = stdinWriter.Close()
	return err
}

func (k *kubernetesExecutionImpl) resize(ctx context.Context, height uint, width uint) error {
	k.logger.Debug(
		message.NewMessage(message.MKubernetesResizing, "Resizing window to %dx%d", width, height).
			Label("width", width).
			Label("height", height))
	return k.terminalSizeQueue.Push(
		ctx,
		remotecommand.TerminalSize{
			Width:  uint16(width),
			Height: uint16(height),
		},
	)
}

type stdinProxyReader struct {
	backend      io.Reader
	startWritten bool
	lock         *sync.Mutex
	tty          bool
}

func (s *stdinProxyReader) Read(b []byte) (n int, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.startWritten {
		return s.backend.Read(b)
	}
	if len(b) == 0 {
		return 0, nil
	}
	s.startWritten = true
	if s.tty {
		b[0] = '\n'
	}
	return 1, nil
}

// pidGatedReader holds input back until the agent has reported the PID. A terminal echoes input,
// so input arriving earlier would be echoed in front of the PID and read in its place.
type pidGatedReader struct {
	backend     io.Reader
	pidReported <-chan struct{}
	done        <-chan struct{}
}

func (g *pidGatedReader) Read(b []byte) (int, error) {
	select {
	case <-g.pidReported:
		return g.backend.Read(b)
	case <-g.done:
		return 0, io.EOF
	}
}

type stdoutProxyWriter struct {
	backend     io.Writer
	pidChannel  chan uint32
	pidReported chan struct{}
	pidRead     bool
	lock        *sync.Mutex
	buf         *bytes.Buffer
	tty         bool
	session     bool
}

func (s *stdoutProxyWriter) Write(p []byte) (n int, err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.pidRead {
		return s.backend.Write(p)
	}
	// Read the pid. See https://github.com/containerssh/agent for details.
	if n, err := s.buf.Write(p); err != nil {
		return n, err
	}
	pidBytes, consumed, ok := s.parsePID(s.buf.Bytes())
	if !ok {
		return len(p), nil
	}
	remainingBytes := s.buf.Bytes()[consumed:]
	s.pidRead = true
	s.buf = nil
	s.pidChannel <- binary.LittleEndian.Uint32(pidBytes)
	close(s.pidReported)
	if len(remainingBytes) > 0 {
		_, err := s.backend.Write(remainingBytes)
		return len(p), err
	}
	return len(p), nil
}

// parsePID returns the 4 PID bytes at the start of buf and how many bytes of buf they take up, or
// false when buf does not hold them all yet.
func (s *stdoutProxyWriter) parsePID(buf []byte) ([]byte, int, bool) {
	if s.session {
		// We need 6 bytes because the first 2 bytes will be \r\n from the --wait function injected in the reader above.
		if (s.tty && len(buf) >= 6) || (!s.tty && len(buf) >= 4) {
			return buf[:4], 4, true
		}
		return nil, 0, false
	}
	if !s.tty {
		if len(buf) >= 4 {
			return buf[:4], 4, true
		}
		return nil, 0, false
	}
	// A terminal's output processing (ONLCR) writes every \n as \r\n, a 0x0a byte of the PID included.
	pid := make([]byte, 0, 4)
	i := 0
	for len(pid) < 4 {
		if i >= len(buf) || (buf[i] == '\r' && i+1 >= len(buf)) {
			return nil, 0, false
		}
		if buf[i] == '\r' && buf[i+1] == '\n' {
			i++
		}
		pid = append(pid, buf[i])
		i++
	}
	return pid, i, true
}

func (k *kubernetesExecutionImpl) run(
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	closeWrite func() error,
	onExit func(exitStatus int),
) {
	pidChannel := make(chan uint32)
	if !k.pod.config.Pod.DisableAgent {
		session := k.pod.config.Pod.Mode == config.KubernetesExecutionModeSession
		pidReported := make(chan struct{})
		if session {
			stdin = &stdinProxyReader{
				backend: stdin,
				lock:    &sync.Mutex{},
				tty:     k.tty,
			}
		} else if k.tty {
			stdin = &pidGatedReader{
				backend:     stdin,
				pidReported: pidReported,
				done:        k.doneChan,
			}
		}
		stdout = &stdoutProxyWriter{
			tty:         k.tty,
			session:     session,
			backend:     stdout,
			lock:        &sync.Mutex{},
			pidChannel:  pidChannel,
			pidReported: pidReported,
			buf:         &bytes.Buffer{},
		}
	}
	go k.handleStream(stdin, stdout, stderr, closeWrite, onExit)
	if !k.pod.config.Pod.DisableAgent {
		pid := <-pidChannel
		k.logger.Debug(
			message.NewMessage(
				message.MKubernetesPIDReceived,
				"Received PID %d from agent",
				pid,
			))
		k.pid = int(pid)
	}
}

func (k *kubernetesExecutionImpl) handleStream(
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	closeWrite func() error,
	onExit func(exitStatus int),
) {
	var tty bool
	if k.pod.config.Pod.Mode == config.KubernetesExecutionModeSession {
		tty = *k.pod.tty
	} else {
		tty = k.tty
	}
	k.backendRequestsMetric.Increment()
	err := k.exec.StreamWithContext(context.Background(),
		remotecommand.StreamOptions{
			Stdin:             stdin,
			Stdout:            stdout,
			Stderr:            stderr,
			Tty:               tty,
			TerminalSizeQueue: k.terminalSizeQueue,
		},
	)
	k.exited = true
	close(k.doneChan)
	_ = closeWrite()
	k.terminalSizeQueue.Stop()
	if k.pod.config.Pod.Mode == config.KubernetesExecutionModeConnection {
		k.pod.wg.Done()
	}
	if err != nil {
		exitErr := &exec.CodeExitError{}
		if errors.As(err, exitErr) {
			onExit(exitErr.Code)
		} else {
			k.sendExitCodeToClient(onExit)
		}
	} else if k.pod.config.Pod.Mode == config.KubernetesExecutionModeConnection {
		onExit(0)
	} else {
		k.sendExitCodeToClient(onExit)
	}
}

func (k *kubernetesExecutionImpl) sendExitCodeToClient(onExit func(exitStatus int)) {
	ctx, cancel := context.WithTimeout(context.Background(), k.pod.config.Timeouts.PodStop)
	defer cancel()
	exitCode, err := k.pod.getExitCode(ctx)
	if err == nil {
		onExit(int(exitCode))
	} else {
		onExit(137)
	}
}
