package kubernetes

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeThroughProxy(t *testing.T, tty bool, chunks ...string) (uint32, string) {
	t.Helper()
	var out bytes.Buffer
	pids := make(chan uint32, 1)
	pidChannel := make(chan uint32)
	go func() { pids <- <-pidChannel }()
	w := &stdoutProxyWriter{
		backend:     &out,
		pidChannel:  pidChannel,
		pidReported: make(chan struct{}),
		lock:        &sync.Mutex{},
		buf:         &bytes.Buffer{},
		tty:         tty,
	}
	for _, chunk := range chunks {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case pid := <-pids:
		return pid, out.String()
	case <-time.After(time.Second):
		t.Fatalf("no PID read from %q", chunks)
		return 0, ""
	}
}

func TestPIDOnATerminalIsReadWithoutTheProgramOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		chunks []string
		pid    uint32
		output string
	}{
		{"plain", []string{"|\x00\x00\x00$ "}, 124, "$ "},
		{"before any output", []string{"|\x00\x00\x00"}, 124, ""},
		{"a low 0x0a byte", []string{"\r\n\x12\x00\x00$ "}, 0x120a, "$ "},
		{"a high 0x0a byte", []string{"\x02\r\n\x00\x00$ "}, 0x0a02, "$ "},
		{"split inside the \\r\\n", []string{"\x02\r", "\n\x00\x00$ "}, 0x0a02, "$ "},
		{"a 0x0d byte", []string{"\r\x01\x00\x00\r\n$ "}, 0x010d, "\r\n$ "},
	} {
		t.Run(test.name, func(t *testing.T) {
			pid, output := writeThroughProxy(t, true, test.chunks...)
			if pid != test.pid || output != test.output {
				t.Fatalf("got PID %#x and output %q, want %#x and %q", pid, output, test.pid, test.output)
			}
		})
	}
}

func TestPIDWithoutATerminalIsTheFirstFourBytes(t *testing.T) {
	pid, output := writeThroughProxy(t, false, "\n\x00\x00\x00\r\n")
	if pid != 10 || output != "\r\n" {
		t.Fatalf("got PID %d and output %q", pid, output)
	}
}

func TestTerminalInputWaitsForThePID(t *testing.T) {
	pidReported := make(chan struct{})
	r := &pidGatedReader{backend: strings.NewReader("ls\r"), pidReported: pidReported, done: make(chan struct{})}
	read := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		read <- string(b)
	}()
	select {
	case got := <-read:
		t.Fatalf("input %q passed before the PID", got)
	case <-time.After(100 * time.Millisecond):
	}
	close(pidReported)
	if got := <-read; got != "ls\r" {
		t.Fatalf("got %q", got)
	}
}

func TestTerminalInputEndsWhenTheProgramEndsWithoutAPID(t *testing.T) {
	done := make(chan struct{})
	close(done)
	r := &pidGatedReader{backend: strings.NewReader("ls\r"), pidReported: make(chan struct{}), done: done}
	if n, err := r.Read(make([]byte, 8)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("got %d, %v", n, err)
	}
}
