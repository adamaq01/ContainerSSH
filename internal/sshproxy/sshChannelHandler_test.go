package sshproxy_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"testing"
	"time"

	"go.containerssh.io/containerssh/config"
	"go.containerssh.io/containerssh/internal/geoip/dummy"
	"go.containerssh.io/containerssh/internal/metrics"
	"go.containerssh.io/containerssh/internal/sshproxy"
	"go.containerssh.io/containerssh/internal/sshserver"
	"go.containerssh.io/containerssh/internal/structutils"
	"go.containerssh.io/containerssh/log"
	"go.containerssh.io/containerssh/metadata"
	"golang.org/x/crypto/ssh"
)

func TestOutputIsDeliveredWhenClientStdinIsClosed(t *testing.T) {
	output := []byte("Hello world!\n")
	client := connectThroughProxy(t, func(channel ssh.Channel) {
		_, _ = io.Copy(io.Discard, channel)
		// Take a moment before writing, like a real program, so that a close sent along with stdin EOF arrives first.
		time.Sleep(200 * time.Millisecond)
		_, _ = channel.Write(output)
		exit(channel)
	})
	session, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}

	got, err := session.Output("program")
	if err != nil {
		t.Fatalf("program failed (%v), output: %q", err, got)
	}
	if !bytes.Equal(got, output) {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestBufferedOutputIsDeliveredWhenBackendCloses(t *testing.T) {
	streams := map[string]struct {
		backend func(ssh.Channel) io.Writer
		client  func(*ssh.Session) (io.Reader, error)
	}{
		"stdout": {func(channel ssh.Channel) io.Writer { return channel }, (*ssh.Session).StdoutPipe},
		"stderr": {func(channel ssh.Channel) io.Writer { return channel.Stderr() }, (*ssh.Session).StderrPipe},
	}
	for name, stream := range streams {
		t.Run(name, func(t *testing.T) {
			output := bytes.Repeat([]byte("0123456789abcdef"), 3<<20/16)
			backendClosed := make(chan struct{})
			client := connectThroughProxy(t, func(channel ssh.Channel) {
				_, _ = stream.backend(channel).Write(output)
				exit(channel)
				close(backendClosed)
			})
			session, err := client.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			reader, err := stream.client(session)
			if err != nil {
				t.Fatal(err)
			}
			// Hold stdin open so that only the backend ends the session.
			if _, err := session.StdinPipe(); err != nil {
				t.Fatal(err)
			}
			if err := session.Start("program"); err != nil {
				t.Fatal(err)
			}

			// The output exceeds the client's channel window, so part of it is still in the proxy when the backend
			// closes. Read only after the proxy has had time to act on that close.
			<-backendClosed
			time.Sleep(200 * time.Millisecond)
			got, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Wait(); err != nil {
				t.Fatalf("program failed (%v)", err)
			}
			if !bytes.Equal(got, output) {
				t.Fatalf("received %d of %d bytes", len(got), len(output))
			}
		})
	}
}

func exit(channel ssh.Channel) {
	_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
	_ = channel.Close()
}

func connectThroughProxy(t *testing.T, program func(ssh.Channel)) *ssh.Client {
	t.Helper()
	logger := log.NewTestLogger(t)
	collector := metrics.New(dummy.New())
	proxy, err := sshproxy.New(
		net.TCPAddr{IP: net.ParseIP("127.0.0.1")},
		sshserver.GenerateConnectionID(),
		startBackend(t, program),
		logger,
		collector.MustCreateCounter("backend_requests", "", ""),
		collector.MustCreateCounter("backend_failures", "", ""),
	)
	if err != nil {
		t.Fatal(err)
	}

	user := sshserver.NewTestUser("test")
	user.RandomPassword()
	srv := sshserver.NewTestServer(
		t,
		sshserver.NewTestAuthenticationHandler(&proxyHandler{backend: proxy}, user),
		logger,
		nil,
	)
	srv.Start()
	hostKey, err := ssh.ParsePrivateKey([]byte(srv.GetHostKey()))
	if err != nil {
		t.Fatal(err)
	}
	client, err := ssh.Dial("tcp", srv.GetListen(), &ssh.ClientConfig{
		User:            user.Username(),
		Auth:            user.GetAuthMethods(),
		HostKeyCallback: ssh.FixedHostKey(hostKey.PublicKey()),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type proxyHandler struct {
	sshserver.AbstractHandler

	backend sshserver.NetworkConnectionHandler
}

func (h *proxyHandler) OnNetworkConnection(
	meta metadata.ConnectionMetadata,
) (sshserver.NetworkConnectionHandler, metadata.ConnectionMetadata, error) {
	return h.backend, meta, nil
}

func startBackend(t *testing.T, program func(ssh.Channel)) config.SSHProxyConfig {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return nil, nil
		},
	}
	serverConfig.AddHostKey(hostKey)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveBackendConnection(conn, serverConfig, program)
		}
	}()

	cfg := config.SSHProxyConfig{}
	structutils.Defaults(&cfg)
	cfg.Server = "127.0.0.1"
	cfg.Port = uint16(listener.Addr().(*net.TCPAddr).Port)
	cfg.Username = "test"
	cfg.AllowedHostKeyFingerprints = []string{ssh.FingerprintSHA256(hostKey.PublicKey())}
	return cfg
}

func serveBackendConnection(conn net.Conn, serverConfig *ssh.ServerConfig, program func(ssh.Channel)) {
	_, channels, requests, err := ssh.NewServerConn(conn, serverConfig)
	if err != nil {
		return
	}
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go func() {
			for request := range channelRequests {
				_ = request.Reply(request.Type == "exec", nil)
				if request.Type == "exec" {
					go program(channel)
				}
			}
		}()
	}
}
