package mobile

import (
	"io"
	"log"
	"net"
	"sync"

	"github.com/xjasonlyu/tun2socks/v2/core"
	"github.com/xjasonlyu/tun2socks/v2/core/adapter"
	"github.com/xjasonlyu/tun2socks/v2/core/device/iobased"
)

var (
	tunOnce      sync.Once
	activeClient *Client
	pipe         *swiftPipe
)

// PacketWriter is an interface for Swift to receive packets asynchronously.
type PacketWriter interface {
	WritePacket(packet []byte)
}

var globalPacketWriter PacketWriter

// SetPacketWriter sets the Swift callback for outbound packets.
func SetPacketWriter(writer PacketWriter) {
	globalPacketWriter = writer
}

// swiftPipe bridges Swift byte array boundaries into a standard io.ReadWriter
type swiftPipe struct {
	in  chan []byte
	out chan []byte
}

func newSwiftPipe() *swiftPipe {
	return &swiftPipe{
		in:  make(chan []byte, 1024),
		out: make(chan []byte, 1024),
	}
}

func (p *swiftPipe) Read(b []byte) (int, error) {
	data, ok := <-p.in
	if !ok {
		return 0, io.EOF
	}
	n := copy(b, data)
	return n, nil
}

func (p *swiftPipe) Write(b []byte) (int, error) {
	if globalPacketWriter != nil {
		clone := make([]byte, len(b))
		copy(clone, b)
		globalPacketWriter.WritePacket(clone)
		return len(b), nil
	}

	clone := make([]byte, len(b))
	copy(clone, b)
	select {
	case p.out <- clone:
	default:
		log.Println("dropped outbound packet (channel full)")
	}
	return len(b), nil
}

// initTUN sets up the gvisor stack for tun2socks.
func initTUN() {
	tunOnce.Do(func() {
		pipe = newSwiftPipe()

		// Create gVisor link endpoint based on our swift pipe
		// MTU is standard 1500, no offset.
		ep, err := iobased.New(pipe, 1500, 0)
		if err != nil {
			log.Fatalf("failed to create iobased endpoint: %v", err)
		}

		handler := &tunHandler{}

		cfg := &core.Config{
			LinkEndpoint:     ep,
			TransportHandler: handler,
		}

		_, err = core.CreateStack(cfg)
		if err != nil {
			log.Fatalf("failed to create tun2socks stack: %v", err)
		}
	})
}

// InputPacket feeds an IP packet (read from iOS NEPacketTunnelProvider) into the TCP/IP stack.
func InputPacket(packet []byte) {
	initTUN()
	clone := make([]byte, len(packet))
	copy(clone, packet)
	select {
	case pipe.in <- clone:
	default:
		// drop if full
	}
}

// GetPacket blocks until an IP packet is ready to be written to iOS NEPacketTunnelProvider.
// This is an alternative to SetPacketWriter.
func GetPacket() []byte {
	return <-pipe.out
}

// tunHandler implements adapter.TransportHandler
type tunHandler struct{}

func (h *tunHandler) HandleTCP(conn adapter.TCPConn) {
	if activeClient == nil {
		_ = conn.Close()
		return
	}

	target := conn.LocalAddr().(*net.TCPAddr)
	host := target.IP.String()
	port := target.Port

	log.Printf("tun2socks intercepting TCP -> %s:%d", host, port)

	// Dial the WhiteNet tunnel
	remoteConn, err := activeClient.ipc.Dial(host, port)
	if err != nil {
		log.Printf("tunnel dial failed for %s:%d: %v", host, port, err)
		_ = conn.Close()
		return
	}

	// Bidirectional copy (two independent goroutines, non-blocking)
	go func() {
		defer conn.Close()
		defer remoteConn.Close()
		_, _ = io.Copy(remoteConn, conn) // Local -> Remote
	}()

	go func() {
		defer conn.Close()
		defer remoteConn.Close()
		_, _ = io.Copy(conn, remoteConn) // Remote -> Local
	}()
}

func (h *tunHandler) HandleUDP(conn adapter.UDPConn) {
	// WhiteNet server поддерживает только TCP.
	// Все UDP сразу закрываем — это заставит iOS/Safari переключиться с QUIC на TCP.
	// DNS-запросы исключены из туннеля через excludedRoutes в Swift.
	_ = conn.Close()
}

// StartVPN is the main entry point for the iOS TUN mode.
func StartVPN(yamlString string) (*Client, error) {
	client, err := Start(yamlString)
	if err != nil {
		return nil, err
	}

	activeClient = client
	initTUN()

	return client, nil
}
