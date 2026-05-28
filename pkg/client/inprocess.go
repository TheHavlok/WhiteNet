package client

import (
	"context"
	"fmt"
	"net"

	"olcrtc-jitsi-dc/pkg/runtime"
)

// InProcessClient wraps Client to manage its background context.
type InProcessClient struct {
	*Client
	cancel context.CancelFunc
}

// StartInProcess initializes the client without starting a local TCP listener.
// It returns an InProcessClient that can be used to Dial remote hosts directly.
func StartInProcess(ctx context.Context, cfg Config) (*InProcessClient, error) {
	runCtx, cancel := context.WithCancel(ctx)

	cipher, err := setupCipher(cfg.KeyHex)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("setupCipher failed: %w", err)
	}

	deviceID, err := resolveDeviceID(cfg.DeviceID, cfg.DeviceIDPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("resolve device id: %w", err)
	}

	c := &Client{
		cipher:    cipher,
		deviceID:  deviceID,
		claims:    cfg.Claims,
		dnsServer: cfg.DNSServer,
		socksUser: cfg.SOCKSUser,
		socksPass: cfg.SOCKSPass,
		health:    runtime.NewHealthTracker(cfg.OnHealth),
	}

	if err := c.bringUpLink(runCtx, cfg, cancel); err != nil {
		c.shutdown()
		cancel()
		return nil, err
	}

	return &InProcessClient{
		Client: c,
		cancel: func() {
			cancel()
			c.shutdown()
		},
	}, nil
}

// Close gracefully shuts down the in-process client and its connections.
func (ipc *InProcessClient) Close() {
	ipc.cancel()
}

// Dial creates a direct tunnel to the target address and port over the WebRTC datachannel.
// It bypasses the local SOCKS5 listener, making it suitable for programmatic usage (e.g. mobile/iOS).
func (c *Client) Dial(targetAddr string, targetPort int) (net.Conn, error) {
	c.sessMu.RLock()
	sess := c.session
	c.sessMu.RUnlock()

	if sess == nil || sess.IsClosed() {
		return nil, ErrRemoteNotReady
	}

	stream, err := sess.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("open stream failed: %w", err)
	}

	if err := c.sendConnectRequest(stream, targetAddr, targetPort); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("connect request failed: %w", err)
	}

	return stream, nil
}
