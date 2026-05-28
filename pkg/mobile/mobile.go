// Package mobile provides the gomobile bindings for iOS and Android.
package mobile

import (
	"context"
	"fmt"
	"net"

	"olcrtc-jitsi-dc/pkg/carrier"
	"olcrtc-jitsi-dc/pkg/client"
	"olcrtc-jitsi-dc/pkg/logger"
	"olcrtc-jitsi-dc/pkg/names"
	"olcrtc-jitsi-dc/pkg/transport"
	"olcrtc-jitsi-dc/pkg/transport/datachannel"

	"gopkg.in/yaml.v3"
)

// Ensure defaults are registered for the mobile build.
func init() {
	carrier.RegisterDefaults()
	transport.Register("datachannel", datachannel.New)
}

// Client represents a running VPN/Tunnel client instance.
type Client struct {
	ipc *client.InProcessClient
}

// Connection represents an active stream to a remote host over the tunnel.
type Connection struct {
	conn net.Conn
}

// yamlConfig duplicates the necessary fields from the desktop client's config
// to allow parsing the same config.yaml string on mobile.
type yamlConfig struct {
	Auth struct {
		Provider string `yaml:"provider"`
	} `yaml:"auth"`
	Room struct {
		ID      string `yaml:"id"`
		Channel string `yaml:"channel"`
	} `yaml:"room"`
	Crypto struct {
		Key string `yaml:"key"`
	} `yaml:"crypto"`
	Net struct {
		DNS string `yaml:"dns"`
	} `yaml:"net"`
	Socks struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		User string `yaml:"user"`
		Pass string `yaml:"pass"`
	} `yaml:"socks"`
	Debug bool `yaml:"debug"`
}

// Start parses the provided YAML configuration string and starts the client.
func Start(yamlString string) (*Client, error) {
	var cfg yamlConfig
	if err := yaml.Unmarshal([]byte(yamlString), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml: %w", err)
	}

	if cfg.Debug {
		logger.SetVerbose(true)
	}

	localAddr := fmt.Sprintf("%s:%d", cfg.Socks.Host, cfg.Socks.Port)

	cCfg := client.Config{
		Transport: "datachannel",
		Carrier:   cfg.Auth.Provider,
		RoomURL:   cfg.Room.ID,
		ChannelID: cfg.Room.Channel,
		KeyHex:    cfg.Crypto.Key,
		DNSServer: cfg.Net.DNS,
		LocalAddr: localAddr,
		SOCKSUser: cfg.Socks.User,
		SOCKSPass: cfg.Socks.Pass,
	}

	ipc, err := client.StartInProcess(context.Background(), cCfg)
	if err != nil {
		return nil, fmt.Errorf("start in-process client: %w", err)
	}

	return &Client{ipc: ipc}, nil
}

// Stop gracefully shuts down the client.
func (c *Client) Stop() {
	if c.ipc != nil {
		c.ipc.Close()
	}
}

// Dial opens a tunnel to the specified host and port.
func (c *Client) Dial(host string, port int) (*Connection, error) {
	if c.ipc == nil {
		return nil, fmt.Errorf("client is not running")
	}

	conn, err := c.ipc.Dial(host, port)
	if err != nil {
		return nil, err
	}
	return &Connection{conn: conn}, nil
}

// Read reads up to max bytes from the connection.
// It returns a byte slice containing the read data.
func (c *Connection) Read(max int) ([]byte, error) {
	if max <= 0 {
		max = 4096
	}
	buf := make([]byte, max)
	n, err := c.conn.Read(buf)
	if n > 0 {
		return buf[:n], err
	}
	return nil, err
}

// Write writes data to the connection.
func (c *Connection) Write(data []byte) (int, error) {
	return c.conn.Write(data)
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.conn.Close()
}

// LoadNames allows overriding the embedded name dictionaries from local files.
func LoadNames(firstPath, lastPath string) error {
	return names.LoadNameFiles(firstPath, lastPath)
}
