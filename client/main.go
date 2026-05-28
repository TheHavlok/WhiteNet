// Package main provides the olcrtc tunnel client (jitsi + datachannel + SOCKS5).
//
// Usage: client <config.yaml>
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"olcrtc-jitsi-dc/pkg/carrier"
	"olcrtc-jitsi-dc/pkg/client"
	"olcrtc-jitsi-dc/pkg/logger"
	"olcrtc-jitsi-dc/pkg/names"
	"olcrtc-jitsi-dc/pkg/transport"
	"olcrtc-jitsi-dc/pkg/transport/datachannel"

	"gopkg.in/yaml.v3"
)

var (
	ErrConfigPathRequired = errors.New("usage: client <config.yaml>")
	ErrDataDirRequired    = errors.New("data directory required (set 'data:' in YAML)")
)

// yamlConfig is the on-disk YAML schema for the client.
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
	Liveness struct {
		Interval string `yaml:"interval"`
		Timeout  string `yaml:"timeout"`
		Failures int    `yaml:"failures"`
	} `yaml:"liveness"`
	SOCKS struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		User string `yaml:"user"`
		Pass string `yaml:"pass"`
	} `yaml:"socks"`
	Data  string `yaml:"data"`
	Debug bool   `yaml:"debug"`
}

func main() {
	if err := run(); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}

func run() error {
	logger.DisableNoisyPionLogs()
	carrier.RegisterDefaults()
	transport.Register("datachannel", datachannel.New)

	if len(os.Args) != 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		return ErrConfigPathRequired
	}

	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg yamlConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	configureLogging(cfg.Debug)

	if err := prepareRuntimeData(cfg.Data); err != nil {
		return err
	}

	localAddr := fmt.Sprintf("%s:%d", cfg.SOCKS.Host, cfg.SOCKS.Port)

	return runManaged(func(ctx context.Context) error {
		return client.Run(ctx, client.Config{
			Transport: "datachannel",
			Carrier:   cfg.Auth.Provider,
			RoomURL:   cfg.Room.ID,
			ChannelID: cfg.Room.Channel,
			KeyHex:    cfg.Crypto.Key,
			LocalAddr: localAddr,
			DNSServer: cfg.Net.DNS,
			SOCKSUser: cfg.SOCKS.User,
			SOCKSPass: cfg.SOCKS.Pass,
		})
	})
}

var noisyPrefixes = [][]byte{
	[]byte("turnc"), []byte("[turn]"), []byte("Fail to refresh permissions"),
}

type filteredWriter struct{ w io.Writer }

func (fw filteredWriter) Write(p []byte) (int, error) {
	for _, prefix := range noisyPrefixes {
		if bytes.Contains(p, prefix) {
			return len(p), nil
		}
	}
	return fw.w.Write(p)
}

func configureLogging(debug bool) {
	log.SetOutput(filteredWriter{w: os.Stderr})
	logger.DisableNoisyPionLogs()
	if debug {
		logger.SetVerbose(true)
		return
	}
	_ = os.Setenv("PION_LOG_DISABLE", "all")
}

func prepareRuntimeData(dataDir string) error {
	if dataDir == "" {
		return ErrDataDirRequired
	}
	resolved, err := resolveDataDir(dataDir)
	if err != nil {
		return err
	}
	return loadNames(resolved)
}

func resolveDataDir(dataDir string) (string, error) {
	if filepath.IsAbs(dataDir) {
		return dataDir, nil
	}
	exePath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable path: %w", err)
	}
	return filepath.Join(filepath.Dir(exePath), dataDir), nil
}

func loadNames(dataDir string) error {
	namesPath := filepath.Join(dataDir, "names")
	surnamesPath := filepath.Join(dataDir, "surnames")
	if err := names.LoadNameFiles(namesPath, surnamesPath); err != nil {
		return fmt.Errorf("load names: %w", err)
	}
	return nil
}

func runManaged(run func(context.Context) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx) }()
	select {
	case <-sigCh:
		logger.Info("Shutting down gracefully...")
		cancel()
		select {
		case err := <-errCh:
			return err
		case <-time.After(5 * time.Second):
			logger.Warn("Shutdown timeout")
			return nil
		}
	case err := <-errCh:
		return err
	}
}
