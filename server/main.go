// Package main provides the olcrtc tunnel server (jitsi + datachannel).
//
// Usage: server <config.yaml>
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
	"strings"
	"syscall"
	"time"

	"olcrtc-jitsi-dc/pkg/carrier"
	"olcrtc-jitsi-dc/pkg/logger"
	"olcrtc-jitsi-dc/pkg/names"
	"olcrtc-jitsi-dc/pkg/server"
	"olcrtc-jitsi-dc/pkg/transport"
	"olcrtc-jitsi-dc/pkg/transport/datachannel"

	"gopkg.in/yaml.v3"
)

var (
	ErrConfigPathRequired = errors.New("usage: server <config.yaml>")
	ErrDataDirRequired    = errors.New("data directory required (set 'data:' in YAML)")
)

// yamlConfig is the on-disk YAML schema for the server.
type yamlConfig struct {
	Auth struct {
		Provider string `yaml:"provider"`
	} `yaml:"auth"`
	Room struct {
		ID      string `yaml:"id"`
		Channel string `yaml:"channel"`
	} `yaml:"room"`
	Users []struct {
		Code string `yaml:"code"`
	} `yaml:"users"`
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
		ProxyAddr string `yaml:"proxy_addr"`
		ProxyPort int    `yaml:"proxy_port"`
		ProxyUser string `yaml:"proxy_user"`
		ProxyPass string `yaml:"proxy_pass"`
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

	return runManaged(func(ctx context.Context) error {
		if len(cfg.Users) == 0 {
			// Fallback for single user mode if no users array provided
			cfg.Users = append(cfg.Users, struct{ Code string `yaml:"code"` }{Code: ""})
		}
		
		errCh := make(chan error, len(cfg.Users))
		for _, user := range cfg.Users {
			userRoomURL := cfg.Room.ID
			if user.Code != "" {
				userRoomURL = strings.TrimRight(cfg.Room.ID, "/") + "/" + user.Code
			}
			go func(code, roomURL string) {
				logger.Infof("starting server for user code: %s", code)
				err := server.Run(ctx, server.Config{
					Transport:      "datachannel",
					Carrier:        cfg.Auth.Provider,
					RoomURL:        roomURL,
					ChannelID:      cfg.Room.Channel,
					KeyHex:         cfg.Crypto.Key,
					DNSServer:      cfg.Net.DNS,
					SOCKSProxyAddr: cfg.SOCKS.ProxyAddr,
					SOCKSProxyPort: cfg.SOCKS.ProxyPort,
					SOCKSProxyUser: cfg.SOCKS.ProxyUser,
					SOCKSProxyPass: cfg.SOCKS.ProxyPass,
					OnSessionOpen: func(sessionID, deviceID string, claims map[string]any) {
						logger.Infof("[user:%s] session opened: id=%s device=%s claims=%v", code, sessionID, deviceID, claims)
					},
					OnSessionClose: func(sessionID, reason string) {
						logger.Infof("[user:%s] session closed: id=%s reason=%s", code, sessionID, reason)
					},
					OnTraffic: func(sessionID, addr string, bytesIn, bytesOut uint64) {
						logger.Infof("[user:%s] traffic: session=%s addr=%s in=%d out=%d", code, sessionID, addr, bytesIn, bytesOut)
					},
				})
				errCh <- err
			}(user.Code, userRoomURL)
		}
		
		// If any server fails, return its error
		for range cfg.Users {
			if err := <-errCh; err != nil {
				return err
			}
		}
		return nil
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
