// Package carrier wires the jitsi auth provider to the jitsi engine.
// Stripped version of engine/builtin — only jitsi + "none" carriers.
package carrier

import (
	"context"
	"errors"
	"fmt"

	"olcrtc-jitsi-dc/pkg/auth"
	authJitsi "olcrtc-jitsi-dc/pkg/auth/jitsi"
	"olcrtc-jitsi-dc/pkg/engine"
	_ "olcrtc-jitsi-dc/pkg/engine/jitsi" // register jitsi engine via init
)

// ErrCarrierNotFound is returned when an unregistered carrier name is requested.
var ErrCarrierNotFound = errors.New("carrier not found")

// ErrAuthFailed wraps an auth provider rejection.
var ErrAuthFailed = errors.New("carrier auth failed")

// Config holds the inputs to [Open].
type Config struct {
	RoomURL    string
	Name       string
	OnData     func([]byte)
	OnPeerData func(peerID string, data []byte)
	DNSServer  string
	ProxyAddr  string
	ProxyPort  int
	Engine     string
	URL        string
	Token      string
}

// Factory creates an engine session for a given carrier.
type Factory func(ctx context.Context, cfg Config) (engine.Session, error)

var registry = map[string]Factory{}

// Register adds a carrier factory.
func Register(name string, f Factory) {
	registry[name] = f
}

// Open looks up the carrier factory and creates an engine session.
func Open(ctx context.Context, name string, cfg Config) (engine.Session, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrCarrierNotFound, name)
	}
	return f(ctx, cfg)
}

// Available reports all registered carrier names.
func Available() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// RegisterDefaults wires the jitsi carrier and the "none" (direct engine) carrier.
func RegisterDefaults() {
	registerEngineAuth("jitsi", authJitsi.Provider{})
	registerDirect("none")
}

func registerDirect(name string) {
	Register(name, func(ctx context.Context, cfg Config) (engine.Session, error) {
		engineName := cfg.Engine
		if engineName == "" {
			engineName = "jitsi"
		}
		sess, err := engine.New(ctx, engineName, engine.Config{
			URL:        cfg.URL,
			Token:      cfg.Token,
			Name:       cfg.Name,
			OnData:     cfg.OnData,
			OnPeerData: cfg.OnPeerData,
			DNSServer:  cfg.DNSServer,
			ProxyAddr:  cfg.ProxyAddr,
			ProxyPort:  cfg.ProxyPort,
		})
		if err != nil {
			return nil, fmt.Errorf("engine new: %w", err)
		}
		return sess, nil
	})
}

func registerEngineAuth(name string, provider auth.Provider) {
	Register(name, func(ctx context.Context, cfg Config) (engine.Session, error) {
		authCfg := auth.Config{
			RoomURL:   cfg.RoomURL,
			Name:      cfg.Name,
			DNSServer: cfg.DNSServer,
			ProxyAddr: cfg.ProxyAddr,
			ProxyPort: cfg.ProxyPort,
		}
		creds, err := provider.Issue(ctx, authCfg)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrAuthFailed, err)
		}
		sess, err := engine.New(ctx, provider.Engine(), engine.Config{
			URL:        creds.URL,
			Token:      creds.Token,
			Name:       cfg.Name,
			Extra:      creds.Extra,
			OnData:     cfg.OnData,
			OnPeerData: cfg.OnPeerData,
			DNSServer:  cfg.DNSServer,
			ProxyAddr:  cfg.ProxyAddr,
			ProxyPort:  cfg.ProxyPort,
			Refresh: func(ctx context.Context) (engine.Credentials, error) {
				fresh, err := provider.Issue(ctx, authCfg)
				if err != nil {
					return engine.Credentials{}, fmt.Errorf("auth refresh: %w", err)
				}
				return engine.Credentials{URL: fresh.URL, Token: fresh.Token, Extra: fresh.Extra}, nil
			},
		})
		if err != nil {
			return nil, fmt.Errorf("engine new: %w", err)
		}
		return sess, nil
	})
}
