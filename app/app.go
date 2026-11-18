// Package app runs asterisk-host under sb-manager.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/slidebolt/asterisk-host/internal/ari"
	"github.com/slidebolt/asterisk-host/internal/host"
	contract "github.com/slidebolt/sb-contract"
	media "github.com/slidebolt/sb-media-sdk"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	runtime "github.com/slidebolt/sb-runtime"
	speech "github.com/slidebolt/sb-speech-sdk"
)

// ServiceID is the identifier other binaries use in DependsOn.
const ServiceID = "asterisk-host"

const asteriskResource = "asterisk-ari"

var ariHealthInterval = 15 * time.Second

// App is the asterisk-host service.
type App struct {
	msg         messenger.Messenger
	host        *host.Host
	srv         *http.Server
	subs        []messenger.Subscription
	reporter    runtime.ResourceReporter
	monitorStop context.CancelFunc
}

func New() *App {
	return &App{}
}

func (a *App) Hello() contract.HelloResponse {
	setup := contract.DependencySetup("Voice",
		contract.SetupField{Key: "bind", Environment: "ASTERISK_HOST_BIND", Name: "Host bind address", Control: contract.SetupText, Default: json.RawMessage(`"127.0.0.1:47832"`), Advanced: true},
		contract.SetupField{Key: "token", Environment: "ASTERISK_HOST_TOKEN", Name: "Host token", Control: contract.SetupSecret, Sensitive: true, Required: true, Generate: "secret", Advanced: true},
		contract.SetupField{Key: "ariURL", Environment: "ASTERISK_ARI_URL", Name: "Asterisk ARI URL", Control: contract.SetupText, Format: "url", Advanced: true},
		contract.SetupField{Key: "ariUser", Environment: "ASTERISK_ARI_USER", Name: "Asterisk ARI user", Control: contract.SetupText, Advanced: true},
		contract.SetupField{Key: "ariPassword", Environment: "ASTERISK_ARI_PASS", Name: "Asterisk ARI password", Control: contract.SetupSecret, Sensitive: true, Advanced: true},
	)
	setup.Resources = []contract.ExternalResourceDefinition{{
		ID: asteriskResource, Kind: contract.ExternalResourceService, Name: "Asterisk ARI", Description: "Controls calls and receives telephony events from Asterisk.",
		Fields: []string{"ariURL", "ariUser", "ariPassword"}, Activation: contract.ResourceActivation{Mode: contract.ResourceActivationConfigured, Fields: []string{"ariURL"}},
	}}
	return contract.HelloResponse{
		ID:              ServiceID,
		Kind:            contract.KindService,
		ContractVersion: contract.ContractVersion,
		DependsOn:       []string{"messenger", "media", "speech"},
		Setup:           setup,
	}
}

func (a *App) SetResourceReporter(reporter runtime.ResourceReporter) { a.reporter = reporter }

// OnStart stays up when it cannot serve. A failed start aborts sb-manager's
// startAll, which would stop every other binary.
func (a *App) OnStart(deps map[string]json.RawMessage) (json.RawMessage, error) {
	msg, err := messenger.Connect(deps)
	if err != nil {
		return nil, fmt.Errorf("asterisk-host: connect messenger: %w", err)
	}
	a.msg = msg
	cfg, configErr := host.LoadConfig()
	if configErr != nil {
		if strings.TrimSpace(cfg.ARIURL) != "" {
			a.resourceDegraded("configuration_invalid", configErr, false)
			return nil, configErr
		}
		log.Printf("asterisk-host: disabled: %v", configErr)
	} else if err := a.serve(cfg); err != nil {
		if strings.TrimSpace(cfg.ARIURL) != "" {
			a.resourceDegraded("ari_unavailable", err, true)
			return nil, err
		}
		log.Printf("asterisk-host: disabled: %v", err)
	} else {
		log.Println("asterisk-host: started")
	}
	payload, err := dependencyPayload(cfg)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (a *App) serve(cfg host.Config) error {
	if err := cfg.Enabled(); err != nil {
		return err
	}
	// ARI is an external resource and may still be booting (or may become
	// unavailable later). The host API must remain available in that state so
	// dependants can start and report disconnected entities while RunEvents
	// reconnects in the background.
	if err := checkARI(cfg); err != nil {
		a.resourceDegraded("ari_unavailable", err, true)
	} else {
		a.resourceReady()
	}
	ln, err := net.Listen("tcp", cfg.Bind)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	client := media.NewClient(a.msg)
	speechClient := speech.NewClient(a.msg)
	a.host = host.New(cfg, client, speechClient)
	sub, err := client.OnServiceStarted(func() { go a.host.OnMediaStarted() })
	if err != nil {
		ln.Close()
		return fmt.Errorf("watch media service: %w", err)
	}
	a.subs = append(a.subs, sub)
	speechSub, err := speechClient.OnServiceStarted(func() { go a.host.OnSpeechStarted() })
	if err != nil {
		ln.Close()
		return fmt.Errorf("watch speech service: %w", err)
	}
	a.subs = append(a.subs, speechSub)
	a.srv = &http.Server{Handler: a.host.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := a.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("asterisk-host: serve: %v", err)
		}
	}()
	a.host.Run()
	a.startARIMonitor(cfg)
	return nil
}

func checkARI(cfg host.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := ari.New(cfg.ARIURL, cfg.ARIUser, cfg.ARIPass).Info(ctx)
	if err != nil {
		return fmt.Errorf("check Asterisk ARI: %w", err)
	}
	return nil
}

func (a *App) startARIMonitor(cfg host.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	a.monitorStop = cancel
	go func() {
		ticker := time.NewTicker(ariHealthInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := checkARI(cfg); err != nil {
					a.resourceDegraded("ari_unavailable", err, true)
				} else {
					a.resourceReady()
				}
			}
		}
	}()
}

func (a *App) resourceReady() {
	if a.reporter != nil {
		a.reporter.Ready(asteriskResource)
	}
}

func (a *App) resourceDegraded(code string, err error, retryable bool) {
	if a.reporter != nil {
		a.reporter.Degraded(asteriskResource, code, err.Error(), retryable)
	}
}

func dependencyPayload(cfg host.Config) (json.RawMessage, error) {
	hostName, port, err := net.SplitHostPort(cfg.Bind)
	if err != nil {
		return nil, fmt.Errorf("asterisk host bind: %w", err)
	}
	if hostName == "" || hostName == "0.0.0.0" || hostName == "::" {
		hostName = "127.0.0.1"
	}
	payload, err := json.Marshal(map[string]string{
		"url":   "http://" + net.JoinHostPort(hostName, port),
		"token": cfg.Token,
	})
	return payload, err
}

func (a *App) OnShutdown() error {
	if a.monitorStop != nil {
		a.monitorStop()
	}
	for _, sub := range a.subs {
		sub.Unsubscribe()
	}
	if a.host != nil {
		a.host.Close()
	}
	if a.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.srv.Shutdown(ctx); err != nil {
			a.srv.Close()
		}
	}
	if a.msg != nil {
		a.msg.Close()
	}
	return nil
}
