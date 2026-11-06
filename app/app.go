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
	"time"

	"github.com/slidebolt/asterisk-host/internal/host"
	contract "github.com/slidebolt/sb-contract"
	media "github.com/slidebolt/sb-media-sdk"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	speech "github.com/slidebolt/sb-speech-sdk"
)

// ServiceID is the identifier other binaries use in DependsOn.
const ServiceID = "asterisk-host"

// App is the asterisk-host service.
type App struct {
	msg  messenger.Messenger
	host *host.Host
	srv  *http.Server
	subs []messenger.Subscription
}

func New() *App {
	return &App{}
}

func (a *App) Hello() contract.HelloResponse {
	return contract.HelloResponse{
		ID:              ServiceID,
		Kind:            contract.KindService,
		ContractVersion: contract.ContractVersion,
		DependsOn:       []string{"messenger", "media"},
	}
}

// OnStart stays up when it cannot serve. A failed start aborts sb-manager's
// startAll, which would stop every other binary.
func (a *App) OnStart(deps map[string]json.RawMessage) (json.RawMessage, error) {
	msg, err := messenger.Connect(deps)
	if err != nil {
		return nil, fmt.Errorf("asterisk-host: connect messenger: %w", err)
	}
	a.msg = msg
	if err := a.serve(); err != nil {
		log.Printf("asterisk-host: disabled: %v", err)
		return nil, nil
	}
	log.Println("asterisk-host: started")
	return nil, nil
}

func (a *App) serve() error {
	cfg, err := host.LoadConfig()
	if err != nil {
		return err
	}
	if err := cfg.Enabled(); err != nil {
		return err
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
	return nil
}

func (a *App) OnShutdown() error {
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
