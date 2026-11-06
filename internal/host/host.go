// Package host models Asterisk as endpoints and calls, and owns all ARI state.
// Calls live here, not in a plugin, so a plugin restart does not end them. It
// holds no behavior: what happens and when is decided by whoever calls the API.
package host

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/slidebolt/asterisk-host/internal/ari"
	media "github.com/slidebolt/sb-media-sdk"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	speech "github.com/slidebolt/sb-speech-sdk"
)

const (
	stasisApp = "slidebolt"
	// Every channel and bridge this host creates has this prefix. Cleanup only
	// ever touches IDs with it, so other calls on the PBX are safe.
	resourcePrefix     = "slidebolt-"
	ariCallTimeout     = 10 * time.Second
	defaultRingSeconds = 30
	// hardLimitMargin keeps the host's own limit first, so a call normally ends with
	// a clear reason. Asterisk's limit only matters if the host has died.
	hardLimitMargin = 5
	maxEvents       = 2000
	maxCommands     = 200
)

// Errors the API maps to status codes.
var (
	ErrNotFound  = errors.New("not found")
	ErrForbidden = errors.New("not allowed")
	ErrConflict  = errors.New("conflict")
	ErrInvalid   = errors.New("invalid request")
	ErrOffline   = errors.New("asterisk is not connected")
)

var (
	callIDPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	numberPattern   = regexp.MustCompile(`^\+?[0-9]{3,20}$`)
	soundPattern    = regexp.MustCompile(`^[A-Za-z0-9_/-]{1,128}$`)
	dtmfPattern     = regexp.MustCompile(`^[0-9A-D*#]{1,32}$`)
	callerIDPattern = regexp.MustCompile(`^"?([^"<]*?)"?\s*<`)
)

// Media is the part of sb-media the host uses.
type Media interface {
	Register(media.RegisterRequest) (string, error)
	Start(media.StartRequest) (media.StartReply, error)
	Stop(sessionID string) error
	OnStatus(stream string, handler func(media.StatusEvent)) (messenger.Subscription, error)
}

type activeStream struct {
	state    *Stream
	key      string
	mediaID  string
	channel  string
	sub      messenger.Subscription
	endpoint string // set when this is an endpoint's listen stream
	stopIdle bool   // the call exists only for this stream
}

type activeParticipant struct {
	state   *Participant
	channel string
	joined  bool
}

type activeCall struct {
	state    *Call
	bridge   string
	parts    map[string]*activeParticipant
	order    []string
	stream   *activeStream
	transcr  *activeTranscription
	timer    *time.Timer
	retain   *time.Timer
	limit    time.Duration
	hangupOn map[string]bool // playbacks that end the call when finished
}

// Speech is the part of sb-speech the host uses.
type Speech interface {
	Start(speech.StartRequest) (speech.StartReply, error)
	Stop(sessionID string) error
	OnEvent(sessionID string, handler func(speech.Event)) (messenger.Subscription, error)
}

// Host owns all Asterisk state.
type Host struct {
	cfg    Config
	ari    *ari.Client
	media  Media
	speech Speech

	mu        sync.Mutex
	streamID  string
	sequence  int64
	events    []Event
	waiters   []chan struct{}
	server    Server
	endpoints map[string]*Endpoint
	calls     map[string]*activeCall
	channels  map[string]*activeCall
	commands  map[string]*Command
	cmdOrder  []string
	cancel    context.CancelFunc
	closed    bool
}

// New creates a Host.
func New(cfg Config, m Media, sp Speech) *Host {
	return &Host{
		cfg:       cfg,
		ari:       ari.New(cfg.ARIURL, cfg.ARIUser, cfg.ARIPass),
		media:     m,
		speech:    sp,
		streamID:  randomID(),
		endpoints: map[string]*Endpoint{},
		calls:     map[string]*activeCall{},
		channels:  map[string]*activeCall{},
		commands:  map[string]*Command{},
	}
}

func randomID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// Run connects to Asterisk and keeps state current until Close.
func (h *Host) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	h.mu.Lock()
	h.cancel = cancel
	h.mu.Unlock()
	go h.ari.RunEvents(ctx, stasisApp, ari.Listener{
		OnConnected:    h.onConnected,
		OnDisconnected: h.onDisconnected,
		OnEvent:        h.onEvent,
	})
	go func() {
		ticker := time.NewTicker(h.cfg.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.mu.Lock()
				if h.server.Connected {
					h.refreshEndpoints()
				}
				h.mu.Unlock()
			}
		}
	}()
}

// Close ends all calls. A stopping host must not leave a microphone open.
func (h *Host) Close() {
	h.mu.Lock()
	h.closed = true
	cancel := h.cancel
	for _, c := range h.activeCalls() {
		h.endCall(c, "host stopped")
	}
	if h.server.Connected {
		ctx, stop := ariContext()
		if err := h.ari.UnsubscribeEndpoints(ctx, stasisApp); err != nil {
			log.Printf("asterisk-host: unsubscribe endpoints: %v", err)
		}
		stop()
	}
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (h *Host) activeCalls() []*activeCall {
	var out []*activeCall
	for _, c := range h.calls {
		if c.state.Status == CallActive {
			out = append(out, c)
		}
	}
	return out
}

func ariContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), ariCallTimeout)
}

func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// emit records an event. Callers hold mu. Data must be a value, not a pointer to
// state that may change later.
func (h *Host) emit(eventType string, data any) {
	h.sequence++
	h.events = append(h.events, Event{
		Version: APIVersion, ID: h.sequence, StreamID: h.streamID,
		Timestamp: now(), Type: eventType, Data: data,
	})
	if len(h.events) > maxEvents {
		h.events = slices.Clone(h.events[len(h.events)-maxEvents:])
	}
	for _, w := range h.waiters {
		close(w)
	}
	h.waiters = nil
}

func cloneCall(c *Call) Call {
	out := *c
	out.Participants = slices.Clone(c.Participants)
	if out.Participants == nil {
		out.Participants = []Participant{}
	}
	if c.Stream != nil {
		s := *c.Stream
		out.Stream = &s
	}
	if c.Transcript != nil {
		t := *c.Transcript
		out.Transcript = &t
	}
	return out
}

func cloneEndpoint(e *Endpoint) Endpoint {
	out := *e
	if e.Stream != nil {
		s := *e.Stream
		out.Stream = &s
	}
	return out
}

func (h *Host) emitServer() {
	h.server.ActiveCalls = len(h.activeCalls())
	h.emit(EventServer, h.server)
}

func (h *Host) emitEndpoint(e *Endpoint) {
	h.emit(EventEndpoint, cloneEndpoint(e))
}

// emitCall publishes a call, after rebuilding the state derived from it.
func (h *Host) emitCall(c *activeCall) {
	c.state.Participants = c.state.Participants[:0]
	for _, id := range c.order {
		c.state.Participants = append(c.state.Participants, *c.parts[id].state)
	}
	h.emit(EventCall, cloneCall(c.state))
}

// setEndpointCall records which call an endpoint is in, and publishes changes.
func (h *Host) setEndpointCall(id, call string) {
	e, ok := h.endpoints[id]
	if !ok || e.Call == call {
		return
	}
	e.Call = call
	h.emitEndpoint(e)
}

// onConnected runs when the Stasis app registers with Asterisk.
func (h *Host) onConnected() {
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx, cancel := ariContext()
	defer cancel()
	h.server.Connected = true
	h.server.Error = ""
	if info, err := h.ari.Info(ctx); err != nil {
		log.Printf("asterisk-host: server info: %v", err)
	} else {
		h.server.Version = info.System.Version
	}
	if err := h.ari.SubscribeEndpoints(ctx, stasisApp); err != nil {
		log.Printf("asterisk-host: subscribe endpoints: %v", err)
	}
	if len(h.activeCalls()) == 0 {
		h.cleanupLeftovers(ctx)
	}
	h.refreshEndpoints()
	h.emitServer()
}

func (h *Host) onDisconnected() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.server.Connected = false
	h.server.Error = "lost connection to asterisk"
	for _, c := range h.activeCalls() {
		h.endCall(c, "lost connection to asterisk")
	}
	h.emitServer()
}

// cleanupLeftovers removes resources from an earlier run. Callers hold mu.
func (h *Host) cleanupLeftovers(ctx context.Context) {
	channels, err := h.ari.Channels(ctx)
	if err != nil {
		log.Printf("asterisk-host: list channels: %v", err)
	}
	for _, ch := range channels {
		if strings.HasPrefix(ch.ID, resourcePrefix) {
			log.Printf("asterisk-host: removing leftover channel %s", ch.ID)
			if err := h.ari.Hangup(ctx, ch.ID); err != nil {
				log.Printf("asterisk-host: remove channel: %v", err)
			}
		}
	}
	bridges, err := h.ari.Bridges(ctx)
	if err != nil {
		log.Printf("asterisk-host: list bridges: %v", err)
	}
	for _, b := range bridges {
		if strings.HasPrefix(b.ID, resourcePrefix) {
			log.Printf("asterisk-host: removing leftover bridge %s", b.ID)
			if err := h.ari.DestroyBridge(ctx, b.ID); err != nil {
				log.Printf("asterisk-host: remove bridge: %v", err)
			}
		}
	}
}

// GetSnapshot returns the complete state.
func (h *Host) GetSnapshot() Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Snapshot{
		APIVersion: APIVersion, StreamID: h.streamID, Sequence: h.sequence,
		Server: h.server, Endpoints: h.listEndpoints(), Calls: h.listCalls(),
	}
	s.Server.ActiveCalls = len(h.activeCalls())
	return s
}

func (h *Host) onEvent(ev ari.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch ev.Type {
	case "EndpointStateChange":
		ctx, cancel := ariContext()
		defer cancel()
		h.applyEndpoint(ctx, ev.Endpoint)
		return
	case "PlaybackFinished":
		for _, c := range h.activeCalls() {
			if c.hangupOn[ev.Playback.ID] {
				h.endCall(c, "ended")
				return
			}
		}
		return
	}

	c, ok := h.channels[ev.Channel.ID]
	if !ok {
		return
	}
	if t := c.transcr; t != nil && t.channel == ev.Channel.ID {
		h.onTranscriptionChannel(c, ev)
		return
	}
	switch ev.Type {
	case "StasisStart":
		h.onStasisStart(c, ev.Channel.ID)
	case "StasisEnd", "ChannelDestroyed":
		h.onChannelGone(c, ev)
	case "ChannelDtmfReceived":
		if _, p := c.participantFor(ev.Channel.ID); p != nil {
			p.state.LastDTMF = ev.Digit
			h.emitCall(c)
		}
	}
}

// Play plays a sound into a call.
func (h *Host) Play(callID string, req PlayRequest) error {
	if !soundPattern.MatchString(req.Sound) {
		return fmt.Errorf("%w: bad sound name", ErrInvalid)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return ErrNotFound
	}
	ctx, cancel := ariContext()
	defer cancel()
	id, err := h.ari.PlayOnBridge(ctx, c.bridge, req.Sound)
	if err != nil {
		return err
	}
	if req.ThenHangup {
		c.hangupOn[id] = true
	}
	return nil
}

// Reload reloads the dialplan and SIP configuration.
func (h *Host) Reload() error {
	ctx, cancel := ariContext()
	defer cancel()
	for _, module := range []string{"res_pjsip.so", "pbx_config.so"} {
		if err := h.ari.ReloadModule(ctx, module); err != nil {
			return err
		}
	}
	return nil
}

// escapePath is used by the API when building Location headers.
func escapePath(s string) string {
	return url.PathEscape(s)
}
