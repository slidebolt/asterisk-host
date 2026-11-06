package host

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	media "github.com/slidebolt/sb-media-sdk"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	speech "github.com/slidebolt/sb-speech-sdk"
)

// fakeARI behaves like Asterisk's REST interface and event WebSocket.
type fakeARI struct {
	srv *httptest.Server

	mu        sync.Mutex
	calls     []string
	queries   map[string]string
	failOn    map[string]bool
	channels  map[string]bool
	bridges   map[string]bool
	endpoints map[string]string
	conn      *websocket.Conn
	writeMu   sync.Mutex
}

func newFakeARI(t *testing.T) *fakeARI {
	t.Helper()
	f := &fakeARI{
		queries: map[string]string{}, failOn: map[string]bool{},
		channels: map[string]bool{}, bridges: map[string]bool{},
		endpoints: map[string]string{"201": "online", "202": "online", "3000": "online", "204": "offline", "trunk1": "online"},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() { f.drop(); f.srv.Close() })
	return f
}

func (f *fakeARI) serve(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/ari")
	if path == "/events" {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conn = conn
		f.mu.Unlock()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	call := r.Method + " " + path
	f.calls = append(f.calls, call)
	f.queries[call] = r.URL.RawQuery
	if f.failOn[call] {
		http.Error(w, "injected failure", http.StatusInternalServerError)
		return
	}
	switch {
	case call == "GET /endpoints/PJSIP":
		var list []map[string]any
		ids := make([]string, 0, len(f.endpoints))
		for id := range f.endpoints {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			list = append(list, map[string]any{"technology": "PJSIP", "resource": id, "state": f.endpoints[id]})
		}
		json.NewEncoder(w).Encode(list)
		return
	case strings.HasPrefix(path, "/asterisk/config/dynamic/res_pjsip/endpoint/"):
		id := strings.TrimPrefix(path, "/asterisk/config/dynamic/res_pjsip/endpoint/")
		value := "<unknown>"
		if id == "201" {
			value = `"Kitchen Phone" <201>`
		}
		json.NewEncoder(w).Encode([]map[string]string{{"attribute": "callerid", "value": value}})
		return
	case call == "GET /asterisk/info":
		w.Write([]byte(`{"system":{"version":"22.7.0"}}`))
		return
	case call == "GET /channels":
		var list []map[string]string
		for id := range f.channels {
			list = append(list, map[string]string{"id": id})
		}
		json.NewEncoder(w).Encode(list)
		return
	case call == "GET /bridges":
		var list []map[string]string
		for id := range f.bridges {
			list = append(list, map[string]string{"id": id})
		}
		json.NewEncoder(w).Encode(list)
		return
	case call == "POST /bridges":
		f.bridges[q.Get("bridgeId")] = true
	case call == "POST /channels" || call == "POST /channels/externalMedia":
		f.channels[q.Get("channelId")] = true
	case strings.HasSuffix(path, "/play"):
		w.Write([]byte(`{"id":"pb1"}`))
		return
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/channels/") && strings.Count(path, "/") == 2:
		delete(f.channels, strings.TrimPrefix(path, "/channels/"))
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/bridges/"):
		delete(f.bridges, strings.TrimPrefix(path, "/bridges/"))
	}
	w.Write([]byte(`{}`))
}

func (f *fakeARI) send(t *testing.T, ev map[string]any) {
	t.Helper()
	f.mu.Lock()
	conn := f.conn
	f.mu.Unlock()
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if err := conn.WriteJSON(ev); err != nil {
		t.Fatalf("send event: %v", err)
	}
}

func (f *fakeARI) channelEvent(t *testing.T, typ, id string, extra ...any) {
	t.Helper()
	if typ == "ChannelDestroyed" {
		f.mu.Lock()
		delete(f.channels, id)
		f.mu.Unlock()
	}
	ev := map[string]any{"type": typ, "channel": map[string]string{"id": id}}
	for i := 0; i+1 < len(extra); i += 2 {
		ev[extra[i].(string)] = extra[i+1]
	}
	f.send(t, ev)
}

func (f *fakeARI) drop() {
	f.mu.Lock()
	conn := f.conn
	f.conn = nil
	f.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

func (f *fakeARI) fail(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOn[call] = true
}

func (f *fakeARI) find(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeARI) originates() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if c == "POST /channels" {
			out = append(out, f.queries[c])
		}
	}
	return out
}

func (f *fakeARI) lastQuery(call string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries[call]
}

func (f *fakeARI) resources() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.channels) + len(f.bridges)
}

// partChannel returns the newest participant channel.
func (f *fakeARI) partChannels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for id := range f.channels {
		if strings.HasPrefix(id, resourcePrefix+"part-") {
			out = append(out, id)
		}
	}
	return out
}

func (f *fakeARI) mediaChannel() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := range f.channels {
		if strings.HasPrefix(id, resourcePrefix+"media-") {
			return id
		}
	}
	return ""
}

func (f *fakeARI) speechChannel() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id := range f.channels {
		if strings.HasPrefix(id, resourcePrefix+"speech-") {
			return id
		}
	}
	return ""
}

// fakeMedia stands in for sb-media.
type fakeMedia struct {
	mu       sync.Mutex
	starts   []string
	stops    []string
	handlers map[string]func(media.StatusEvent)
	regFail  error
	startErr error
}

type noSub struct{}

func (noSub) Unsubscribe() error { return nil }

func (m *fakeMedia) Register(r media.RegisterRequest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.regFail != nil {
		return "", m.regFail
	}
	return "http://media/m/" + r.Stream, nil
}

func (m *fakeMedia) Start(r media.StartRequest) (media.StartReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startErr != nil {
		return media.StartReply{}, m.startErr
	}
	m.starts = append(m.starts, r.Stream)
	return media.StartReply{SessionID: "session-" + r.Stream, RTPHost: "10.0.0.1", RTPPort: 41000}, nil
}

func (m *fakeMedia) Stop(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stops = append(m.stops, id)
	return nil
}

func (m *fakeMedia) OnStatus(stream string, fn func(media.StatusEvent)) (messenger.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handlers == nil {
		m.handlers = map[string]func(media.StatusEvent){}
	}
	m.handlers[stream] = fn
	return noSub{}, nil
}

func (m *fakeMedia) publish(stream, status, errMsg string, viewers int) {
	m.mu.Lock()
	fn := m.handlers[stream]
	m.mu.Unlock()
	fn(media.StatusEvent{Stream: stream, SessionID: "session-" + stream, Status: status, Error: errMsg, Viewers: viewers})
}

func (m *fakeMedia) stopCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.stops)
}

// fakeSpeech stands in for sb-speech.
type fakeSpeech struct {
	mu       sync.Mutex
	starts   int
	stops    []string
	handler  func(speech.Event)
	startErr error
}

func (s *fakeSpeech) Start(speech.StartRequest) (speech.StartReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startErr != nil {
		return speech.StartReply{}, s.startErr
	}
	s.starts++
	return speech.StartReply{SessionID: "speech-1", RTPHost: "10.0.0.2", RTPPort: 41100}, nil
}

func (s *fakeSpeech) Stop(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops = append(s.stops, id)
	return nil
}

func (s *fakeSpeech) OnEvent(_ string, fn func(speech.Event)) (messenger.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = fn
	return noSub{}, nil
}

func (s *fakeSpeech) publish(ev speech.Event) {
	s.mu.Lock()
	fn := s.handler
	s.mu.Unlock()
	ev.SessionID = "speech-1"
	fn(ev)
}

func (s *fakeSpeech) stopCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.stops)
}

type harness struct {
	t      *testing.T
	ari    *fakeARI
	media  *fakeMedia
	speech *fakeSpeech
	host   *Host
	api    *httptest.Server
}

func start(t *testing.T, mutate func(*Config, *fakeARI)) *harness {
	t.Helper()
	f := newFakeARI(t)
	cfg := Config{
		Token: "tok", ARIURL: f.srv.URL, ARIUser: "u", ARIPass: "p",
		Endpoints: map[string]EndpointConfig{
			"3000": {Name: "Intercom", Role: "intercom", AutoAnswer: true, Listen: PolicyOnCommand},
			"202":  {Listen: PolicyInCall},
		},
		Trunks: map[string]bool{"trunk1": true}, DialAllow: map[string]bool{"+15551234567": true},
		MaxCall: time.Minute, MaxListen: time.Minute, RetainEnded: time.Minute, RefreshInterval: time.Hour,
	}
	if mutate != nil {
		mutate(&cfg, f)
	}
	m := &fakeMedia{}
	sp := &fakeSpeech{}
	h := &harness{t: t, ari: f, media: m, speech: sp, host: New(cfg, m, sp)}
	h.api = httptest.NewServer(h.host.Handler())
	h.host.Run()
	t.Cleanup(func() { h.host.Close(); h.api.Close() })
	h.eventually("connected and discovered", func() bool {
		return h.host.GetSnapshot().Server.Connected && len(h.host.Endpoints("")) > 0
	})
	return h
}

func (h *harness) eventually(what string, ok func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func (h *harness) participant(call, id string) Participant {
	h.t.Helper()
	c, err := h.host.GetCall(call)
	if err != nil {
		h.t.Fatalf("get call: %v", err)
	}
	for _, p := range c.Participants {
		if p.ID == id {
			return p
		}
	}
	h.t.Fatalf("no participant %s in %+v", id, c)
	return Participant{}
}

func (h *harness) waitParticipant(call, id, status string) {
	h.t.Helper()
	h.eventually(id+" to be "+status, func() bool { return h.participant(call, id).Status == status })
}

func (h *harness) callStatus(id string) string {
	c, err := h.host.GetCall(id)
	if err != nil {
		return ""
	}
	return c.Status
}

// dial adds a target and returns its channel ID.
func (h *harness) dial(call, target string) string {
	h.t.Helper()
	before := h.ari.partChannels()
	if _, err := h.host.AddParticipant(call, AddParticipantRequest{Target: target}); err != nil {
		h.t.Fatalf("add %s: %v", target, err)
	}
	for _, ch := range h.ari.partChannels() {
		if !slices.Contains(before, ch) {
			return ch
		}
	}
	h.t.Fatal("no new channel")
	return ""
}

func (h *harness) answer(call, target string) string {
	h.t.Helper()
	ch := h.dial(call, target)
	h.ari.channelEvent(h.t, "StasisStart", ch)
	h.waitParticipant(call, target, StatusJoined)
	return ch
}

func TestDiscovery_NamesAndFeaturesComeFromAsteriskAndConfig(t *testing.T) {
	h := start(t, nil)
	s := h.host.GetSnapshot()
	if s.Server.Version != "22.7.0" || !s.Server.Connected || s.StreamID == "" {
		t.Fatalf("server = %+v", s.Server)
	}
	phone, _ := h.host.Endpoint("201")
	if phone.Name != "Kitchen Phone" || phone.Status != "online" || phone.Kind != "extension" || phone.Features.Listen != PolicyNever || phone.Stream != nil {
		t.Fatalf("phone = %+v", phone)
	}
	intercom, _ := h.host.Endpoint("3000")
	if intercom.Name != "Intercom" || intercom.Role != "intercom" || !intercom.Features.AutoAnswer ||
		intercom.Features.Listen != PolicyOnCommand || intercom.Stream == nil || intercom.Stream.URL == "" {
		t.Fatalf("intercom = %+v", intercom)
	}
	trunk, _ := h.host.Endpoint("trunk1")
	if trunk.Kind != "trunk" || trunk.Stream != nil {
		t.Fatalf("trunk = %+v", trunk)
	}
	if got := h.host.Endpoints("trunk"); len(got) != 1 {
		t.Fatalf("trunks = %v", got)
	}
	offline, _ := h.host.Endpoint("204")
	if offline.Status != "offline" || offline.Name != "204" {
		t.Fatalf("no name should fall back to the id: %+v", offline)
	}
}

func TestDiscovery_EndpointStateChangeUpdatesAndNewEndpointsAppear(t *testing.T) {
	h := start(t, nil)
	h.ari.send(t, map[string]any{"type": "EndpointStateChange", "endpoint": map[string]any{"resource": "204", "state": "online"}})
	h.eventually("online", func() bool { e, _ := h.host.Endpoint("204"); return e.Status == "online" })
	h.ari.send(t, map[string]any{"type": "EndpointStateChange", "endpoint": map[string]any{"resource": "205", "state": "online"}})
	h.eventually("new endpoint", func() bool { _, err := h.host.Endpoint("205"); return err == nil })
}

func TestCall_PhoneFirstThenIntercomArePatched(t *testing.T) {
	h := start(t, nil)
	phone := h.dial("front-door", "201")
	if got := h.ari.originates(); !strings.Contains(got[0], "endpoint=PJSIP%2F201") {
		t.Fatalf("originate = %v", got)
	}
	if got := h.participant("front-door", "201").Status; got != StatusRinging {
		t.Fatalf("status = %s", got)
	}
	if len(h.ari.find("POST /bridges/")) != 0 {
		t.Fatal("must not bridge before the answer")
	}

	h.ari.channelEvent(t, "StasisStart", phone)
	h.waitParticipant("front-door", "201", StatusJoined)
	h.answer("front-door", "3000")

	got := h.ari.find("POST /bridges/")
	if len(got) != 2 || got[0] != got[1] {
		t.Fatalf("both must join the same bridge: %v", got)
	}
	e, _ := h.host.Endpoint("201")
	if e.Call != "front-door" {
		t.Fatalf("endpoint call = %q", e.Call)
	}
}

func TestCall_EndCleansUpEverything(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "201")
	h.answer("c", "3000")
	if err := h.host.EndCall("c"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("c") != CallEnded {
		t.Fatal("call should stay visible as ended")
	}
	if h.ari.resources() != 0 {
		t.Fatal("channels or bridges left behind")
	}
	if e, _ := h.host.Endpoint("201"); e.Call != "" {
		t.Fatalf("endpoint still in call: %+v", e)
	}
	if p := h.participant("c", "201"); p.Status != StatusLeft {
		t.Fatalf("participant = %+v", p)
	}
}

func TestCall_EndedCallIsRemovedAfterRetention(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.RetainEnded = 50 * time.Millisecond })
	h.answer("c", "201")
	h.host.EndCall("c")
	h.eventually("removal", func() bool { _, err := h.host.GetCall("c"); return errors.Is(err, ErrNotFound) })
	found := false
	for _, ev := range h.events() {
		found = found || ev.Type == EventRemoved
	}
	if !found {
		t.Fatal("removal must be published")
	}
}

func (h *harness) events() []Event {
	h.host.mu.Lock()
	defer h.host.mu.Unlock()
	return slices.Clone(h.host.events)
}

func TestCall_RetriesAreSafeAndAnEndedIDCanBeReused(t *testing.T) {
	h := start(t, nil)
	h.host.EnsureCall("c")
	h.host.EnsureCall("c")
	if got := h.ari.find("POST /bridges"); len(got) != 1 {
		t.Fatalf("bridges = %v", got)
	}
	ch := h.dial("c", "201")
	if _, err := h.host.AddParticipant("c", AddParticipantRequest{Target: "201"}); err != nil {
		t.Fatal(err)
	}
	if got := h.ari.originates(); len(got) != 1 {
		t.Fatalf("a repeated dial must not ring twice: %v", got)
	}
	h.ari.channelEvent(t, "StasisStart", ch)
	h.waitParticipant("c", "201", StatusJoined)
	h.host.EndCall("c")
	h.answer("c", "201")
	if h.callStatus("c") != CallActive {
		t.Fatal("reused call should be active")
	}
}

func TestCall_InvalidID(t *testing.T) {
	h := start(t, nil)
	for _, id := range []string{"", "A", "a b", "../x", "a.b", strings.Repeat("a", 65)} {
		if _, err := h.host.EnsureCall(id); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: err = %v", id, err)
		}
	}
}

func TestParticipant_Outcomes(t *testing.T) {
	cases := []struct {
		name       string
		answered   bool
		cause      int
		wantStatus string
	}{
		{"no answer", false, 19, StatusNoAnswer},
		{"busy", false, 17, StatusBusy},
		{"hangup after answering", true, 16, StatusLeft},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t, nil)
			ch := h.dial("c", "201")
			if tc.answered {
				h.ari.channelEvent(t, "StasisStart", ch)
				h.waitParticipant("c", "201", StatusJoined)
			}
			h.ari.channelEvent(t, "ChannelDestroyed", ch, "cause", tc.cause)
			h.waitParticipant("c", "201", tc.wantStatus)
			h.eventually("empty call to end", func() bool { return h.callStatus("c") == CallEnded })
			if h.ari.resources() != 0 {
				t.Fatal("left behind resources")
			}
		})
	}
}

func TestParticipant_LeavingDoesNotEndCallWhileOthersRemain(t *testing.T) {
	h := start(t, nil)
	a := h.answer("c", "201")
	h.answer("c", "202")
	h.ari.channelEvent(t, "ChannelDestroyed", a)
	h.waitParticipant("c", "201", StatusLeft)
	if h.callStatus("c") != CallActive {
		t.Fatal("call must continue; ending it is a behavior for scripts to choose")
	}
}

func TestParticipant_Remove(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "201")
	h.answer("c", "202")
	if err := h.host.RemoveParticipant("c", "201"); err != nil {
		t.Fatal(err)
	}
	if e, _ := h.host.Endpoint("201"); e.Call != "" {
		t.Fatalf("endpoint = %+v", e)
	}
	if err := h.host.RemoveParticipant("c", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if err := h.host.HangupEndpoint("202"); err != nil {
		t.Fatal(err)
	}
	h.eventually("call end", func() bool { return h.callStatus("c") == CallEnded })
	if err := h.host.HangupEndpoint("201"); err != nil {
		t.Fatalf("hanging up an idle endpoint is safe: %v", err)
	}
}

func TestParticipant_Commands(t *testing.T) {
	h := start(t, nil)
	ch := h.answer("c", "201")
	act := func(action string, params map[string]any) Command {
		t.Helper()
		cmd, err := h.host.ParticipantCommand("c", "201", CommandRequest{Action: action, Parameters: params})
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		return cmd
	}
	act("mute", nil)
	if !h.participant("c", "201").Muted {
		t.Fatal("not muted")
	}
	if len(h.ari.find("POST /channels/"+ch+"/mute")) != 1 {
		t.Fatal("mute not sent")
	}
	act("unmute", nil)
	if h.participant("c", "201").Muted {
		t.Fatal("still muted")
	}
	act("hold", nil)
	if !h.participant("c", "201").Held {
		t.Fatal("not held")
	}
	act("unhold", nil)
	cmd := act("dtmf", map[string]any{"digits": "12#"})
	if got := h.ari.lastQuery("POST /channels/" + ch + "/dtmf"); got != "dtmf=12%23" {
		t.Fatalf("dtmf query = %q", got)
	}
	got, err := h.host.GetCommand(cmd.ID)
	if err != nil || got.Status != "succeeded" {
		t.Fatalf("command = %+v, %v", got, err)
	}

	if _, err := h.host.ParticipantCommand("c", "201", CommandRequest{Action: "dtmf", Parameters: map[string]any{"digits": "x;rm"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad digits: %v", err)
	}
	if _, err := h.host.ParticipantCommand("c", "201", CommandRequest{Action: "explode"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad action: %v", err)
	}
	h.ari.channelEvent(t, "ChannelDtmfReceived", ch, "digit", "#")
	h.eventually("keypad press", func() bool { return h.participant("c", "201").LastDTMF == "#" })

	h.ari.fail("POST /channels/" + ch + "/mute")
	if cmd := act("mute", nil); cmd.Status != "failed" || cmd.Error == "" {
		t.Fatalf("command = %+v", cmd)
	}
}

func TestParticipant_CommandBeforeAnswerIsRefused(t *testing.T) {
	h := start(t, nil)
	h.dial("c", "201")
	if _, err := h.host.ParticipantCommand("c", "201", CommandRequest{Action: "mute"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v", err)
	}
}

func TestDialPolicy(t *testing.T) {
	h := start(t, nil)
	if _, err := h.host.AddParticipant("c", AddParticipantRequest{Target: "number:+15551234567"}); err != nil {
		t.Fatal(err)
	}
	if got := h.ari.originates(); !strings.Contains(got[0], "endpoint=PJSIP%2F%2B15551234567%40trunk1") {
		t.Fatalf("originate = %v", got)
	}

	before := len(h.ari.originates())
	cases := map[string]error{
		"number:+19005550000": ErrForbidden, // not on the allowlist
		"trunk1":              ErrForbidden, // trunks are not dialed directly
		"999":                 ErrInvalid,   // unknown
		"number:abc":          ErrInvalid,
		"PJSIP/201":           ErrInvalid, // raw dial strings are never accepted
	}
	for target, want := range cases {
		if _, err := h.host.AddParticipant("c", AddParticipantRequest{Target: target}); !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", target, err, want)
		}
	}
	if got := len(h.ari.originates()); got != before {
		t.Fatalf("a refused target was dialed: %d", got)
	}
	// A script only sees state, so a refusal has to appear there too.
	refused := h.participant("c", "n19005550000")
	if refused.Status != StatusFailed || !strings.Contains(refused.Reason, "allowlist") {
		t.Fatalf("refused = %+v", refused)
	}
}

func TestDialPolicy_OfflineEndpointFailsVisibly(t *testing.T) {
	h := start(t, nil)
	if _, err := h.host.AddParticipant("c", AddParticipantRequest{Target: "204"}); err == nil {
		t.Fatal("expected an error")
	}
	if p := h.participant("c", "204"); p.Status != StatusFailed || !strings.Contains(p.Reason, "offline") {
		t.Fatalf("participant = %+v", p)
	}
	h.eventually("call end", func() bool { return h.callStatus("c") == CallEnded })
	if h.ari.resources() != 0 {
		t.Fatal("left behind resources")
	}
}

func TestDialOverride_IsConfigData(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) {
		c.Endpoints["3000"] = EndpointConfig{Listen: PolicyOnCommand, Dial: "Local/601@from-internal"}
	})
	h.dial("c", "3000")
	if got := h.ari.originates(); !strings.Contains(got[0], "endpoint=Local%2F601%40from-internal") {
		t.Fatalf("originate = %v", got)
	}
}

func TestOriginateFailureIsVisibleAndCleansUp(t *testing.T) {
	h := start(t, nil)
	h.ari.fail("POST /channels")
	if _, err := h.host.AddParticipant("c", AddParticipantRequest{Target: "201"}); err != nil {
		t.Fatal(err)
	}
	if p := h.participant("c", "201"); p.Status != StatusFailed {
		t.Fatalf("participant = %+v", p)
	}
	h.eventually("call end", func() bool { return h.callStatus("c") == CallEnded })
	if h.ari.resources() != 0 {
		t.Fatal("left behind resources")
	}
}

func TestClose_RemovesSubscription(t *testing.T) {
	h := start(t, nil)
	h.host.Close()
	if got := h.ari.find("DELETE /applications/slidebolt/subscription"); len(got) != 1 {
		t.Fatalf("subscription left behind: %v", got)
	}
}

func TestTimeLimitEndsCall(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.MaxCall = 80 * time.Millisecond })
	h.answer("c", "201")
	h.eventually("limit", func() bool { return h.callStatus("c") == CallEnded })
	c, _ := h.host.GetCall("c")
	if c.Reason != "time limit reached" {
		t.Fatalf("call = %+v", c)
	}
	if h.ari.resources() != 0 {
		t.Fatal("left behind resources")
	}
}

func TestPlay(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "201")
	if err := h.host.Play("c", PlayRequest{Sound: "closed"}); err != nil {
		t.Fatal(err)
	}
	h.ari.send(t, map[string]any{"type": "PlaybackFinished", "playback": map[string]string{"id": "other"}})
	if err := h.host.Play("c", PlayRequest{Sound: "../../etc/passwd"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
	if err := h.host.Play("c", PlayRequest{Sound: "closed", ThenHangup: true}); err != nil {
		t.Fatal(err)
	}
	h.ari.send(t, map[string]any{"type": "PlaybackFinished", "playback": map[string]string{"id": "pb1"}})
	h.eventually("hang up after playback", func() bool { return h.callStatus("c") == CallEnded })
}

func TestListen_IdleOnCommandPlacesPrivateCallAndStopEndsIt(t *testing.T) {
	h := start(t, nil)
	if _, err := h.host.StartListen("3000"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("listen-3000") != CallActive {
		t.Fatal("expected a private call")
	}
	h.media.publish("asterisk.endpoint.3000", media.StatusLive, "", 2)
	h.eventually("live", func() bool {
		e, _ := h.host.Endpoint("3000")
		return e.Stream.Status == StreamLive && e.Stream.Viewers == 2
	})
	if err := h.host.StopListen("3000"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("listen-3000") != CallEnded || h.ari.resources() != 0 || h.media.stopCount() != 1 {
		t.Fatalf("call %s, resources %d, stops %d", h.callStatus("listen-3000"), h.ari.resources(), h.media.stopCount())
	}
	if e, _ := h.host.Endpoint("3000"); e.Stream.Status != StreamIdle || e.Stream.URL == "" {
		t.Fatalf("stream = %+v", e.Stream)
	}
}

// The reason streams are separate from calls: stopping one must not end a real
// conversation.
func TestListen_DuringRealCallStopLeavesCallRunning(t *testing.T) {
	h := start(t, nil)
	h.answer("front-door", "3000")
	h.answer("front-door", "202") // 202 allows being heard in a call
	if _, err := h.host.StartListen("3000"); err != nil {
		t.Fatal(err)
	}
	if got := h.ari.originates(); len(got) != 2 {
		t.Fatalf("listening must not place another call: %v", got)
	}
	if h.ari.mediaChannel() == "" {
		t.Fatal("no media channel")
	}
	if err := h.host.StopListen("3000"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("front-door") != CallActive {
		t.Fatal("stopping a stream ended the conversation")
	}
	if p := h.participant("front-door", "202"); p.Status != StatusJoined {
		t.Fatalf("participant = %+v", p)
	}
}

func TestListen_Policy(t *testing.T) {
	h := start(t, nil)
	if _, err := h.host.StartListen("201"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("never: %v", err)
	}
	if _, err := h.host.StartListen("trunk1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("trunk: %v", err)
	}
	if _, err := h.host.StartListen("202"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("in_call while idle: %v", err)
	}
	if got := h.ari.originates(); len(got) != 0 {
		t.Fatalf("refused listens placed a call: %v", got)
	}
	// A call with someone who forbids listening cannot be streamed.
	h.answer("c", "3000")
	h.answer("c", "201")
	if _, err := h.host.StartListen("3000"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mixed call: %v", err)
	}
	if _, err := h.host.StartCallStream("c"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mixed call stream: %v", err)
	}
}

func TestCallStream_DetachLeavesCall(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "3000")
	h.answer("c", "202")
	s, err := h.host.StartCallStream("c")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(s.URL, "asterisk.call.c") {
		t.Fatalf("stream = %+v", s)
	}
	if err := h.host.StopCallStream("c"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("c") != CallActive || h.ari.mediaChannel() != "" {
		t.Fatalf("call %s, media %q", h.callStatus("c"), h.ari.mediaChannel())
	}
}

func TestStreamFailure_EndsPrivateCallButNotRealOne(t *testing.T) {
	t.Run("private", func(t *testing.T) {
		h := start(t, nil)
		h.host.StartListen("3000")
		h.media.publish("asterisk.endpoint.3000", media.StatusError, "transcoder exited", 0)
		h.eventually("end", func() bool { return h.callStatus("listen-3000") == CallEnded })
		if e, _ := h.host.Endpoint("3000"); e.Stream.Status != StreamError || e.Stream.Error != "transcoder exited" {
			t.Fatalf("stream = %+v", e.Stream)
		}
		if h.ari.resources() != 0 {
			t.Fatal("left behind resources")
		}
	})
	t.Run("real", func(t *testing.T) {
		h := start(t, nil)
		h.answer("c", "3000")
		h.host.StartCallStream("c")
		h.media.publish("asterisk.call.c", media.StatusError, "media stalled", 0)
		h.eventually("stream error", func() bool {
			c, _ := h.host.GetCall("c")
			return c.Stream != nil && c.Stream.Status == StreamError
		})
		if h.callStatus("c") != CallActive {
			t.Fatal("a stream failure ended a conversation")
		}
	})
}

func TestStream_StaleMediaEventIgnored(t *testing.T) {
	h := start(t, nil)
	h.host.StartListen("3000")
	h.media.mu.Lock()
	fn := h.media.handlers["asterisk.endpoint.3000"]
	h.media.mu.Unlock()
	fn(media.StatusEvent{SessionID: "old", Status: media.StatusError, Error: "x"})
	if h.callStatus("listen-3000") != CallActive {
		t.Fatal("stale event ended the call")
	}
}

func TestMediaServiceRestart(t *testing.T) {
	h := start(t, nil)
	h.host.StartListen("3000")
	h.host.OnMediaStarted()
	if h.callStatus("listen-3000") != CallEnded {
		t.Fatal("private call must end")
	}
	if e, _ := h.host.Endpoint("3000"); e.Stream.Error != "media service restarted" || e.Stream.URL == "" {
		t.Fatalf("stream = %+v", e.Stream)
	}
	if h.media.stopCount() != 0 {
		t.Fatal("a restarted service has no session to stop")
	}
}

func TestMediaDown_ReportedAndRecovers(t *testing.T) {
	h := start(t, nil)
	h.media.mu.Lock()
	h.media.startErr = errors.New("no free RTP port")
	h.media.mu.Unlock()
	if _, err := h.host.StartListen("3000"); err == nil {
		t.Fatal("expected error")
	}
	if e, _ := h.host.Endpoint("3000"); e.Stream.Status != StreamError || !strings.Contains(e.Stream.Error, "no free RTP port") {
		t.Fatalf("stream = %+v", e.Stream)
	}
	if h.ari.resources() != 0 || h.callStatus("listen-3000") != CallEnded {
		t.Fatal("failure left a call open")
	}
}

func TestStartup_RemovesOnlyOurLeftovers(t *testing.T) {
	h := start(t, func(_ *Config, f *fakeARI) {
		f.channels[resourcePrefix+"part-old"] = true
		f.channels["1234.5"] = true
		f.bridges[resourcePrefix+"call-old"] = true
		f.bridges["somebody-elses"] = true
	})
	h.eventually("cleanup", func() bool { return h.ari.resources() == 2 })
	h.ari.mu.Lock()
	defer h.ari.mu.Unlock()
	if !h.ari.channels["1234.5"] || !h.ari.bridges["somebody-elses"] {
		t.Fatal("cleanup touched resources it does not own")
	}
}

func TestLostAsteriskEndsCallsAndClose(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "201")
	h.ari.drop()
	h.eventually("end", func() bool { return h.callStatus("c") == CallEnded })
	c, _ := h.host.GetCall("c")
	if c.Reason != "lost connection to asterisk" {
		t.Fatalf("call = %+v", c)
	}
	if h.host.GetSnapshot().Server.Connected {
		t.Fatal("still connected")
	}
	h.eventually("reconnect", func() bool { return h.host.GetSnapshot().Server.Connected })
}

func TestCloseEndsCalls(t *testing.T) {
	h := start(t, nil)
	h.answer("c", "201")
	h.host.Close()
	if h.callStatus("c") != CallEnded || h.ari.resources() != 0 {
		t.Fatal("close left a call open")
	}
}

func (h *harness) transcript(call string) Transcript {
	h.t.Helper()
	c, err := h.host.GetCall(call)
	if err != nil {
		h.t.Fatal(err)
	}
	if c.Transcript == nil {
		h.t.Fatalf("no transcript on %+v", c)
	}
	return *c.Transcript
}

func TestTranscription_TextReachesTheCall(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall} })
	h.answer("voice", "201")
	if _, err := h.host.StartTranscription("voice"); err != nil {
		t.Fatal(err)
	}
	if got := h.transcript("voice").Status; got != TranscriptStarting {
		t.Fatalf("status = %s", got)
	}
	if !strings.Contains(h.ari.lastQuery("POST /channels/externalMedia"), "external_host=10.0.0.2%3A41100") {
		t.Fatalf("audio is not going to sb-speech: %s", h.ari.lastQuery("POST /channels/externalMedia"))
	}
	h.ari.channelEvent(t, "StasisStart", h.ari.speechChannel())
	h.eventually("listening", func() bool { return h.transcript("voice").Status == TranscriptListening })
	h.speech.publish(speech.Event{Type: speech.EventTranscript, Seq: 1, Text: "turn the lights on"})
	h.eventually("text", func() bool { got := h.transcript("voice"); return got.Text == "turn the lights on" && got.Seq == 1 })
}

func TestTranscription_StopLeavesCallRunning(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall} })
	h.answer("voice", "201")
	h.host.StartTranscription("voice")
	if err := h.host.StopTranscription("voice"); err != nil {
		t.Fatal(err)
	}
	if h.callStatus("voice") != CallActive || h.participant("voice", "201").Status != StatusJoined {
		t.Fatal("stopping transcription affected the call")
	}
	if h.speech.stopCount() != 1 {
		t.Fatalf("speech stops = %d", h.speech.stopCount())
	}
	if got := h.transcript("voice"); got.Status != TranscriptIdle {
		t.Fatalf("transcript = %+v", got)
	}
	// It can start again, and a repeated start does nothing extra.
	h.host.StartTranscription("voice")
	h.host.StartTranscription("voice")
	if h.speech.starts != 2 {
		t.Fatalf("speech starts = %d", h.speech.starts)
	}
}

func TestTranscription_EndsWithCall(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall} })
	h.answer("voice", "201")
	h.host.StartTranscription("voice")
	h.host.EndCall("voice")
	if h.speech.stopCount() != 1 || h.ari.resources() != 0 {
		t.Fatalf("stops %d, resources %d", h.speech.stopCount(), h.ari.resources())
	}
}

func TestTranscription_PolicyApplies(t *testing.T) {
	h := start(t, nil)
	h.answer("voice", "201") // 201 forbids listening by default
	if _, err := h.host.StartTranscription("voice"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("err = %v", err)
	}
	if h.speech.starts != 0 {
		t.Fatal("audio was sent to sb-speech against policy")
	}
}

func TestTranscription_FailuresAreVisibleAndLeaveCallUp(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall} })
	h.answer("voice", "201")
	h.speech.startErr = errors.New("speech service is disabled")
	if _, err := h.host.StartTranscription("voice"); err == nil {
		t.Fatal("expected an error")
	}
	if got := h.transcript("voice"); got.Status != TranscriptError || !strings.Contains(got.Error, "disabled") {
		t.Fatalf("transcript = %+v", got)
	}
	h.speech.startErr = nil
	h.host.StartTranscription("voice")
	h.speech.publish(speech.Event{Type: speech.EventError, Error: "model broken"})
	h.eventually("error", func() bool { return h.transcript("voice").Error == "model broken" })
	h.host.OnSpeechStarted()
	if got := h.transcript("voice"); got.Status != TranscriptError || got.Error != "speech service restarted" {
		t.Fatalf("transcript = %+v", got)
	}
	if h.callStatus("voice") != CallActive {
		t.Fatal("a speech failure ended the call")
	}
}

func TestTranscription_MediaChannelLost(t *testing.T) {
	h := start(t, func(c *Config, _ *fakeARI) { c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall} })
	h.answer("voice", "201")
	h.host.StartTranscription("voice")
	ch := h.ari.speechChannel()
	if ch == "" {
		t.Fatal("no speech channel")
	}
	h.ari.channelEvent(t, "StasisStart", ch)
	h.eventually("bridged", func() bool { return len(h.ari.find("POST /bridges/")) == 2 })
	h.ari.channelEvent(t, "ChannelDestroyed", ch)
	h.eventually("error", func() bool { return h.transcript("voice").Error == "media channel ended" })
	if h.callStatus("voice") != CallActive {
		t.Fatal("call must continue")
	}
}

// --- API ---

func (h *harness) request(method, path, body string, token ...string) (int, string, http.Header) {
	h.t.Helper()
	req, _ := http.NewRequest(method, h.api.URL+path, bytes.NewBufferString(body))
	if len(token) == 0 {
		req.Header.Set("Authorization", "Bearer tok")
	} else if token[0] != "" {
		req.Header.Set("Authorization", "Bearer "+token[0])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String(), resp.Header
}

func TestAPI_Auth(t *testing.T) {
	h := start(t, nil)
	if code, _, _ := h.request("GET", "/v1/health", "", ""); code != 200 {
		t.Fatalf("health = %d", code)
	}
	for _, token := range []string{"", "wrong"} {
		if code, _, _ := h.request("GET", "/v1/snapshot", "", token); code != 401 {
			t.Fatalf("token %q: %d", token, code)
		}
		if code, _, _ := h.request("POST", "/v1/calls/c/participants", `{"target":"201"}`, token); code != 401 {
			t.Fatalf("token %q: %d", token, code)
		}
	}
	if got := h.ari.originates(); len(got) != 0 {
		t.Fatal("an unauthenticated request placed a call")
	}
}

func TestAPI_Flow(t *testing.T) {
	h := start(t, nil)
	check := func(method, path, body string, want int) string {
		t.Helper()
		code, out, _ := h.request(method, path, body)
		if code != want {
			t.Fatalf("%s %s: %d, want %d: %s", method, path, code, want, out)
		}
		return out
	}
	check("GET", "/v1/openapi.json", "", 200)
	check("GET", "/v1/endpoints", "", 200)
	if out := check("GET", "/v1/trunks", "", 200); !strings.Contains(out, "trunk1") {
		t.Fatalf("trunks = %s", out)
	}
	if out := check("GET", "/v1/endpoints/201", "", 200); !strings.Contains(out, "Kitchen Phone") {
		t.Fatalf("endpoint = %s", out)
	}
	check("GET", "/v1/endpoints/nope", "", 404)
	check("GET", "/v1/calls/nope", "", 404)
	check("PUT", "/v1/calls/c", "", 200)
	check("PUT", "/v1/calls/BAD", "", 400)
	check("GET", "/v1/calls", "", 200)
	code, _, header := h.request("POST", "/v1/calls/c/participants", `{"target":"201","ringSeconds":10}`)
	if code != 202 || header.Get("Location") != "/v1/calls/c/participants/201" {
		t.Fatalf("add: %d %v", code, header)
	}
	if got := h.ari.originates(); !strings.Contains(got[0], "timeout=10") {
		t.Fatalf("originate = %v", got)
	}
	check("GET", "/v1/calls/c/participants/201", "", 200)
	check("GET", "/v1/calls/c/participants/nope", "", 404)
	check("POST", "/v1/calls/c/participants", `not json`, 400)
	check("POST", "/v1/calls/c/participants", `{"target":"number:+19005550000"}`, 403)
	check("POST", "/v1/calls/c/participants", `{"target":"999"}`, 400)
	check("POST", "/v1/calls/c/participants/201/commands", `{"action":"transfer"}`, 501)
	check("POST", "/v1/calls/c/recordings", `{}`, 501)
	check("POST", "/v1/calls/c/participants/201/commands", `{"action":"mute"}`, 409)
	h.ari.channelEvent(t, "StasisStart", h.ari.partChannels()[0])
	h.waitParticipant("c", "201", StatusJoined)
	code, out, header := h.request("POST", "/v1/calls/c/participants/201/commands", `{"action":"mute"}`)
	if code != 202 || !strings.HasPrefix(header.Get("Location"), "/v1/commands/") || !strings.Contains(out, "succeeded") {
		t.Fatalf("command: %d %s", code, out)
	}
	check("GET", header.Get("Location"), "", 200)
	check("GET", "/v1/commands/nope", "", 404)
	check("POST", "/v1/calls/c/playbacks", `{"sound":"closed"}`, 202)
	check("POST", "/v1/calls/c/streams", "", 403) // 201 forbids being heard
	check("POST", "/v1/calls/c/transcription", "", 403)
	check("DELETE", "/v1/calls/nope/transcription", "", 404)
	check("DELETE", "/v1/calls/c/participants/201", "", 204)
	check("DELETE", "/v1/calls/c", "", 204)
	check("POST", "/v1/endpoints/201/listen", "", 403)
	check("POST", "/v1/endpoints/3000/listen", "", 202)
	check("DELETE", "/v1/endpoints/3000/listen", "", 204)
	check("POST", "/v1/endpoints/201/hangup", "", 204)
	check("POST", "/v1/server/reload", "", 204)
}

func TestAPI_EventsReplayFollowAndExpire(t *testing.T) {
	h := start(t, nil)
	snap := h.host.GetSnapshot()
	code, _, _ := h.request("GET", "/v1/events?streamId=wrong&after=0", "")
	if code != 409 {
		t.Fatalf("wrong stream: %d", code)
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/events?streamId=%s&after=%d", h.api.URL, snap.StreamID, snap.Sequence), nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	h.host.EnsureCall("c")
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if out := string(buf[:n]); !strings.Contains(out, "event: call.updated") || !strings.Contains(out, `"id":"c"`) {
		t.Fatalf("event = %s", out)
	}
}

func TestEvents_CursorExpiresWhenTooOld(t *testing.T) {
	h := start(t, nil)
	h.host.mu.Lock()
	for range maxEvents + 5 {
		h.host.emit(EventServer, Server{})
	}
	h.host.mu.Unlock()
	if code, _, _ := h.request("GET", "/v1/events?after=1", ""); code != 409 {
		t.Fatalf("code = %d", code)
	}
}

func TestParseCallerName(t *testing.T) {
	cases := map[string]string{
		`"Intercom" <3000>`:     "Intercom",
		`"Kitchen Phone" <201>`: "Kitchen Phone",
		`<unknown>`:             "",
		``:                      "",
		`Plain Name <123>`:      "Plain Name",
	}
	for in, want := range cases {
		if got := parseCallerName(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

func TestConfig(t *testing.T) {
	if err := (Config{}).Enabled(); err == nil {
		t.Fatal("empty config should be disabled")
	}
	t.Setenv("ASTERISK_HOST_ENDPOINTS", `{"3000":{"listen":"sometimes"}}`)
	if _, err := LoadConfig(); err == nil {
		t.Fatal("an unknown listen policy must be refused")
	}
	t.Setenv("ASTERISK_HOST_ENDPOINTS", `{"3000":{"name":"X","listen":"on_command"}}`)
	t.Setenv("ASTERISK_HOST_TRUNKS", "a, b")
	t.Setenv("ASTERISK_HOST_DIAL_ALLOW", "+1")
	c, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.Endpoints["3000"].Name != "X" || !c.Trunks["b"] || !c.DialAllow["+1"] || c.Bind == "" {
		t.Fatalf("config = %+v", c)
	}
}
