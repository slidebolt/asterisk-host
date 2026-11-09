package ari

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRequests_ShapeAndAuth(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "u" || pass != "p" {
			t.Errorf("auth = %q %q %v", user, pass, ok)
		}
		got = append(got, r.Method+" "+r.URL.Path+"?"+r.URL.Query().Encode())
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := New(srv.URL+"/", "u", "p")
	ctx := context.Background()
	calls := []error{
		c.CreateBridge(ctx, "b"),
		c.ExternalMedia(ctx, "m", "app", "172.27.0.1", 41000, FormatG722),
		c.Originate(ctx, "c", "PJSIP/3000", "app", 30, 900),
		c.AddChannel(ctx, "b", "c"),
		c.Hangup(ctx, "c"),
		c.DestroyBridge(ctx, "b"),
	}
	for i, err := range calls {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	want := []string{
		"POST /ari/bridges?bridgeId=b&type=mixing",
		"POST /ari/channels/externalMedia?app=app&channelId=m&external_host=172.27.0.1%3A41000&format=g722",
		"POST /ari/channels?app=app&channelId=c&endpoint=PJSIP%2F3000&timeout=30",
		"POST /ari/bridges/b/addChannel?channel=c",
		"DELETE /ari/channels/c?",
		"DELETE /ari/bridges/b?",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOriginate_AsksAsteriskToEnforceTheLimit(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := make([]byte, 1024)
		n, _ := r.Body.Read(data)
		body = string(data[:n])
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	if err := New(srv.URL, "u", "p").Originate(context.Background(), "c", "PJSIP/1", "app", 30, 42); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, `"TIMEOUT(absolute)":"42"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestErrors(t *testing.T) {
	status := http.StatusNotFound
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", status)
	}))
	defer srv.Close()
	c := New(srv.URL, "u", "secret-pass")
	ctx := context.Background()
	if err := c.Hangup(ctx, "x"); err != nil {
		t.Errorf("hangup of missing channel: %v", err)
	}
	if err := c.DestroyBridge(ctx, "x"); err != nil {
		t.Errorf("destroy of missing bridge: %v", err)
	}
	if err := c.AddChannel(ctx, "b", "c"); !errors.Is(err, ErrNotFound) {
		t.Errorf("add channel err = %v", err)
	}
	status = http.StatusInternalServerError
	err := c.CreateBridge(ctx, "b")
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("create err = %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "secret-pass") {
		t.Error("error leaks the password")
	}
}

func TestRunEvents_DeliversReconnectsAndStops(t *testing.T) {
	var connects atomic.Int32
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("app") != "app" {
			t.Errorf("app query = %q", r.URL.RawQuery)
		}
		if _, pass, _ := r.BasicAuth(); pass != "p" {
			t.Errorf("no auth on websocket")
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connects.Add(1)
		conn.WriteMessage(websocket.TextMessage, []byte(`not json`))
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"StasisStart","channel":{"id":"c1"}}`))
	}))
	defer srv.Close()

	events := make(chan Event, 10)
	var connected, disconnected atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		New(srv.URL, "u", "p").RunEvents(ctx, "app", Listener{
			OnConnected:    func() { connected.Add(1) },
			OnDisconnected: func() { disconnected.Add(1) },
			OnEvent:        func(ev Event) { events <- ev },
		})
		close(done)
	}()

	for range 2 {
		select {
		case ev := <-events:
			if ev.Type != "StasisStart" || ev.Channel.ID != "c1" {
				t.Fatalf("event = %+v", ev)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no event, reconnect did not happen")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunEvents did not stop")
	}
	if connected.Load() < 2 || disconnected.Load() < 1 {
		t.Fatalf("connected=%d disconnected=%d", connected.Load(), disconnected.Load())
	}
}
