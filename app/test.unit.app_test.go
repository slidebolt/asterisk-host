package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/slidebolt/asterisk-host/internal/host"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	runtime "github.com/slidebolt/sb-runtime"
)

type resourceEvent struct {
	id    string
	state string
}

type resourceRecorder struct{ events []resourceEvent }

func (r *resourceRecorder) Ready(id string) { r.events = append(r.events, resourceEvent{id, "ready"}) }
func (r *resourceRecorder) Degraded(id, _, _ string, _ bool) {
	r.events = append(r.events, resourceEvent{id, "degraded"})
}

var _ runtime.ResourceReporter = (*resourceRecorder)(nil)

func TestHello(t *testing.T) {
	h := New().Hello()
	if h.ID != ServiceID || len(h.DependsOn) != 3 || h.DependsOn[2] != "speech" {
		t.Fatalf("hello = %+v", h)
	}
	if h.Setup == nil || h.Setup.Mode != "dependency" {
		t.Fatalf("setup = %+v", h.Setup)
	}
	if len(h.Setup.Resources) != 1 || h.Setup.Resources[0].ID != asteriskResource {
		t.Fatalf("resources = %+v", h.Setup.Resources)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfiguredARIReportsHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ari/asterisk/info" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"system":{"version":"test"}}`))
	}))
	t.Setenv("ASTERISK_HOST_BIND", "127.0.0.1:0")
	t.Setenv("ASTERISK_HOST_TOKEN", "test-token")
	t.Setenv("ASTERISK_ARI_URL", server.URL)
	t.Setenv("ASTERISK_ARI_USER", "test-user")
	t.Setenv("ASTERISK_ARI_PASS", "test-password")
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()
	recorder := &resourceRecorder{}
	a := New()
	a.SetResourceReporter(recorder)
	if _, err := a.OnStart(map[string]json.RawMessage{"messenger": payload}); err != nil {
		t.Fatal(err)
	}
	server.Close()
	cfg, err := host.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkARI(cfg); err == nil {
		t.Fatal("closed ARI unexpectedly healthy")
	} else {
		a.resourceDegraded("ari_unavailable", err, true)
	}
	_ = a.OnShutdown()
	if len(recorder.events) != 2 || recorder.events[0] != (resourceEvent{asteriskResource, "ready"}) || recorder.events[1] != (resourceEvent{asteriskResource, "degraded"}) {
		t.Fatalf("events = %+v", recorder.events)
	}
}

func TestDependencyPayloadUsesLoopbackForWildcardBind(t *testing.T) {
	payload, err := dependencyPayload(host.Config{Bind: "0.0.0.0:47832", Token: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got["url"] != "http://127.0.0.1:47832" || got["token"] != "secret" {
		t.Fatalf("payload = %v", got)
	}
}

// A failed OnStart aborts sb-manager's startAll and stops every binary.
func TestOnStart_MissingConfigStaysUp(t *testing.T) {
	for _, name := range []string{"ASTERISK_HOST_TOKEN", "ASTERISK_ARI_URL", "ASTERISK_ARI_USER", "ASTERISK_ARI_PASS"} {
		t.Setenv(name, "")
	}
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()
	a := New()
	if _, err := a.OnStart(map[string]json.RawMessage{"messenger": payload}); err != nil {
		t.Fatalf("OnStart must not fail: %v", err)
	}
	if a.host != nil {
		t.Fatal("expected disabled")
	}
	if err := a.OnShutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestOnStart_BadEndpointConfigStaysUp(t *testing.T) {
	t.Setenv("ASTERISK_HOST_ENDPOINTS", "{not json")
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()
	a := New()
	if _, err := a.OnStart(map[string]json.RawMessage{"messenger": payload}); err != nil {
		t.Fatalf("OnStart must not fail: %v", err)
	}
	a.OnShutdown()
}
