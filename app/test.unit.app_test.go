package app

import (
	"encoding/json"
	"testing"

	"github.com/slidebolt/asterisk-host/internal/host"
	messenger "github.com/slidebolt/sb-messenger-sdk"
)

func TestHello(t *testing.T) {
	h := New().Hello()
	if h.ID != ServiceID || len(h.DependsOn) != 3 || h.DependsOn[2] != "speech" {
		t.Fatalf("hello = %+v", h)
	}
	if h.Setup == nil || h.Setup.Mode != "dependency" {
		t.Fatalf("setup = %+v", h.Setup)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
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
