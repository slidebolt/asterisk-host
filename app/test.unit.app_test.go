package app

import (
	"encoding/json"
	"testing"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

func TestHello(t *testing.T) {
	h := New().Hello()
	if h.ID != ServiceID || len(h.DependsOn) != 2 {
		t.Fatalf("hello = %+v", h)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
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
