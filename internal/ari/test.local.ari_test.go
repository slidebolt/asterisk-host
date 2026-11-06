//go:build integration

// Integration tests that read from a real Asterisk. They place no calls.
//
// These require a running Asterisk instance. Configure via environment variables
// (see plugin-asterisk/.env.integration.example):
//
//	ASTERISK_ARI_URL=http://<host>:8088
//	ASTERISK_ARI_USER=<username>
//	ASTERISK_ARI_PASS=<password>
//
// Run:
//
//	go test -tags integration -v ./internal/ari/...
package ari

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func realClient(t *testing.T) (*Client, context.Context) {
	t.Helper()
	url, user, pass := os.Getenv("ASTERISK_ARI_URL"), os.Getenv("ASTERISK_ARI_USER"), os.Getenv("ASTERISK_ARI_PASS")
	if url == "" || user == "" || pass == "" {
		t.Fatal("ASTERISK_ARI_URL, ASTERISK_ARI_USER and ASTERISK_ARI_PASS are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return New(url, user, pass), ctx
}

func TestARI_ServerInfo(t *testing.T) {
	c, ctx := realClient(t)
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.System.Version == "" {
		t.Fatal("no version reported")
	}
	t.Logf("Asterisk %s", info.System.Version)
}

func TestARI_ListEndpoints(t *testing.T) {
	c, ctx := realClient(t)
	endpoints, err := c.Endpoints(ctx, "PJSIP")
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) == 0 {
		t.Fatal("no endpoints")
	}
	for _, e := range endpoints {
		if e.Resource == "" || e.State == "" {
			t.Errorf("incomplete endpoint: %+v", e)
		}
		callerID, err := c.EndpointCallerID(ctx, e.Resource)
		if err != nil {
			t.Errorf("caller id for %s: %v", e.Resource, err)
		}
		t.Logf("%s: %s %q", e.Resource, e.State, callerID)
	}
}

func TestARI_ListChannelsAndBridges(t *testing.T) {
	c, ctx := realClient(t)
	if _, err := c.Channels(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Bridges(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestARI_WrongPasswordIsRefused(t *testing.T) {
	url := os.Getenv("ASTERISK_ARI_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := New(url, "nobody", "wrong").Info(ctx)
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v", err)
	}
}
