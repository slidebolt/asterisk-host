// Package ari is a minimal client for the Asterisk REST Interface, covering
// what is needed to bridge a call to an external media channel.
package ari

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	requestTimeout = 10 * time.Second
	minBackoff     = time.Second
	maxBackoff     = 30 * time.Second
	pingInterval   = 30 * time.Second
	pongTimeout    = 75 * time.Second
)

// ErrNotFound means the resource does not exist.
var ErrNotFound = errors.New("ari: not found")

// Client talks to one Asterisk instance.
type Client struct {
	baseURL string
	user    string
	pass    string
	http    *http.Client
}

// New creates a Client. baseURL is the server root, such as http://host:8088.
func New(baseURL, user, pass string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		user:    user,
		pass:    pass,
		http:    &http.Client{Timeout: requestTimeout},
	}
}

// Channel is the part of an ARI channel this package uses.
type Channel struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// Endpoint is an ARI endpoint.
type Endpoint struct {
	Technology string   `json:"technology"`
	Resource   string   `json:"resource"`
	State      string   `json:"state"`
	ChannelIDs []string `json:"channel_ids"`
}

// Info is the part of the server information this package uses.
type Info struct {
	System struct {
		Version string `json:"version"`
	} `json:"system"`
}

// Playback is the part of an ARI playback this package uses.
type Playback struct {
	ID string `json:"id"`
}

// Bridge is the part of an ARI bridge this package uses.
type Bridge struct {
	ID string `json:"id"`
}

// Event is an ARI event.
type Event struct {
	Type     string   `json:"type"`
	Channel  Channel  `json:"channel"`
	Endpoint Endpoint `json:"endpoint"`
	Playback Playback `json:"playback"`
	Digit    string   `json:"digit"`
	Cause    int      `json:"cause"`
	CauseTxt string   `json:"cause_txt"`
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, out any) error {
	return c.doBody(ctx, method, path, query, nil, out)
}

func (c *Client) doBody(ctx context.Context, method, path string, query url.Values, in, out any) error {
	u := c.baseURL + "/ari" + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var body io.Reader = http.NoBody
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("ari: encode %s %s: %w", method, path, err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return fmt.Errorf("ari: build %s %s: %w", method, path, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(c.user, c.pass)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ari: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("ari: read %s %s: %w", method, path, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ari: %s %s: HTTP %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("ari: decode %s %s: %w", method, path, err)
		}
	}
	return nil
}

// CreateBridge creates a mixing bridge.
func (c *Client) CreateBridge(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodPost, "/bridges", url.Values{"type": {"mixing"}, "bridgeId": {id}}, nil)
}

// DestroyBridge removes a bridge. A missing bridge is not an error.
func (c *Client) DestroyBridge(ctx context.Context, id string) error {
	return ignoreNotFound(c.do(ctx, http.MethodDelete, "/bridges/"+url.PathEscape(id), nil, nil))
}

// AddChannel adds a channel to a bridge.
func (c *Client) AddChannel(ctx context.Context, bridgeID, channelID string) error {
	path := "/bridges/" + url.PathEscape(bridgeID) + "/addChannel"
	return c.do(ctx, http.MethodPost, path, url.Values{"channel": {channelID}}, nil)
}

// ExternalMedia creates a channel that sends ulaw RTP to host:port.
func (c *Client) ExternalMedia(ctx context.Context, channelID, app, host string, port int) error {
	return c.do(ctx, http.MethodPost, "/channels/externalMedia", url.Values{
		"channelId":     {channelID},
		"app":           {app},
		"external_host": {host + ":" + strconv.Itoa(port)},
		"format":        {"ulaw"},
	}, nil)
}

// Originate calls an endpoint and places the answered channel in a Stasis app.
// limitSeconds makes Asterisk itself hang up the channel, so a call cannot outlive
// this process by more than that.
func (c *Client) Originate(ctx context.Context, channelID, endpoint, app string, timeoutSeconds, limitSeconds int) error {
	query := url.Values{
		"channelId": {channelID},
		"endpoint":  {endpoint},
		"app":       {app},
		"timeout":   {strconv.Itoa(timeoutSeconds)},
	}
	body := map[string]any{"variables": map[string]string{"TIMEOUT(absolute)": strconv.Itoa(limitSeconds)}}
	return c.doBody(ctx, http.MethodPost, "/channels", query, body, nil)
}

// RemoveChannel takes a channel out of a bridge.
func (c *Client) RemoveChannel(ctx context.Context, bridgeID, channelID string) error {
	path := "/bridges/" + url.PathEscape(bridgeID) + "/removeChannel"
	return ignoreNotFound(c.do(ctx, http.MethodPost, path, url.Values{"channel": {channelID}}, nil))
}

// PlayOnBridge plays a sound into a bridge and returns the playback ID.
func (c *Client) PlayOnBridge(ctx context.Context, bridgeID, sound string) (string, error) {
	var out Playback
	path := "/bridges/" + url.PathEscape(bridgeID) + "/play"
	err := c.do(ctx, http.MethodPost, path, url.Values{"media": {"sound:" + sound}}, &out)
	return out.ID, err
}

// Mute mutes or unmutes both directions of a channel.
func (c *Client) Mute(ctx context.Context, id string, on bool) error {
	method := http.MethodPost
	if !on {
		method = http.MethodDelete
	}
	return c.do(ctx, method, "/channels/"+url.PathEscape(id)+"/mute", url.Values{"direction": {"both"}}, nil)
}

// Hold places a channel on hold, or takes it off.
func (c *Client) Hold(ctx context.Context, id string, on bool) error {
	method := http.MethodPost
	if !on {
		method = http.MethodDelete
	}
	return c.do(ctx, method, "/channels/"+url.PathEscape(id)+"/hold", nil, nil)
}

// DTMF sends keypad tones to a channel.
func (c *Client) DTMF(ctx context.Context, id, digits string) error {
	return c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(id)+"/dtmf", url.Values{"dtmf": {digits}}, nil)
}

// Endpoints lists endpoints for one technology.
func (c *Client) Endpoints(ctx context.Context, technology string) ([]Endpoint, error) {
	var out []Endpoint
	err := c.do(ctx, http.MethodGet, "/endpoints/"+url.PathEscape(technology), nil, &out)
	return out, err
}

// EndpointCallerID returns the configured caller ID of a PJSIP endpoint, such as
// "Intercom" <3000>. It returns an empty string if none is set.
func (c *Client) EndpointCallerID(ctx context.Context, id string) (string, error) {
	var attrs []struct {
		Attribute string `json:"attribute"`
		Value     string `json:"value"`
	}
	path := "/asterisk/config/dynamic/res_pjsip/endpoint/" + url.PathEscape(id)
	if err := c.do(ctx, http.MethodGet, path, nil, &attrs); err != nil {
		return "", err
	}
	for _, a := range attrs {
		if a.Attribute == "callerid" {
			return a.Value, nil
		}
	}
	return "", nil
}

// Info returns server information.
func (c *Client) Info(ctx context.Context) (Info, error) {
	var out Info
	err := c.do(ctx, http.MethodGet, "/asterisk/info", url.Values{"only": {"system"}}, &out)
	return out, err
}

// SubscribeEndpoints subscribes an app to state changes of all endpoints.
func (c *Client) SubscribeEndpoints(ctx context.Context, app string) error {
	path := "/applications/" + url.PathEscape(app) + "/subscription"
	return c.do(ctx, http.MethodPost, path, url.Values{"eventSource": {"endpoint:PJSIP"}}, nil)
}

// UnsubscribeEndpoints undoes SubscribeEndpoints. Asterisk keeps an app listed while
// it has subscriptions, even with nothing connected.
func (c *Client) UnsubscribeEndpoints(ctx context.Context, app string) error {
	path := "/applications/" + url.PathEscape(app) + "/subscription"
	return ignoreNotFound(c.do(ctx, http.MethodDelete, path, url.Values{"eventSource": {"endpoint:PJSIP"}}, nil))
}

// ReloadModule reloads one Asterisk module.
func (c *Client) ReloadModule(ctx context.Context, module string) error {
	return c.do(ctx, http.MethodPut, "/asterisk/modules/"+url.PathEscape(module), nil, nil)
}

// Hangup ends a channel. A missing channel is not an error.
func (c *Client) Hangup(ctx context.Context, id string) error {
	return ignoreNotFound(c.do(ctx, http.MethodDelete, "/channels/"+url.PathEscape(id), nil, nil))
}

// Channels lists all channels.
func (c *Client) Channels(ctx context.Context) ([]Channel, error) {
	var out []Channel
	err := c.do(ctx, http.MethodGet, "/channels", nil, &out)
	return out, err
}

// Bridges lists all bridges.
func (c *Client) Bridges(ctx context.Context) ([]Bridge, error) {
	var out []Bridge
	err := c.do(ctx, http.MethodGet, "/bridges", nil, &out)
	return out, err
}

func ignoreNotFound(err error) error {
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// Listener receives connection changes and events from RunEvents.
type Listener struct {
	OnConnected    func()
	OnDisconnected func()
	OnEvent        func(Event)
}

// RunEvents registers app with Asterisk and delivers its events until ctx ends,
// reconnecting as needed. A Stasis app only exists while this is connected.
func (c *Client) RunEvents(ctx context.Context, app string, l Listener) {
	backoff := minBackoff
	for ctx.Err() == nil {
		connected, err := c.runEventsOnce(ctx, app, l)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = minBackoff
		}
		log.Printf("ari: events disconnected: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (c *Client) runEventsOnce(ctx context.Context, app string, l Listener) (bool, error) {
	u, err := url.Parse(c.baseURL + "/ari/events")
	if err != nil {
		return false, err
	}
	u.Scheme = strings.Replace(u.Scheme, "http", "ws", 1)
	u.RawQuery = url.Values{"app": {app}}.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.SetBasicAuth(c.user, c.pass)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), req.Header)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				conn.Close()
				return
			case <-stop:
				return
			case <-ticker.C:
				conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			}
		}
	}()
	conn.SetReadDeadline(time.Now().Add(pongTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongTimeout))
	})

	l.OnConnected()
	defer l.OnDisconnected()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return true, err
		}
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		var ev Event
		if json.Unmarshal(data, &ev) == nil {
			l.OnEvent(ev)
		}
	}
}
