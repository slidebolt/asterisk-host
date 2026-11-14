package host

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func withTextLine(c *Config, _ *fakeARI) {
	c.Services = map[string]ServiceConfig{
		"4000": {Name: "Agent", Call: "agent", Text: true},
		"4001": {Name: "Calls only", Call: "other"},
	}
}

// text sends what Asterisk sends when an endpoint sends a text message.
func (h *harness) text(from, to, body string) {
	h.t.Helper()
	h.ari.send(h.t, map[string]any{
		"type":     "TextMessageReceived",
		"endpoint": map[string]any{"technology": "PJSIP", "resource": from, "state": "online"},
		"message":  map[string]any{"from": "<sip:" + from + "@pbx>", "to": "pjsip:" + to + "@pbx", "body": body},
	})
}

func (h *harness) inbox(id string) Inbox {
	h.t.Helper()
	e, err := h.host.Endpoint(id)
	if err != nil || e.Inbox == nil {
		h.t.Fatalf("%s has no inbox: %+v %v", id, e, err)
	}
	return *e.Inbox
}

// settle gives the host time to handle anything already sent, for checks that
// something did not happen. A marker message proves earlier ones were handled.
func (h *harness) settle(from string) {
	h.t.Helper()
	before := h.inbox("4000").Seq
	h.text(from, "4000", "marker")
	h.eventually("marker", func() bool {
		inbox := h.inbox("4000")
		return inbox.Seq > before && inbox.Text == "marker"
	})
}

func TestText_ServiceWithTextHasAnInbox(t *testing.T) {
	h := start(t, withTextLine)
	if in := h.inbox("4000"); in.Seq != 0 {
		t.Fatalf("new inbox = %+v", in)
	}
	if e, _ := h.host.Endpoint("4001"); e.Inbox != nil {
		t.Fatal("a service without text has an inbox")
	}
}

func TestText_FromAnExtensionReachesTheInbox(t *testing.T) {
	h := start(t, withTextLine)
	h.text("201", "4000", "  turn on the lamp\x07 ")
	h.eventually("message in inbox", func() bool { return h.inbox("4000").Seq == 1 })
	in := h.inbox("4000")
	if in.From != "201" || in.Text != "turn on the lamp" || in.At == "" {
		t.Fatalf("inbox = %+v", in)
	}
	h.text("202", "4000", "second")
	h.eventually("second message", func() bool { return h.inbox("4000").Seq == 2 })
	if in := h.inbox("4000"); in.From != "202" || in.Text != "second" {
		t.Fatalf("inbox = %+v", in)
	}
}

func TestText_IsIgnoredUnlessForAServiceWithTextFromAnExtension(t *testing.T) {
	h := start(t, withTextLine)
	h.text("201", "202", "private, between phones") // not for SlideBolt
	h.text("201", "4001", "service without text")
	h.text("201", "4999", "unknown number")
	h.text("trunk1", "4000", "from the trunk")
	h.text("4000", "4000", "from a service")
	h.text("999", "4000", "from an unknown endpoint")
	h.text("201", "4000", strings.Repeat("x", maxMessageLength+1))
	h.text("201", "4000", " \x01 ")
	h.settle("201")
	if in := h.inbox("4000"); in.Seq != 1 || in.Text != "marker" {
		t.Fatalf("something got through: %+v", in)
	}
}

func TestText_RateLimitPerSender(t *testing.T) {
	h := start(t, withTextLine)
	for i := 0; i < messageRateLimit+5; i++ {
		h.text("201", "4000", "spam")
	}
	h.settle("202") // a different sender is not limited
	if in := h.inbox("4000"); in.Seq != messageRateLimit+1 {
		t.Fatalf("seq = %d, want %d", in.Seq, messageRateLimit+1)
	}
	// The window slides.
	h.host.mu.Lock()
	for i := range h.host.msgTimes["201"] {
		h.host.msgTimes["201"][i] = time.Now().Add(-2 * messageRateWindow)
	}
	h.host.mu.Unlock()
	h.settle("201")
}

func (h *harness) postMessage(service string, body any) (int, Command) {
	h.t.Helper()
	data, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, h.api.URL+"/v1/endpoints/"+service+"/messages", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var cmd Command
	json.NewDecoder(resp.Body).Decode(&cmd)
	return resp.StatusCode, cmd
}

func TestText_ReplyGoesToTheExtensionFromTheService(t *testing.T) {
	h := start(t, withTextLine)
	status, cmd := h.postMessage("4000", MessageRequest{To: "201", Text: "The lamp is on."})
	if status != http.StatusAccepted || cmd.Status != "succeeded" {
		t.Fatalf("status %d, %+v", status, cmd)
	}
	q, _ := url.ParseQuery(h.ari.lastQuery("PUT /endpoints/sendMessage"))
	if q.Get("to") != "pjsip:201" || q.Get("from") != "4000" || q.Get("body") != "The lamp is on." {
		t.Fatalf("sent %v", q)
	}
	if got, err := h.host.GetCommand(cmd.ID); err != nil || got.Status != "succeeded" {
		t.Fatalf("command = %+v %v", got, err)
	}
}

func TestText_SendingIsRestricted(t *testing.T) {
	h := start(t, withTextLine)
	cases := []struct {
		service string
		req     MessageRequest
		want    int
	}{
		{"4000", MessageRequest{To: "trunk1", Text: "x"}, http.StatusForbidden},
		{"4000", MessageRequest{To: "+15551234567", Text: "x"}, http.StatusForbidden},
		{"4000", MessageRequest{To: "4001", Text: "x"}, http.StatusForbidden},
		{"4000", MessageRequest{To: "999", Text: "x"}, http.StatusForbidden},
		{"4001", MessageRequest{To: "201", Text: "x"}, http.StatusForbidden},
		{"201", MessageRequest{To: "202", Text: "x"}, http.StatusNotFound},
		{"4000", MessageRequest{To: "201", Text: " "}, http.StatusBadRequest},
		{"4000", MessageRequest{To: "201", Text: strings.Repeat("x", maxMessageLength+1)}, http.StatusBadRequest},
	}
	for _, c := range cases {
		if status, _ := h.postMessage(c.service, c.req); status != c.want {
			t.Errorf("%s -> %s: status %d, want %d", c.service, c.req.To, status, c.want)
		}
	}
	if n := len(h.ari.find("PUT /endpoints/sendMessage")); n != 0 {
		t.Fatalf("%d messages reached Asterisk", n)
	}
}

func TestText_FailedSendIsReported(t *testing.T) {
	h := start(t, withTextLine)
	h.ari.fail("PUT /endpoints/sendMessage")
	status, cmd := h.postMessage("4000", MessageRequest{To: "201", Text: "hi"})
	if status != http.StatusAccepted || cmd.Status != "failed" || cmd.Error == "" {
		t.Fatalf("status %d, %+v", status, cmd)
	}
}

func TestMessageUser(t *testing.T) {
	for in, want := range map[string]string{
		"pjsip:4000@pbx":             "4000",
		`"Agent" <sip:4000@pbx>`:     "4000",
		"<sip:4000@pbx;transport=x>": "4000",
		"sip:4000":                   "4000",
		"4000":                       "4000",
	} {
		if got := messageUser(in); got != want {
			t.Errorf("messageUser(%q) = %q, want %q", in, got, want)
		}
	}
}
