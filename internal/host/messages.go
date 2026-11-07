package host

import (
	"github.com/slidebolt/asterisk-host/internal/ari"

	"fmt"
	"log"
	"strings"
	"time"
	"unicode"
)

const (
	// maxMessageLength is the longest text message accepted or sent, in characters.
	maxMessageLength = 1000
	// messageRateLimit is how many texts one extension may send in messageRateWindow.
	messageRateLimit  = 10
	messageRateWindow = time.Minute
)

// onTextMessage takes a text message from an extension to a service number.
// Asterisk reports every message an extension sends, including ones between
// phones, so anything not addressed to a service with text on is ignored without
// reading or logging it. Callers hold mu.
func (h *Host) onTextMessage(ev ari.Event) {
	to := messageUser(ev.Message.To)
	svc, ok := h.cfg.Services[to]
	if !ok || !svc.Text {
		return
	}
	from := ev.Endpoint.Resource
	sender, ok := h.endpoints[from]
	if ev.Endpoint.Technology != "PJSIP" || !ok || sender.Kind != "extension" {
		log.Printf("asterisk-host: dropping text to %s: sender %q is not an extension", to, from)
		return
	}
	text := cleanText(ev.Message.Body)
	if text == "" {
		return
	}
	if len([]rune(text)) > maxMessageLength {
		log.Printf("asterisk-host: dropping text from %s to %s: longer than %d characters", from, to, maxMessageLength)
		return
	}
	if !h.allowMessage(from) {
		log.Printf("asterisk-host: dropping text from %s to %s: more than %d a minute", from, to, messageRateLimit)
		return
	}
	e, ok := h.endpoints[to]
	if !ok || e.Inbox == nil {
		return
	}
	e.Inbox.Seq++
	e.Inbox.From = from
	e.Inbox.Text = text
	e.Inbox.At = now()
	h.emitEndpoint(e)
}

// allowMessage applies the per-sender rate limit. Callers hold mu.
func (h *Host) allowMessage(from string) bool {
	cutoff := time.Now().Add(-messageRateWindow)
	recent := h.msgTimes[from][:0]
	for _, t := range h.msgTimes[from] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	if len(recent) >= messageRateLimit {
		h.msgTimes[from] = recent
		return false
	}
	h.msgTimes[from] = append(recent, time.Now())
	return true
}

// SendMessage sends a text message from a service number to an extension. Only
// services with text on may send, and only to extensions, never to trunks or
// outside numbers.
func (h *Host) SendMessage(service string, req MessageRequest) (Command, error) {
	text := cleanText(req.Text)
	if text == "" {
		return Command{}, fmt.Errorf("%w: text is empty", ErrInvalid)
	}
	if len([]rune(text)) > maxMessageLength {
		return Command{}, fmt.Errorf("%w: text is longer than %d characters", ErrInvalid, maxMessageLength)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	svc, ok := h.cfg.Services[service]
	if !ok {
		return Command{}, ErrNotFound
	}
	if !svc.Text {
		return Command{}, fmt.Errorf("%w: %s does not send text messages", ErrForbidden, service)
	}
	to, ok := h.endpoints[req.To]
	if !ok || to.Kind != "extension" {
		return Command{}, fmt.Errorf("%w: %q is not an extension", ErrForbidden, req.To)
	}
	if !h.server.Connected {
		return Command{}, ErrOffline
	}
	ctx, cancel := ariContext()
	defer cancel()
	cmd := Command{ID: randomID(), Status: "succeeded", Action: "message"}
	if err := h.ari.SendMessage(ctx, "pjsip:"+to.ID, service, text); err != nil {
		cmd.Status = "failed"
		cmd.Error = err.Error()
		log.Printf("asterisk-host: text from %s to %s: %v", service, to.ID, err)
	}
	h.recordCommand(&cmd)
	return cmd, nil
}

// messageUser is the user part of a message address such as pjsip:4000@pbx or
// "Name" <sip:4000@pbx>.
func messageUser(addr string) string {
	if i := strings.LastIndex(addr, "<"); i >= 0 {
		addr = addr[i+1:]
	}
	addr = strings.TrimSuffix(strings.TrimSpace(addr), ">")
	if i := strings.Index(addr, ":"); i >= 0 {
		addr = addr[i+1:]
	}
	if i := strings.IndexAny(addr, "@;"); i >= 0 {
		addr = addr[:i]
	}
	return addr
}

// cleanText removes control characters other than line breaks and tabs, and
// surrounding space.
func cleanText(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, s)
	return strings.TrimSpace(s)
}
