package host

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	speech "github.com/slidebolt/sb-speech-sdk"
)

func withAgentLine(c *Config, _ *fakeARI) {
	c.Services = map[string]ServiceConfig{"4000": {Name: "Agent", Call: "agent"}}
	c.Endpoints["201"] = EndpointConfig{Listen: PolicyInCall}
}

// dialService sends what Asterisk sends when a phone dials a service number.
func (h *harness) dialService(channel, number, caller string) {
	h.t.Helper()
	h.ari.mu.Lock()
	h.ari.channels[channel] = true
	h.ari.mu.Unlock()
	h.ari.send(h.t, map[string]any{
		"type": "StasisStart", "args": []string{"service", number},
		"channel": map[string]any{"id": channel, "caller": map[string]string{"number": caller, "name": "Phone"}},
	})
}

// waitJoined waits for a participant of a call that may not exist yet.
func (h *harness) waitJoined(call, id string) {
	h.t.Helper()
	h.eventually(id+" in "+call, func() bool {
		c, err := h.host.GetCall(call)
		if err != nil {
			return false
		}
		for _, p := range c.Participants {
			if p.ID == id && p.Status == StatusJoined {
				return true
			}
		}
		return false
	})
}

func TestService_ListedAsAnEndpoint(t *testing.T) {
	h := start(t, withAgentLine)
	e, err := h.host.Endpoint("4000")
	if err != nil || e.Kind != "service" || e.Name != "Agent" || e.Status != "online" || e.Features.Listen != PolicyNever {
		t.Fatalf("service endpoint = %+v err=%v", e, err)
	}
}

func TestService_CallerIsAnsweredIntoTheServiceCall(t *testing.T) {
	h := start(t, withAgentLine)
	h.dialService("in-1", "4000", "201")
	h.waitJoined("agent", "201")

	if len(h.ari.find("POST /channels/in-1/answer")) != 1 {
		t.Fatal("caller was not answered")
	}
	if q := h.ari.lastQuery("POST /channels/in-1/variable"); !strings.Contains(q, "TIMEOUT%28absolute%29") || !strings.Contains(q, "value=65") {
		t.Fatalf("no Asterisk-side time limit: %q", q)
	}
	added := false
	for _, c := range h.ari.find("POST /bridges/") {
		if strings.HasSuffix(c, "/addChannel") && strings.Contains(h.ari.lastQuery(c), "channel=in-1") {
			added = true
		}
	}
	if !added {
		t.Fatal("caller was not added to the call's bridge")
	}
	if e, _ := h.host.Endpoint("201"); e.Call != "agent" {
		t.Fatalf("201 should show it is in the agent call, got %q", e.Call)
	}
}

func TestService_UnknownNumberIsRefused(t *testing.T) {
	h := start(t, withAgentLine)
	h.dialService("in-x", "4999", "201")
	h.eventually("hang up", func() bool { return len(h.ari.find("DELETE /channels/in-x")) == 1 })
	if len(h.ari.find("POST /channels/in-x/answer")) != 0 || len(h.host.Calls()) != 0 {
		t.Fatal("an unconfigured number was answered")
	}
}

func TestService_SecondCallerGetsBusy(t *testing.T) {
	h := start(t, withAgentLine)
	h.dialService("in-1", "4000", "201")
	h.waitJoined("agent", "201")
	h.dialService("in-2", "4000", "202")
	h.eventually("busy", func() bool { return len(h.ari.find("DELETE /channels/in-2")) == 1 })
	if q := h.ari.lastQuery("DELETE /channels/in-2"); q != "reason=busy" {
		t.Fatalf("second caller hangup query = %q", q)
	}
	if len(h.ari.find("POST /channels/in-2/answer")) != 0 {
		t.Fatal("second caller was answered")
	}
}

func TestService_CallerHangingUpEndsTheCall(t *testing.T) {
	h := start(t, withAgentLine)
	h.dialService("in-1", "4000", "201")
	h.waitJoined("agent", "201")
	h.ari.channelEvent(t, "ChannelDestroyed", "in-1")
	h.eventually("ended", func() bool { return h.callStatus("agent") == CallEnded })
	if h.participant("agent", "201").Status != StatusLeft {
		t.Fatal("caller should have left")
	}
	// The line is free again.
	h.dialService("in-3", "4000", "201")
	h.waitJoined("agent", "201")
}

func TestService_RejectedWithoutServices(t *testing.T) {
	h := start(t, nil)
	h.dialService("in-1", "4000", "201")
	h.eventually("hang up", func() bool { return len(h.ari.find("DELETE /channels/in-1")) == 1 })
}

// agentCallListening answers the agent line and starts transcription.
func (h *harness) agentCallListening() {
	h.t.Helper()
	h.dialService("in-1", "4000", "201")
	h.waitJoined("agent", "201")
	if _, err := h.host.StartTranscription("agent"); err != nil {
		h.t.Fatal(err)
	}
	h.ari.channelEvent(h.t, "StasisStart", h.ari.speechChannel())
	h.eventually("listening", func() bool { return h.transcript("agent").Status == TranscriptListening })
}

func (h *harness) speechState(call string) CallSpeech {
	h.t.Helper()
	c, err := h.host.GetCall(call)
	if err != nil || c.Speech == nil {
		h.t.Fatalf("no speech on %s: %+v %v", call, c, err)
	}
	return *c.Speech
}

func TestSpeech_NeedsTranscription(t *testing.T) {
	h := start(t, withAgentLine)
	h.dialService("in-1", "4000", "201")
	h.waitJoined("agent", "201")
	if _, err := h.host.Say("agent", "hello"); !errors.Is(err, ErrConflict) {
		t.Fatalf("say without transcription: %v", err)
	}
	if code, _, _ := h.request(http.MethodPost, "/v1/calls/agent/speech", `{"text":"hello"}`); code != http.StatusConflict {
		t.Fatalf("API status = %d", code)
	}
}

func TestSpeech_TextGoesToSpeechAndProgressIsShown(t *testing.T) {
	h := start(t, withAgentLine)
	h.agentCallListening()
	code, body, _ := h.request(http.MethodPost, "/v1/calls/agent/speech", `{"text":"eight lights are on"}`)
	if code != http.StatusAccepted || !strings.Contains(body, `"status":"speaking"`) {
		t.Fatalf("say: %d %s", code, body)
	}
	if len(h.speech.said) != 1 || h.speech.said[0] != "speech-1: eight lights are on" {
		t.Fatalf("speech said %v", h.speech.said)
	}
	h.speech.publish(speech.Event{Type: speech.EventSpeaking, Seq: 1, Text: "eight lights are on"})
	h.speech.publish(speech.Event{Type: speech.EventSpoken, Seq: 1, Text: "eight lights are on"})
	h.eventually("idle", func() bool { sp := h.speechState("agent"); return sp.Status == SpeechIdle && sp.Seq == 1 })
}

func TestSpeech_InterruptionIsNotAnErrorAndStaleEventsAreIgnored(t *testing.T) {
	h := start(t, withAgentLine)
	h.agentCallListening()
	h.host.Say("agent", "first")
	h.host.Say("agent", "second")
	h.speech.publish(speech.Event{Type: speech.EventSpeaking, Seq: 2, Text: "second"})
	h.speech.publish(speech.Event{Type: speech.EventSpoken, Seq: 1, Text: "first", Error: "interrupted"})
	h.eventually("still speaking the second", func() bool {
		sp := h.speechState("agent")
		return sp.Status == SpeechSpeaking && sp.Seq == 2 && sp.Text == "second"
	})
	h.speech.publish(speech.Event{Type: speech.EventSpoken, Seq: 2, Text: "second", Error: "interrupted"})
	h.eventually("idle", func() bool { sp := h.speechState("agent"); return sp.Status == SpeechIdle && sp.Error == "" })
}

func TestSpeech_FailuresAreVisible(t *testing.T) {
	h := start(t, withAgentLine)
	h.agentCallListening()
	h.host.Say("agent", "hello")
	h.speech.publish(speech.Event{Type: speech.EventSpoken, Seq: 1, Text: "hello", Error: "no audio from the call to reply to"})
	h.eventually("error", func() bool { return h.speechState("agent").Status == SpeechError })

	h.speech.mu.Lock()
	h.speech.sayErr = errors.New("speech output is not configured")
	h.speech.mu.Unlock()
	if _, err := h.host.Say("agent", "again"); err == nil {
		t.Fatal("say should fail")
	}
	if sp := h.speechState("agent"); sp.Status != SpeechError || !strings.Contains(sp.Error, "not configured") {
		t.Fatalf("speech = %+v", sp)
	}
}

func TestSpeech_TextIsValidated(t *testing.T) {
	h := start(t, withAgentLine)
	h.agentCallListening()
	for _, text := range []string{"", "   ", strings.Repeat("a", speech.MaxSayLength+1)} {
		if _, err := h.host.Say("agent", text); !errors.Is(err, ErrInvalid) {
			t.Errorf("say %d chars: %v", len(text), err)
		}
	}
}

func TestSpeech_StopsBeingSpeakingWhenTranscriptionEnds(t *testing.T) {
	h := start(t, withAgentLine)
	h.agentCallListening()
	h.host.Say("agent", "hello")
	h.host.StopTranscription("agent")
	if sp := h.speechState("agent"); sp.Status != SpeechIdle {
		t.Fatalf("speech = %+v", sp)
	}
}

func TestConfig_Services(t *testing.T) {
	t.Setenv("ASTERISK_HOST_SERVICES", `{"4000":{"name":"Agent","call":"agent"}}`)
	c, err := LoadConfig()
	if err != nil || c.Services["4000"].Call != "agent" {
		t.Fatalf("config = %+v err=%v", c.Services, err)
	}
	for _, bad := range []string{`{"40a0":{"call":"agent"}}`, `{"4000":{"call":"Bad Call"}}`, `{"4000":{}}`, `not json`} {
		t.Setenv("ASTERISK_HOST_SERVICES", bad)
		if _, err := LoadConfig(); err == nil {
			t.Errorf("%s should be rejected", bad)
		}
	}
}
