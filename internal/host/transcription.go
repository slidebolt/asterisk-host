package host

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/slidebolt/asterisk-host/internal/ari"
	messenger "github.com/slidebolt/sb-messenger-sdk"
	speech "github.com/slidebolt/sb-speech-sdk"
)

// transcriptionInput is the audio sb-speech gets from a call: wideband G.722,
// so speech recognition and spoken replies are not limited to telephone
// quality. A G.722 caller passes straight through; Asterisk converts any other
// codec, so a narrowband caller sounds as it did before.
var transcriptionInput = speech.Input{Type: speech.InputRTP, Codec: speech.CodecG722, SampleRate: 16000}

// activeTranscription is a speech-to-text session attached to a call's bridge. It
// uses the same external media path as a stream, and it never affects the call.
type activeTranscription struct {
	sessionID string
	channel   string
	sub       messenger.Subscription
}

// StartTranscription sends a call's audio to sb-speech. It is safe to retry.
func (h *Host) StartTranscription(callID string) (Transcript, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return Transcript{}, ErrNotFound
	}
	if c.transcr != nil {
		return *c.state.Transcript, nil
	}
	if err := h.checkListenPolicy(c); err != nil {
		return Transcript{}, err
	}
	if c.state.Transcript == nil {
		c.state.Transcript = &Transcript{}
	}
	t := c.state.Transcript
	t.Status = TranscriptStarting
	t.Error = ""

	reply, err := h.speech.Start(speech.StartRequest{Input: transcriptionInput})
	if err != nil {
		t.Status = TranscriptError
		t.Error = err.Error()
		h.emitCall(c)
		return *t, fmt.Errorf("start speech: %w", err)
	}
	at := &activeTranscription{sessionID: reply.SessionID, channel: resourcePrefix + "speech-" + randomID()}
	at.sub, err = h.speech.OnEvent(reply.SessionID, func(ev speech.Event) { h.onSpeechEvent(c, at, ev) })
	if err != nil {
		log.Printf("asterisk-host: watch speech: %v", err)
	}
	c.transcr = at
	h.channels[at.channel] = c

	ctx, cancel := ariContext()
	defer cancel()
	if err := h.ari.ExternalMedia(ctx, at.channel, stasisApp, reply.RTPHost, reply.RTPPort, ari.FormatG722); err != nil {
		h.releaseTranscription(ctx, c, TranscriptError, err.Error())
		h.emitCall(c)
		return *t, err
	}
	h.emitCall(c)
	return *t, nil
}

// StopTranscription stops speech-to-text. The call carries on.
func (h *Host) StopTranscription(callID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return ErrNotFound
	}
	if c.transcr != nil {
		ctx, cancel := ariContext()
		defer cancel()
		h.releaseTranscription(ctx, c, TranscriptIdle, "")
		h.emitCall(c)
	}
	return nil
}

func (h *Host) onSpeechEvent(c *activeCall, at *activeTranscription, ev speech.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.transcr != at || ev.SessionID != at.sessionID {
		return
	}
	t := c.state.Transcript
	switch ev.Type {
	case speech.EventTranscript:
		t.Status = TranscriptListening
		t.Seq++
		t.Text = ev.Text
		t.Error = ""
	case speech.EventError:
		t.Error = ev.Error
	case speech.EventSpeaking, speech.EventSpoken:
		if !h.applySpeech(c, ev) {
			return
		}
	default:
		return
	}
	h.emitCall(c)
}

// onTranscriptionChannel handles events on the media channel of a transcription.
func (h *Host) onTranscriptionChannel(c *activeCall, ev ari.Event) {
	switch ev.Type {
	case "StasisStart":
		ctx, cancel := ariContext()
		defer cancel()
		if err := h.ari.AddChannel(ctx, c.bridge, ev.Channel.ID); err != nil {
			log.Printf("asterisk-host: add %s to bridge: %v", ev.Channel.ID, err)
			h.releaseTranscription(ctx, c, TranscriptError, err.Error())
			h.emitCall(c)
			return
		}
		// Audio is flowing to sb-speech from here. This cannot rely on an event from
		// sb-speech, which starts the session before anyone can have subscribed to it.
		c.state.Transcript.Status = TranscriptListening
		h.emitCall(c)
	case "StasisEnd", "ChannelDestroyed":
		delete(h.channels, ev.Channel.ID)
		c.transcr.channel = ""
		ctx, cancel := ariContext()
		defer cancel()
		h.releaseTranscription(ctx, c, TranscriptError, "media channel ended")
		h.emitCall(c)
	}
}

// OnSpeechStarted handles sb-speech restarting, which loses all its sessions.
func (h *Host) OnSpeechStarted() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.activeCalls() {
		if c.transcr == nil {
			continue
		}
		c.transcr.sessionID = ""
		ctx, cancel := ariContext()
		h.releaseTranscription(ctx, c, TranscriptError, "speech service restarted")
		cancel()
		h.emitCall(c)
	}
}

// releaseTranscription closes a session and reports its final status. Callers hold mu.
func (h *Host) releaseTranscription(ctx context.Context, c *activeCall, status, errMsg string) {
	at := c.transcr
	if at == nil {
		return
	}
	c.transcr = nil
	if at.sub != nil {
		at.sub.Unsubscribe()
	}
	if at.channel != "" {
		delete(h.channels, at.channel)
		if err := h.ari.Hangup(ctx, at.channel); err != nil && h.server.Connected {
			log.Printf("asterisk-host: hang up %s: %v", at.channel, err)
		}
	}
	if at.sessionID != "" {
		if err := h.speech.Stop(at.sessionID); err != nil {
			log.Printf("asterisk-host: stop speech: %v", err)
		}
	}
	c.state.Transcript.Status = status
	c.state.Transcript.Error = errMsg
	if sp := c.state.Speech; sp != nil && sp.Status == SpeechSpeaking {
		sp.Status = SpeechIdle
	}
}

// Say speaks text into a call through its transcription session, whose media
// channel is already in the call's bridge. Saying something new interrupts
// anything still being spoken.
func (h *Host) Say(callID, text string) (CallSpeech, error) {
	text = strings.TrimSpace(text)
	if text == "" || len(text) > speech.MaxSayLength {
		return CallSpeech{}, fmt.Errorf("%w: text must be 1 to %d characters", ErrInvalid, speech.MaxSayLength)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return CallSpeech{}, ErrNotFound
	}
	if c.transcr == nil || c.transcr.sessionID == "" {
		return CallSpeech{}, fmt.Errorf("%w: start transcription first; speech uses its audio path", ErrConflict)
	}
	if c.state.Speech == nil {
		c.state.Speech = &CallSpeech{Status: SpeechIdle}
	}
	sp := c.state.Speech
	seq, err := h.speech.Say(c.transcr.sessionID, text)
	if err != nil {
		sp.Status, sp.Error = SpeechError, err.Error()
		h.emitCall(c)
		return *sp, fmt.Errorf("say: %w", err)
	}
	sp.Status, sp.Seq, sp.Text, sp.Error = SpeechSpeaking, seq, text, ""
	h.emitCall(c)
	return *sp, nil
}

// applySpeech records speaking and spoken events, ignoring ones about an
// utterance that has since been replaced. Callers hold mu.
func (h *Host) applySpeech(c *activeCall, ev speech.Event) bool {
	if c.state.Speech == nil {
		c.state.Speech = &CallSpeech{Status: SpeechIdle}
	}
	sp := c.state.Speech
	if ev.Seq < sp.Seq {
		return false
	}
	sp.Seq, sp.Text = ev.Seq, ev.Text
	switch {
	case ev.Type == speech.EventSpeaking:
		sp.Status, sp.Error = SpeechSpeaking, ""
	case ev.Error != "" && ev.Error != "interrupted":
		sp.Status, sp.Error = SpeechError, ev.Error
	default:
		sp.Status, sp.Error = SpeechIdle, ""
	}
	return true
}
