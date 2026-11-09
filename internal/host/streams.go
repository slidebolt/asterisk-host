package host

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/slidebolt/asterisk-host/internal/ari"
	media "github.com/slidebolt/sb-media-sdk"
)

// checkListenPolicy refuses a stream of anyone who has not allowed it.
func (h *Host) checkListenPolicy(c *activeCall) error {
	for id, p := range c.parts {
		if p.state.Status != StatusRinging && p.state.Status != StatusJoined {
			continue
		}
		e, ok := h.endpoints[id]
		if !ok || e.Features.Listen == PolicyNever {
			return fmt.Errorf("%w: %s does not allow listening", ErrForbidden, id)
		}
	}
	return nil
}

// StartCallStream attaches a stream to a call. It does not affect the call.
func (h *Host) StartCallStream(callID string) (Stream, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return Stream{}, ErrNotFound
	}
	if err := h.checkListenPolicy(c); err != nil {
		return Stream{}, err
	}
	key := callStreamKey(callID)
	u, err := h.media.Register(media.RegisterRequest{Stream: key, Kind: media.KindAudio, Format: media.FormatMPEGTS})
	if err != nil {
		return Stream{}, err
	}
	if err := h.attachStream(c, &activeStream{key: key, state: &Stream{Status: StreamStarting, URL: u}}); err != nil {
		return Stream{}, err
	}
	return *c.stream.state, nil
}

// StopCallStream detaches a stream from a call. The call carries on.
func (h *Host) StopCallStream(callID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return ErrNotFound
	}
	if c.stream != nil {
		ctx, cancel := ariContext()
		defer cancel()
		h.releaseStream(ctx, c, StreamIdle, "")
		h.emitCall(c)
	}
	return nil
}

// StartListen streams an endpoint. If it is already in a call the stream joins
// that call. If not, its policy decides whether a private call may be placed.
func (h *Host) StartListen(id string) (Stream, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.endpoints[id]
	if !ok {
		return Stream{}, ErrNotFound
	}
	if e.Features.Listen == PolicyNever || e.Stream == nil {
		return Stream{}, fmt.Errorf("%w: %s does not allow listening", ErrForbidden, id)
	}
	if e.Stream.URL == "" {
		h.registerEndpointStream(e)
		if e.Stream.URL == "" {
			h.emitEndpoint(e)
			return *e.Stream, fmt.Errorf("media service unavailable: %s", e.Stream.Error)
		}
	}

	stream := &activeStream{key: endpointStreamKey(id), state: e.Stream, endpoint: id}
	if e.Call != "" {
		c, ok := h.calls[e.Call]
		if !ok || c.state.Status != CallActive {
			return Stream{}, fmt.Errorf("%w: %s has no active call", ErrConflict, id)
		}
		if c.stream != nil {
			if c.stream.endpoint == id {
				return *e.Stream, nil
			}
			return Stream{}, fmt.Errorf("%w: call already has a stream", ErrConflict)
		}
		if err := h.checkListenPolicy(c); err != nil {
			return Stream{}, err
		}
		if err := h.attachStream(c, stream); err != nil {
			return Stream{}, err
		}
		return *e.Stream, nil
	}
	if e.Features.Listen != PolicyOnCommand {
		return Stream{}, fmt.Errorf("%w: %s can only be heard during a call", ErrForbidden, id)
	}

	c, err := h.ensureCall("listen-"+strings.ToLower(sanitizeKey(id)), h.cfg.MaxListen)
	if err != nil {
		e.Stream.Status = StreamError
		e.Stream.Error = err.Error()
		h.emitEndpoint(e)
		return *e.Stream, err
	}
	stream.stopIdle = true
	if err := h.attachStream(c, stream); err != nil {
		h.endCall(c, err.Error())
		return *e.Stream, err
	}
	if _, err := h.addParticipant(c, AddParticipantRequest{Target: id}); err != nil {
		return *e.Stream, err
	}
	return *e.Stream, nil
}

// StopListen detaches an endpoint's stream, and ends the call only if the call
// existed just for listening. A real conversation carries on.
func (h *Host) StopListen(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.endpoints[id]; !ok {
		return ErrNotFound
	}
	for _, c := range h.activeCalls() {
		if c.stream == nil || c.stream.endpoint != id {
			continue
		}
		if c.stream.stopIdle {
			h.endCall(c, "ended")
			return nil
		}
		ctx, cancel := ariContext()
		defer cancel()
		h.releaseStream(ctx, c, StreamIdle, "")
		h.emitCall(c)
		return nil
	}
	return nil
}

// attachStream opens a media session and adds it to the call's bridge.
func (h *Host) attachStream(c *activeCall, s *activeStream) error {
	if c.stream != nil {
		return fmt.Errorf("%w: call already has a stream", ErrConflict)
	}
	s.state.Status = StreamStarting
	s.state.Error = ""
	s.state.Viewers = 0
	reply, err := h.media.Start(media.StartRequest{
		Stream: s.key,
		Input:  media.Input{Type: media.InputRTP, Codec: media.CodecPCMU, SampleRate: 8000},
	})
	if err != nil {
		s.state.Status = StreamError
		s.state.Error = fmt.Sprintf("start media: %v", err)
		h.publishStream(c, s)
		return errors.New(s.state.Error)
	}
	s.mediaID = reply.SessionID
	s.sub, err = h.media.OnStatus(s.key, func(ev media.StatusEvent) { h.onMediaStatus(c, s, ev) })
	if err != nil {
		log.Printf("asterisk-host: watch stream: %v", err)
	}
	s.channel = resourcePrefix + "media-" + randomID()
	h.channels[s.channel] = c
	c.stream = s
	c.state.Stream = s.state
	ctx, cancel := ariContext()
	defer cancel()
	if err := h.ari.ExternalMedia(ctx, s.channel, stasisApp, reply.RTPHost, reply.RTPPort, ari.FormatULaw); err != nil {
		h.releaseStream(ctx, c, StreamError, err.Error())
		h.emitCall(c)
		return err
	}
	h.publishStream(c, s)
	return nil
}

func (h *Host) publishStream(c *activeCall, s *activeStream) {
	if s.endpoint != "" {
		if e, ok := h.endpoints[s.endpoint]; ok {
			h.emitEndpoint(e)
		}
	}
	h.emitCall(c)
}

func (h *Host) onMediaStatus(c *activeCall, s *activeStream, ev media.StatusEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.stream != s || ev.SessionID != s.mediaID {
		return
	}
	switch ev.Status {
	case media.StatusLive:
		s.state.Status = StreamLive
		s.state.Viewers = ev.Viewers
		h.publishStream(c, s)
	case media.StatusError:
		s.mediaID = ""
		h.failStream(c, ev.Error)
	case media.StatusIdle:
		s.mediaID = ""
		h.failStream(c, "")
	}
}

// failStream handles a stream dying. A call that existed only for this stream
// ends with it, so no microphone stays open with nobody listening.
func (h *Host) failStream(c *activeCall, errMsg string) {
	s := c.stream
	if s == nil {
		return
	}
	ctx, cancel := ariContext()
	defer cancel()
	status := StreamError
	if errMsg == "" {
		status = StreamIdle
	}
	stopIdle := s.stopIdle
	h.releaseStream(ctx, c, status, errMsg)
	if stopIdle {
		h.endCall(c, "stream ended")
	}
	h.emitCall(c)
}

// releaseStream closes a stream and reports its final status. Callers hold mu.
func (h *Host) releaseStream(ctx context.Context, c *activeCall, status, errMsg string) {
	s := c.stream
	if s == nil {
		return
	}
	c.stream = nil
	if s.sub != nil {
		s.sub.Unsubscribe()
	}
	if s.channel != "" {
		delete(h.channels, s.channel)
		if err := h.ari.Hangup(ctx, s.channel); err != nil && h.server.Connected {
			log.Printf("asterisk-host: hang up %s: %v", s.channel, err)
		}
	}
	if s.mediaID != "" {
		if err := h.media.Stop(s.mediaID); err != nil {
			log.Printf("asterisk-host: stop media: %v", err)
		}
	}
	s.state.Status = status
	s.state.Error = errMsg
	s.state.Viewers = 0
	if s.endpoint != "" {
		if e, ok := h.endpoints[s.endpoint]; ok {
			h.emitEndpoint(e)
		}
	}
}

// OnMediaStarted handles sb-media restarting, which loses all its sessions.
func (h *Host) OnMediaStarted() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.activeCalls() {
		if c.stream != nil {
			c.stream.mediaID = ""
			h.failStream(c, "media service restarted")
		}
	}
	for _, e := range h.endpoints {
		if e.Stream == nil {
			continue
		}
		u, err := h.media.Register(media.RegisterRequest{
			Stream: endpointStreamKey(e.ID), Kind: media.KindAudio, Format: media.FormatMPEGTS,
		})
		if err != nil {
			log.Printf("asterisk-host: register stream for %s: %v", e.ID, err)
			continue
		}
		e.Stream.URL = u
		h.emitEndpoint(e)
	}
}
