package host

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/slidebolt/asterisk-host/internal/ari"
)

// EnsureCall creates a call with a caller-chosen ID, and is safe to retry.
func (h *Host) EnsureCall(id string) (Call, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, err := h.ensureCall(id, h.cfg.MaxCall)
	if err != nil {
		return Call{}, err
	}
	return cloneCall(c.state), nil
}

func (h *Host) ensureCall(id string, limit time.Duration) (*activeCall, error) {
	if !callIDPattern.MatchString(id) {
		return nil, fmt.Errorf("%w: call id must match %s", ErrInvalid, callIDPattern)
	}
	if existing, ok := h.calls[id]; ok && existing.state.Status == CallActive {
		return existing, nil
	}
	if !h.server.Connected {
		return nil, ErrOffline
	}
	if h.closed {
		return nil, ErrOffline
	}
	if old, ok := h.calls[id]; ok {
		old.stopTimers()
		delete(h.calls, id)
	}
	c := &activeCall{
		state:    &Call{ID: id, Status: CallActive, CreatedAt: now(), Participants: []Participant{}},
		bridge:   resourcePrefix + "call-" + randomID(),
		parts:    map[string]*activeParticipant{},
		limit:    limit,
		hangupOn: map[string]bool{},
	}
	ctx, cancel := ariContext()
	defer cancel()
	if err := h.ari.CreateBridge(ctx, c.bridge); err != nil {
		return nil, err
	}
	h.calls[id] = c
	c.timer = time.AfterFunc(limit, func() { h.onTimeLimit(c) })
	h.emitCall(c)
	h.emitServer()
	return c, nil
}

func (c *activeCall) stopTimers() {
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.retain != nil {
		c.retain.Stop()
	}
}

func (h *Host) onTimeLimit(c *activeCall) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.state.Status == CallActive {
		h.endCall(c, "time limit reached")
	}
}

// EndCall ends a call for everyone.
func (h *Host) EndCall(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[id]
	if !ok {
		return ErrNotFound
	}
	if c.state.Status == CallActive {
		h.endCall(c, "ended")
	}
	return nil
}

// endCall releases everything for a call. Callers hold mu.
func (h *Host) endCall(c *activeCall, reason string) {
	if c.state.Status != CallActive {
		return
	}
	c.state.Status = CallEnded
	c.state.Reason = reason
	c.state.EndedAt = now()
	if c.timer != nil {
		c.timer.Stop()
	}
	ctx, cancel := ariContext()
	defer cancel()
	if c.stream != nil {
		h.releaseStream(ctx, c, StreamIdle, streamEndError(reason))
	}
	if c.transcr != nil {
		h.releaseTranscription(ctx, c, TranscriptIdle, "")
	}
	for _, id := range c.order {
		p := c.parts[id]
		if p.channel != "" {
			delete(h.channels, p.channel)
			if err := h.ari.Hangup(ctx, p.channel); err != nil && h.server.Connected {
				log.Printf("asterisk-host: hang up %s: %v", p.channel, err)
			}
			p.channel = ""
		}
		switch p.state.Status {
		case StatusRinging:
			p.state.Status = StatusLeft
			p.state.Reason = reason
		case StatusJoined:
			p.state.Status = StatusLeft
		}
		h.setEndpointCall(id, "")
	}
	if err := h.ari.DestroyBridge(ctx, c.bridge); err != nil && h.server.Connected {
		log.Printf("asterisk-host: destroy bridge %s: %v", c.bridge, err)
	}
	h.emitCall(c)
	h.emitServer()
	c.retain = time.AfterFunc(h.cfg.RetainEnded, func() { h.removeCall(c) })
}

// streamEndError is what a listener sees for why a stream ended with its call.
func streamEndError(reason string) string {
	if reason == "ended" {
		return ""
	}
	return reason
}

// removeCall forgets an ended call. It stays visible for a while first, because
// script triggers fire on changes but cannot see a deletion.
func (h *Host) removeCall(c *activeCall) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.calls[c.state.ID] != c {
		return
	}
	delete(h.calls, c.state.ID)
	h.emit(EventRemoved, map[string]string{"id": c.state.ID})
}

// Calls lists calls.
func (h *Host) Calls() []Call {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.listCalls()
}

// GetCall returns one call.
func (h *Host) GetCall(id string) (Call, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.calls[id]
	if !ok {
		return Call{}, ErrNotFound
	}
	return cloneCall(c.state), nil
}

func (h *Host) listCalls() []Call {
	out := make([]Call, 0, len(h.calls))
	for _, c := range h.calls {
		out = append(out, cloneCall(c.state))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// AddParticipant dials a target into a call. The result is always reported on
// the participant as well, because a script only ever sees state.
func (h *Host) AddParticipant(callID string, req AddParticipantRequest) (Participant, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, err := h.ensureCall(callID, h.cfg.MaxCall)
	if err != nil {
		return Participant{}, err
	}
	return h.addParticipant(c, req)
}

func (h *Host) addParticipant(c *activeCall, req AddParticipantRequest) (Participant, error) {
	id, dial, err := h.resolveTarget(req.Target)
	if err != nil && id == "" {
		return Participant{}, err
	}
	if existing, ok := c.parts[id]; ok {
		if existing.state.Status == StatusRinging || existing.state.Status == StatusJoined {
			return *existing.state, nil
		}
	} else {
		c.order = append(c.order, id)
	}
	p := &activeParticipant{state: &Participant{ID: id, Target: req.Target, Status: StatusRinging}}
	c.parts[id] = p
	if err != nil {
		p.state.Status = StatusFailed
		p.state.Reason = err.Error()
		h.emitCall(c)
		h.endIfEmpty(c)
		return *p.state, err
	}

	ring := req.RingSeconds
	if ring <= 0 {
		ring = defaultRingSeconds
	}
	p.channel = resourcePrefix + "part-" + randomID()
	h.channels[p.channel] = c
	ctx, cancel := ariContext()
	defer cancel()
	limit := int(c.limit.Seconds()) + hardLimitMargin
	if err := h.ari.Originate(ctx, p.channel, dial, stasisApp, ring, limit); err != nil {
		delete(h.channels, p.channel)
		p.channel = ""
		p.state.Status = StatusFailed
		p.state.Reason = err.Error()
		h.emitCall(c)
		h.endIfEmpty(c)
		return *p.state, nil
	}
	h.setEndpointCall(id, c.state.ID)
	h.emitCall(c)
	return *p.state, nil
}

// resolveTarget applies the dial policy. On refusal it returns the participant
// ID as well as the error, so the refusal can be shown on that participant.
func (h *Host) resolveTarget(target string) (id, dial string, err error) {
	if number, ok := strings.CutPrefix(target, "number:"); ok {
		if !numberPattern.MatchString(number) {
			return "", "", fmt.Errorf("%w: bad number", ErrInvalid)
		}
		id = "n" + sanitizeKey(strings.TrimPrefix(number, "+"))
		if !h.cfg.DialAllow[number] {
			return id, "", fmt.Errorf("%w: %s is not on the dial allowlist", ErrForbidden, number)
		}
		trunk := h.firstTrunk()
		if trunk == "" {
			return id, "", fmt.Errorf("%w: no trunk is configured", ErrInvalid)
		}
		return id, "PJSIP/" + number + "@" + trunk, nil
	}
	e, ok := h.endpoints[target]
	if !ok {
		return "", "", fmt.Errorf("%w: unknown target %q", ErrInvalid, target)
	}
	if e.Kind == "trunk" {
		return target, "", fmt.Errorf("%w: %s is a trunk; dial a number through it", ErrForbidden, target)
	}
	if override := h.cfg.Endpoints[target].Dial; override != "" {
		return target, override, nil
	}
	if e.Status != "online" {
		return target, "", fmt.Errorf("%s is offline", target)
	}
	return target, "PJSIP/" + target, nil
}

func (h *Host) firstTrunk() string {
	var names []string
	for name := range h.cfg.Trunks {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func (c *activeCall) participantFor(channel string) (string, *activeParticipant) {
	for id, p := range c.parts {
		if p.channel == channel {
			return id, p
		}
	}
	return "", nil
}

func (c *activeCall) hasActiveParticipants() bool {
	for _, p := range c.parts {
		if p.state.Status == StatusRinging || p.state.Status == StatusJoined {
			return true
		}
	}
	return false
}

// endIfEmpty ends a call once nobody is left in it.
func (h *Host) endIfEmpty(c *activeCall) {
	if c.state.Status == CallActive && len(c.parts) > 0 && !c.hasActiveParticipants() {
		h.endCall(c, "everyone left")
	}
}

func (h *Host) onStasisStart(c *activeCall, channel string) {
	ctx, cancel := ariContext()
	defer cancel()
	if err := h.ari.AddChannel(ctx, c.bridge, channel); err != nil {
		log.Printf("asterisk-host: add %s to bridge: %v", channel, err)
		if c.stream != nil && c.stream.channel == channel {
			h.failStream(c, err.Error())
			return
		}
		h.endCall(c, "could not join the call")
		return
	}
	if _, p := c.participantFor(channel); p != nil {
		p.joined = true
		p.state.Status = StatusJoined
		h.emitCall(c)
	}
}

// causes maps Q.850 hangup causes to participant statuses.
func hangupStatus(cause int) string {
	switch cause {
	case 17:
		return StatusBusy
	default:
		return StatusNoAnswer
	}
}

func (h *Host) onChannelGone(c *activeCall, ev ari.Event) {
	channel := ev.Channel.ID
	if c.stream != nil && c.stream.channel == channel {
		delete(h.channels, channel)
		c.stream.channel = ""
		h.failStream(c, "media channel ended")
		return
	}
	id, p := c.participantFor(channel)
	if p == nil {
		return
	}
	delete(h.channels, channel)
	p.channel = ""
	if p.joined {
		p.state.Status = StatusLeft
	} else {
		p.state.Status = hangupStatus(ev.Cause)
		p.state.Reason = ev.CauseTxt
	}
	p.state.Muted, p.state.Held = false, false
	h.setEndpointCall(id, "")
	h.emitCall(c)
	h.endIfEmpty(c)
}

// RemoveParticipant takes one party out of a call.
func (h *Host) RemoveParticipant(callID, id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, p, err := h.participant(callID, id)
	if err != nil {
		return err
	}
	if p.channel != "" {
		ctx, cancel := ariContext()
		defer cancel()
		channel := p.channel
		delete(h.channels, channel)
		p.channel = ""
		if err := h.ari.Hangup(ctx, channel); err != nil {
			log.Printf("asterisk-host: hang up %s: %v", channel, err)
		}
	}
	p.state.Status = StatusLeft
	p.state.Muted, p.state.Held = false, false
	h.setEndpointCall(id, "")
	h.emitCall(c)
	h.endIfEmpty(c)
	return nil
}

func (h *Host) participant(callID, id string) (*activeCall, *activeParticipant, error) {
	c, ok := h.calls[callID]
	if !ok || c.state.Status != CallActive {
		return nil, nil, ErrNotFound
	}
	p, ok := c.parts[id]
	if !ok {
		return nil, nil, ErrNotFound
	}
	return c, p, nil
}

// HangupEndpoint removes an endpoint from whatever call it is in.
func (h *Host) HangupEndpoint(id string) error {
	h.mu.Lock()
	e, ok := h.endpoints[id]
	if !ok {
		h.mu.Unlock()
		return ErrNotFound
	}
	call := e.Call
	h.mu.Unlock()
	if call == "" {
		return nil
	}
	return h.RemoveParticipant(call, id)
}

// ParticipantCommand acts on one party, and records the outcome.
func (h *Host) ParticipantCommand(callID, id string, req CommandRequest) (Command, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, p, err := h.participant(callID, id)
	if err != nil {
		return Command{}, err
	}
	if p.channel == "" || !p.joined {
		return Command{}, fmt.Errorf("%w: %s is not in the call", ErrConflict, id)
	}
	ctx, cancel := ariContext()
	defer cancel()
	var runErr error
	switch req.Action {
	case "mute", "unmute":
		on := req.Action == "mute"
		if runErr = h.ari.Mute(ctx, p.channel, on); runErr == nil {
			p.state.Muted = on
		}
	case "hold", "unhold":
		on := req.Action == "hold"
		if runErr = h.ari.Hold(ctx, p.channel, on); runErr == nil {
			p.state.Held = on
		}
	case "dtmf":
		digits, _ := req.Parameters["digits"].(string)
		if !dtmfPattern.MatchString(digits) {
			return Command{}, fmt.Errorf("%w: bad digits", ErrInvalid)
		}
		runErr = h.ari.DTMF(ctx, p.channel, digits)
	default:
		return Command{}, fmt.Errorf("%w: unknown action %q", ErrInvalid, req.Action)
	}
	cmd := Command{ID: randomID(), Status: "succeeded", Action: req.Action}
	if runErr != nil {
		cmd.Status = "failed"
		cmd.Error = runErr.Error()
	} else {
		h.emitCall(c)
	}
	h.recordCommand(&cmd)
	return cmd, nil
}

func (h *Host) recordCommand(cmd *Command) {
	h.commands[cmd.ID] = cmd
	h.cmdOrder = append(h.cmdOrder, cmd.ID)
	if len(h.cmdOrder) > maxCommands {
		delete(h.commands, h.cmdOrder[0])
		h.cmdOrder = h.cmdOrder[1:]
	}
}

// GetCommand returns a recorded command.
func (h *Host) GetCommand(id string) (Command, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cmd, ok := h.commands[id]
	if !ok {
		return Command{}, ErrNotFound
	}
	return *cmd, nil
}
