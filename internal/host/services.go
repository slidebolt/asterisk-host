package host

import (
	"log"
	"strconv"

	"github.com/slidebolt/asterisk-host/internal/ari"
)

// onServiceCall answers a call to a service number and puts the caller in that
// service's call. Callers hold mu.
func (h *Host) onServiceCall(ev ari.Event) {
	channel, number := ev.Channel.ID, ev.Args[1]
	ctx, cancel := ariContext()
	defer cancel()
	svc, ok := h.cfg.Services[number]
	if !ok {
		log.Printf("asterisk-host: refusing call to %s: not a configured service", number)
		if err := h.ari.Hangup(ctx, channel); err != nil {
			log.Printf("asterisk-host: hang up %s: %v", channel, err)
		}
		return
	}
	if existing, ok := h.calls[svc.Call]; ok && existing.state.Status == CallActive && existing.hasActiveParticipants() {
		log.Printf("asterisk-host: %s is busy", number)
		if err := h.ari.HangupBusy(ctx, channel); err != nil {
			log.Printf("asterisk-host: hang up %s: %v", channel, err)
		}
		return
	}
	c, err := h.ensureCall(svc.Call, h.cfg.MaxCall)
	if err != nil {
		log.Printf("asterisk-host: call for %s: %v", number, err)
		h.ari.Hangup(ctx, channel)
		return
	}
	if err := h.ari.Answer(ctx, channel); err != nil {
		log.Printf("asterisk-host: answer %s: %v", channel, err)
		h.ari.Hangup(ctx, channel)
		h.endIfEmptyOrUnused(c)
		return
	}
	// Asterisk ends the channel itself if this host dies.
	limit := strconv.Itoa(int(c.limit.Seconds()) + hardLimitMargin)
	if err := h.ari.SetVariable(ctx, channel, "TIMEOUT(absolute)", limit); err != nil {
		log.Printf("asterisk-host: limit %s: %v", channel, err)
	}

	id := sanitizeKey(ev.Channel.Caller.Number)
	if id == "" {
		id = "caller"
	}
	if _, exists := c.parts[id]; !exists {
		c.order = append(c.order, id)
	}
	c.parts[id] = &activeParticipant{
		state:   &Participant{ID: id, Target: id, Status: StatusRinging},
		channel: channel,
	}
	h.channels[channel] = c
	h.setEndpointCall(id, c.state.ID)
	h.onStasisStart(c, channel)
}

// endIfEmptyOrUnused ends a call that never got a participant.
func (h *Host) endIfEmptyOrUnused(c *activeCall) {
	if len(c.parts) == 0 {
		h.endCall(c, "could not answer")
		return
	}
	h.endIfEmpty(c)
}
