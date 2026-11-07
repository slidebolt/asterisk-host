package host

import (
	"context"
	"log"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/slidebolt/asterisk-host/internal/ari"
	media "github.com/slidebolt/sb-media-sdk"
)

// refreshEndpoints reconciles endpoints with Asterisk. Callers hold mu.
func (h *Host) refreshEndpoints() {
	ctx, cancel := ariContext()
	defer cancel()
	found, err := h.ari.Endpoints(ctx, "PJSIP")
	if err != nil {
		log.Printf("asterisk-host: list endpoints: %v", err)
		return
	}
	seen := map[string]bool{}
	for _, ep := range found {
		if _, isService := h.cfg.Services[ep.Resource]; isService {
			log.Printf("asterisk-host: %s is both a PJSIP endpoint and a service; treating it as a service", ep.Resource)
			continue
		}
		seen[ep.Resource] = true
		h.applyEndpoint(ctx, ep)
	}
	for id, svc := range h.cfg.Services {
		seen[id] = true
		h.applyService(id, svc)
	}
	for id := range h.endpoints {
		if !seen[id] {
			delete(h.endpoints, id)
		}
	}
}

// applyEndpoint updates one endpoint from ARI, and publishes any change.
func (h *Host) applyEndpoint(ctx context.Context, ep ari.Endpoint) {
	if ep.Resource == "" {
		return
	}
	status := "offline"
	if ep.State == "online" {
		status = "online"
	}
	e, known := h.endpoints[ep.Resource]
	if !known {
		e = h.describeEndpoint(ctx, ep.Resource)
		h.endpoints[ep.Resource] = e
	}
	if known && e.Status == status {
		return
	}
	e.Status = status
	h.emitEndpoint(e)
}

// applyService lists a service number as an endpoint. It is online whenever
// Asterisk is connected, since SlideBolt itself answers it.
func (h *Host) applyService(id string, svc ServiceConfig) {
	e, ok := h.endpoints[id]
	if !ok {
		name := svc.Name
		if name == "" {
			name = id
		}
		e = &Endpoint{ID: id, Name: name, Kind: "service", Role: "service", Features: Features{Listen: PolicyNever}}
		h.endpoints[id] = e
	} else if e.Status == "online" {
		return
	}
	e.Status = "online"
	h.emitEndpoint(e)
}

// describeEndpoint builds a new endpoint: Asterisk supplies the identity and
// name, and config supplies what Asterisk cannot know.
func (h *Host) describeEndpoint(ctx context.Context, id string) *Endpoint {
	cfg := h.cfg.Endpoints[id]
	e := &Endpoint{
		ID: id, Name: cfg.Name, Kind: "extension", Role: cfg.Role,
		Features: Features{AutoAnswer: cfg.AutoAnswer, Listen: cfg.Listen},
	}
	if e.Features.Listen == "" {
		e.Features.Listen = PolicyNever
	}
	if h.cfg.Trunks[id] {
		e.Kind = "trunk"
		e.Features.Listen = PolicyNever
	}
	if e.Name == "" {
		callerID, err := h.ari.EndpointCallerID(ctx, id)
		if err != nil {
			log.Printf("asterisk-host: caller id for %s: %v", id, err)
		}
		e.Name = parseCallerName(callerID)
	}
	if e.Name == "" {
		e.Name = id
	}
	if e.Features.Listen != PolicyNever {
		e.Stream = &Stream{Status: StreamIdle}
		h.registerEndpointStream(e)
	}
	return e
}

func parseCallerName(callerID string) string {
	m := callerIDPattern.FindStringSubmatch(callerID)
	if m == nil {
		return ""
	}
	name := strings.TrimSpace(m[1])
	if name == "<unknown>" || name == "unknown" {
		return ""
	}
	return name
}

func endpointStreamKey(id string) string {
	return "asterisk.endpoint." + sanitizeKey(id)
}

func callStreamKey(id string) string {
	return "asterisk.call." + sanitizeKey(id)
}

var keyUnsafe = regexp.MustCompile(`[^A-Za-z0-9_-]`)

func sanitizeKey(s string) string {
	return keyUnsafe.ReplaceAllString(s, "_")
}

// registerEndpointStream fetches the stable URL for an endpoint. Callers hold mu.
func (h *Host) registerEndpointStream(e *Endpoint) {
	u, err := h.media.Register(media.RegisterRequest{
		Stream: endpointStreamKey(e.ID), Kind: media.KindAudio, Format: media.FormatMPEGTS,
	})
	if err != nil {
		log.Printf("asterisk-host: register stream for %s: %v", e.ID, err)
		e.Stream.URL = ""
		e.Stream.Status = StreamError
		e.Stream.Error = err.Error()
		return
	}
	e.Stream.URL = u
	if e.Stream.Status == StreamError {
		e.Stream.Status = StreamIdle
		e.Stream.Error = ""
	}
}

func (h *Host) listEndpoints() []Endpoint {
	out := make([]Endpoint, 0, len(h.endpoints))
	for _, e := range h.endpoints {
		out = append(out, cloneEndpoint(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Endpoints lists endpoints of one kind, or all when kind is empty.
func (h *Host) Endpoints(kind string) []Endpoint {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := h.listEndpoints()
	if kind == "" {
		return out
	}
	return slices.DeleteFunc(out, func(e Endpoint) bool { return e.Kind != kind })
}

// Endpoint returns one endpoint.
func (h *Host) Endpoint(id string) (Endpoint, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.endpoints[id]
	if !ok {
		return Endpoint{}, ErrNotFound
	}
	return cloneEndpoint(e), nil
}
