package host

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

const openAPI = `{
  "openapi": "3.0.3",
  "info": {"title": "asterisk-host", "version": "1"},
  "paths": {
    "/v1/health": {"get": {"operationId": "getHealth"}},
    "/v1/snapshot": {"get": {"operationId": "getSnapshot"}},
    "/v1/events": {"get": {"operationId": "streamEvents", "parameters": [{"name": "streamId", "in": "query"}, {"name": "after", "in": "query"}]}},
    "/v1/server/reload": {"post": {"operationId": "reloadServer"}},
    "/v1/endpoints": {"get": {"operationId": "listEndpoints"}},
    "/v1/endpoints/{id}": {"get": {"operationId": "getEndpoint"}},
    "/v1/endpoints/{id}/listen": {"post": {"operationId": "startListen"}, "delete": {"operationId": "stopListen"}},
    "/v1/endpoints/{id}/hangup": {"post": {"operationId": "hangupEndpoint"}},
    "/v1/trunks": {"get": {"operationId": "listTrunks"}},
    "/v1/calls": {"get": {"operationId": "listCalls"}},
    "/v1/calls/{id}": {"put": {"operationId": "ensureCall"}, "get": {"operationId": "getCall"}, "delete": {"operationId": "endCall"}},
    "/v1/calls/{id}/participants": {"post": {"operationId": "addParticipant"}},
    "/v1/calls/{id}/participants/{pid}": {"get": {"operationId": "getParticipant"}, "delete": {"operationId": "removeParticipant"}},
    "/v1/calls/{id}/participants/{pid}/commands": {"post": {"operationId": "participantCommand", "description": "mute, unmute, hold, unhold, dtmf. transfer returns 501."}},
    "/v1/calls/{id}/playbacks": {"post": {"operationId": "play"}},
    "/v1/calls/{id}/recordings": {"post": {"operationId": "record", "description": "Not implemented yet, returns 501."}},
    "/v1/calls/{id}/transcription": {"post": {"operationId": "startTranscription"}, "delete": {"operationId": "stopTranscription"}},
    "/v1/calls/{id}/speech": {"post": {"operationId": "say", "description": "Speaks text into the call. Needs an active transcription, whose audio path it uses."}},
    "/v1/calls/{id}/streams": {"post": {"operationId": "startCallStream"}, "delete": {"operationId": "stopCallStream"}},
    "/v1/commands/{id}": {"get": {"operationId": "getCommand"}}
  }
}`

// Handler returns the HTTP API.
func (h *Host) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(openAPI))
	})
	mux.HandleFunc("GET /v1/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, h.GetSnapshot())
	})
	mux.HandleFunc("GET /v1/events", h.serveEvents)
	mux.HandleFunc("POST /v1/server/reload", func(w http.ResponseWriter, _ *http.Request) {
		if err := h.Reload(); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /v1/endpoints", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, h.Endpoints("extension"))
	})
	mux.HandleFunc("GET /v1/trunks", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, h.Endpoints("trunk"))
	})
	mux.HandleFunc("GET /v1/endpoints/{id}", func(w http.ResponseWriter, r *http.Request) {
		e, err := h.Endpoint(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, e)
	})
	mux.HandleFunc("POST /v1/endpoints/{id}/listen", func(w http.ResponseWriter, r *http.Request) {
		s, err := h.StartListen(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, s)
	})
	mux.HandleFunc("DELETE /v1/endpoints/{id}/listen", func(w http.ResponseWriter, r *http.Request) {
		if err := h.StopListen(r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/endpoints/{id}/hangup", func(w http.ResponseWriter, r *http.Request) {
		if err := h.HangupEndpoint(r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /v1/calls", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, h.Calls())
	})
	mux.HandleFunc("PUT /v1/calls/{id}", func(w http.ResponseWriter, r *http.Request) {
		c, err := h.EnsureCall(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, c)
	})
	mux.HandleFunc("GET /v1/calls/{id}", func(w http.ResponseWriter, r *http.Request) {
		c, err := h.GetCall(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, c)
	})
	mux.HandleFunc("DELETE /v1/calls/{id}", func(w http.ResponseWriter, r *http.Request) {
		if err := h.EndCall(r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/calls/{id}/participants", func(w http.ResponseWriter, r *http.Request) {
		var req AddParticipantRequest
		if !readJSON(w, r, &req) {
			return
		}
		p, err := h.AddParticipant(r.PathValue("id"), req)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Location", "/v1/calls/"+escapePath(r.PathValue("id"))+"/participants/"+escapePath(p.ID))
		writeJSON(w, http.StatusAccepted, p)
	})
	mux.HandleFunc("GET /v1/calls/{id}/participants/{pid}", func(w http.ResponseWriter, r *http.Request) {
		c, err := h.GetCall(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		for _, p := range c.Participants {
			if p.ID == r.PathValue("pid") {
				writeJSON(w, http.StatusOK, p)
				return
			}
		}
		writeError(w, ErrNotFound)
	})
	mux.HandleFunc("DELETE /v1/calls/{id}/participants/{pid}", func(w http.ResponseWriter, r *http.Request) {
		if err := h.RemoveParticipant(r.PathValue("id"), r.PathValue("pid")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/calls/{id}/participants/{pid}/commands", func(w http.ResponseWriter, r *http.Request) {
		var req CommandRequest
		if !readJSON(w, r, &req) {
			return
		}
		if req.Action == "transfer" {
			writeProblem(w, http.StatusNotImplemented, "transfer is not implemented yet")
			return
		}
		cmd, err := h.ParticipantCommand(r.PathValue("id"), r.PathValue("pid"), req)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Location", "/v1/commands/"+escapePath(cmd.ID))
		writeJSON(w, http.StatusAccepted, cmd)
	})
	mux.HandleFunc("POST /v1/calls/{id}/playbacks", func(w http.ResponseWriter, r *http.Request) {
		var req PlayRequest
		if !readJSON(w, r, &req) {
			return
		}
		if err := h.Play(r.PathValue("id"), req); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("POST /v1/calls/{id}/recordings", func(w http.ResponseWriter, _ *http.Request) {
		writeProblem(w, http.StatusNotImplemented, "recording is not implemented yet")
	})
	mux.HandleFunc("POST /v1/calls/{id}/streams", func(w http.ResponseWriter, r *http.Request) {
		s, err := h.StartCallStream(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, s)
	})
	mux.HandleFunc("DELETE /v1/calls/{id}/streams", func(w http.ResponseWriter, r *http.Request) {
		if err := h.StopCallStream(r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/calls/{id}/transcription", func(w http.ResponseWriter, r *http.Request) {
		t, err := h.StartTranscription(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, t)
	})
	mux.HandleFunc("DELETE /v1/calls/{id}/transcription", func(w http.ResponseWriter, r *http.Request) {
		if err := h.StopTranscription(r.PathValue("id")); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/calls/{id}/speech", func(w http.ResponseWriter, r *http.Request) {
		var req SayRequest
		if !readJSON(w, r, &req) {
			return
		}
		sp, err := h.Say(r.PathValue("id"), req.Text)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, sp)
	})
	mux.HandleFunc("GET /v1/commands/{id}", func(w http.ResponseWriter, r *http.Request) {
		cmd, err := h.GetCommand(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, cmd)
	})
	return h.authenticate(mux)
}

// authenticate requires the bearer token on everything except health.
func (h *Host) authenticate(next http.Handler) http.Handler {
	want := []byte("Bearer " + h.cfg.Token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeProblem(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Host) serveEvents(w http.ResponseWriter, r *http.Request) {
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "bad after")
			return
		}
		after = parsed
	}
	streamID := r.URL.Query().Get("streamId")
	// Fail before streaming starts, so a client can resync from a snapshot.
	h.mu.Lock()
	expired := (streamID != "" && streamID != h.streamID) || (len(h.events) > 0 && after < h.events[0].ID-1)
	h.mu.Unlock()
	if expired {
		writeProblem(w, http.StatusConflict, "event cursor expired")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	rc.Flush()
	for {
		events, err := h.waitEvents(r.Context(), streamID, after)
		if err != nil {
			return
		}
		for _, ev := range events {
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, data); err != nil {
				return
			}
			after = ev.ID
		}
		rc.Flush()
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(v); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid JSON")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"title":  strings.ToLower(http.StatusText(status)),
		"status": status,
		"detail": detail,
	})
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrForbidden):
		status = http.StatusForbidden
	case errors.Is(err, ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, ErrOffline):
		status = http.StatusServiceUnavailable
	}
	writeProblem(w, status, err.Error())
}
