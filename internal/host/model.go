package host

// APIVersion is the version of this API, and appears in its paths.
const APIVersion = 1

// Listen policies, set per endpoint in config. Unknown endpoints get PolicyNever.
const (
	PolicyNever     = "never"      // never streamed
	PolicyInCall    = "in_call"    // streamed only while already in a call
	PolicyOnCommand = "on_command" // an explicit command may also place a private call
)

// Participant statuses.
const (
	StatusRinging  = "ringing"
	StatusJoined   = "joined"
	StatusNoAnswer = "no_answer"
	StatusBusy     = "busy"
	StatusFailed   = "failed"
	StatusLeft     = "left"
)

// Call statuses.
const (
	CallActive = "active"
	CallEnded  = "ended"
)

// Stream statuses match the media_stream entity.
const (
	StreamIdle     = "idle"
	StreamStarting = "starting"
	StreamLive     = "live"
	StreamError    = "error"
)

// Server is the Asterisk server.
type Server struct {
	Connected   bool   `json:"connected"`
	Version     string `json:"version,omitempty"`
	ActiveCalls int    `json:"activeCalls"`
	Error       string `json:"error,omitempty"`
}

// Features describes what an endpoint can do. Asterisk cannot report these, so
// they come from config.
type Features struct {
	AutoAnswer bool   `json:"autoAnswer"`
	Listen     string `json:"listen"`
}

// Endpoint is a phone, an intercom or a trunk.
type Endpoint struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Kind     string   `json:"kind"` // extension, trunk, service
	Role     string   `json:"role,omitempty"`
	Status   string   `json:"status"` // online, offline
	Call     string   `json:"call,omitempty"`
	Features Features `json:"features"`
	Stream   *Stream  `json:"stream,omitempty"`
	// Inbox is set on service numbers that accept text messages.
	Inbox *Inbox `json:"inbox,omitempty"`
}

// Inbox is the latest text message to a service number. Seq rises with every one.
type Inbox struct {
	Seq  int    `json:"seq"`
	From string `json:"from,omitempty"`
	Text string `json:"text,omitempty"`
	At   string `json:"at,omitempty"`
}

// MessageRequest sends a text message from a service number to an extension.
type MessageRequest struct {
	To   string `json:"to"`
	Text string `json:"text"`
}

// Stream is a way to listen to a call.
type Stream struct {
	Status  string `json:"status"`
	URL     string `json:"url,omitempty"`
	Viewers int    `json:"viewers"`
	Error   string `json:"error,omitempty"`
}

// Transcript is speech-to-text on a call. Seq rises with every utterance.
type Transcript struct {
	Status string `json:"status"` // idle, starting, listening, error
	Seq    int    `json:"seq"`
	Text   string `json:"text,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Transcript statuses.
const (
	TranscriptIdle      = "idle"
	TranscriptStarting  = "starting"
	TranscriptListening = "listening"
	TranscriptError     = "error"
)

// CallSpeech is text being spoken into a call. Seq rises with every utterance.
type CallSpeech struct {
	Status string `json:"status"` // idle, speaking, error
	Seq    int    `json:"seq"`
	Text   string `json:"text,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Speech statuses.
const (
	SpeechIdle     = "idle"
	SpeechSpeaking = "speaking"
	SpeechError    = "error"
)

// SayRequest speaks text into a call.
type SayRequest struct {
	Text string `json:"text"`
}

// Participant is one party in a call.
type Participant struct {
	ID       string `json:"id"`
	Target   string `json:"target"`
	Status   string `json:"status"`
	Muted    bool   `json:"muted"`
	Held     bool   `json:"held"`
	LastDTMF string `json:"lastDtmf,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Call is a conversation. Everyone in it can hear each other.
type Call struct {
	ID           string        `json:"id"`
	Status       string        `json:"status"`
	Reason       string        `json:"reason,omitempty"`
	CreatedAt    string        `json:"createdAt"`
	EndedAt      string        `json:"endedAt,omitempty"`
	Participants []Participant `json:"participants"`
	Stream       *Stream       `json:"stream,omitempty"`
	Transcript   *Transcript   `json:"transcript,omitempty"`
	Speech       *CallSpeech   `json:"speech,omitempty"`
}

// Snapshot is the complete state, and the sequence to follow events from.
type Snapshot struct {
	APIVersion int        `json:"apiVersion"`
	StreamID   string     `json:"streamId"`
	Sequence   int64      `json:"sequence"`
	Server     Server     `json:"server"`
	Endpoints  []Endpoint `json:"endpoints"`
	Calls      []Call     `json:"calls"`
}

// Event types.
const (
	EventServer   = "server.updated"
	EventEndpoint = "endpoint.updated"
	EventCall     = "call.updated"
	EventRemoved  = "call.removed"
)

// Event is one change. Data is the complete updated object.
type Event struct {
	Version   int    `json:"version"`
	ID        int64  `json:"id"`
	StreamID  string `json:"streamId"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Data      any    `json:"data"`
}

// Command tracks an action that was accepted.
type Command struct {
	ID     string `json:"id"`
	Status string `json:"status"` // succeeded, failed
	Action string `json:"action"`
	Error  string `json:"error,omitempty"`
}

// AddParticipantRequest dials someone into a call.
type AddParticipantRequest struct {
	Target      string `json:"target"`
	RingSeconds int    `json:"ringSeconds,omitempty"`
}

// CommandRequest acts on a participant.
type CommandRequest struct {
	Action     string         `json:"action"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

// PlayRequest plays a sound into a call.
type PlayRequest struct {
	Sound      string `json:"sound"`
	ThenHangup bool   `json:"thenHangup,omitempty"`
}
