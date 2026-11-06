# asterisk-host

Models Asterisk as endpoints and calls, behind a REST API and event stream. See
`internal/host/api.go` for the routes, or `GET /v1/openapi.json`.

## Concepts

- **Endpoint:** a phone, intercom or trunk, discovered from Asterisk.
- **Call:** a conversation with a name chosen by the caller. Everyone in it can hear
  each other. Creating or dialing into a call is safe to retry.
- **Participant:** one party in a call. Each ends as `no_answer`, `busy`, `failed` or
  `left`, and the reason is reported in state, because scripts only ever see state.
- **Stream:** a way to listen to a call. Stopping one never ends a real conversation.
- **Transcription:** speech-to-text on a call, from `sb-speech`. Each utterance updates
  the call's transcript. It is optional, and follows the same listen policy as a stream.

## Configuration

| Variable | Purpose |
|---|---|
| `ASTERISK_HOST_TOKEN` | Bearer token for the API. Required |
| `ASTERISK_ARI_URL`, `ASTERISK_ARI_USER`, `ASTERISK_ARI_PASS` | Asterisk connection. Required |
| `ASTERISK_HOST_BIND` | Listen address, default `127.0.0.1:47832` |
| `ASTERISK_HOST_ENDPOINTS` | JSON of what Asterisk cannot report: `name`, `role`, `autoAnswer`, `listen`, `dial` |
| `ASTERISK_HOST_TRUNKS` | Comma-separated endpoints that are trunks |
| `ASTERISK_HOST_DIAL_ALLOW` | Comma-separated outside numbers that may be dialed. Empty means none |
| `ASTERISK_HOST_MAX_CALL_SECONDS` | Call limit, default 900 |
| `ASTERISK_HOST_MAX_LISTEN_SECONDS` | Limit on a private listening call, default 300 |
| `ASTERISK_HOST_RETAIN_ENDED_SECONDS` | How long an ended call stays visible, default 60 |

Listen policies are `never` (the default), `in_call` and `on_command`. Connecting to a
stream URL never places a call.

## Safety

The host enforces these whoever calls it. Scripts cannot override them.

- Only known extensions and allowlisted numbers can be dialed, and raw dial strings are refused.
- A stream of a call needs every party's listen policy to allow it.
- Calls end at the time limit. Asterisk also enforces it a few seconds later, in case the host has died.
- A call that exists only for listening ends when its stream ends.
- On start, the host removes its own leftover channels and bridges, and nothing else.
