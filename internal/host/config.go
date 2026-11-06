package host

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// EndpointConfig is what Asterisk cannot tell us about an endpoint.
type EndpointConfig struct {
	Name       string `json:"name,omitempty"`
	Role       string `json:"role,omitempty"`
	AutoAnswer bool   `json:"autoAnswer,omitempty"`
	Listen     string `json:"listen,omitempty"`
	// Dial overrides the dial string, such as Local/601@from-internal for a test tone.
	Dial string `json:"dial,omitempty"`
}

// Config configures a Host.
type Config struct {
	Bind      string
	Token     string
	ARIURL    string
	ARIUser   string
	ARIPass   string
	Endpoints map[string]EndpointConfig
	Trunks    map[string]bool
	// DialAllow lists outside numbers that may be dialed. Empty means none.
	DialAllow       map[string]bool
	MaxCall         time.Duration
	MaxListen       time.Duration
	RetainEnded     time.Duration
	RefreshInterval time.Duration
}

// Enabled reports whether the host has what it needs to serve.
func (c Config) Enabled() error {
	var missing []string
	for name, v := range map[string]string{
		"ASTERISK_HOST_TOKEN": c.Token,
		"ASTERISK_ARI_URL":    c.ARIURL,
		"ASTERISK_ARI_USER":   c.ARIUser,
		"ASTERISK_ARI_PASS":   c.ARIPass,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	return nil
}

// LoadConfig reads config from the environment.
func LoadConfig() (Config, error) {
	c := Config{
		Bind:            os.Getenv("ASTERISK_HOST_BIND"),
		Token:           os.Getenv("ASTERISK_HOST_TOKEN"),
		ARIURL:          os.Getenv("ASTERISK_ARI_URL"),
		ARIUser:         os.Getenv("ASTERISK_ARI_USER"),
		ARIPass:         os.Getenv("ASTERISK_ARI_PASS"),
		Endpoints:       map[string]EndpointConfig{},
		Trunks:          listSet(os.Getenv("ASTERISK_HOST_TRUNKS")),
		DialAllow:       listSet(os.Getenv("ASTERISK_HOST_DIAL_ALLOW")),
		MaxCall:         envSeconds("ASTERISK_HOST_MAX_CALL_SECONDS", 900),
		MaxListen:       envSeconds("ASTERISK_HOST_MAX_LISTEN_SECONDS", 300),
		RetainEnded:     envSeconds("ASTERISK_HOST_RETAIN_ENDED_SECONDS", 60),
		RefreshInterval: envSeconds("ASTERISK_HOST_REFRESH_SECONDS", 30),
	}
	if c.Bind == "" {
		c.Bind = "127.0.0.1:47832"
	}
	if raw := os.Getenv("ASTERISK_HOST_ENDPOINTS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Endpoints); err != nil {
			return c, fmt.Errorf("ASTERISK_HOST_ENDPOINTS: %w", err)
		}
		for id, e := range c.Endpoints {
			switch e.Listen {
			case "", PolicyNever, PolicyInCall, PolicyOnCommand:
			default:
				return c, fmt.Errorf("ASTERISK_HOST_ENDPOINTS: %s: unknown listen policy %q", id, e.Listen)
			}
		}
	}
	return c, nil
}

func listSet(raw string) map[string]bool {
	out := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out[item] = true
		}
	}
	return out
}

func envSeconds(name string, fallback int) time.Duration {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return time.Duration(v) * time.Second
	}
	return time.Duration(fallback) * time.Second
}
