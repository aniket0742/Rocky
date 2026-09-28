package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestNormalizeDefaults(t *testing.T) {
	p := EnqueueParams{Type: "send_email"}
	if err := p.Normalize(); err != nil {
		t.Fatal(err)
	}
	if p.Queue != "default" || p.Priority != PriorityNormal || p.MaxAttempts != 5 ||
		p.TimeoutSeconds != 300 || string(p.Payload) != "{}" || string(p.Metadata) != "{}" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestNormalizeRejects(t *testing.T) {
	big := json.RawMessage(`"` + strings.Repeat("x", MaxPayloadBytes) + `"`)
	tests := []struct {
		field string
		p     EnqueueParams
	}{
		{"type", EnqueueParams{}},
		{"type", EnqueueParams{Type: "has space"}},
		{"queue", EnqueueParams{Type: "t", Queue: strings.Repeat("q", 65)}},
		{"payload", EnqueueParams{Type: "t", Payload: json.RawMessage(`{bad`)}},
		{"payload", EnqueueParams{Type: "t", Payload: big}},
		{"metadata", EnqueueParams{Type: "t", Metadata: json.RawMessage(`[1]`)}},
		{"priority", EnqueueParams{Type: "t", Priority: 9}},
		{"max_attempts", EnqueueParams{Type: "t", MaxAttempts: -1}},
		{"max_attempts", EnqueueParams{Type: "t", MaxAttempts: MaxAttemptsLimit + 1}},
		{"timeout_seconds", EnqueueParams{Type: "t", TimeoutSeconds: MaxTimeoutSeconds + 1}},
		{"idempotency_key", EnqueueParams{Type: "t", IdempotencyKey: strings.Repeat("k", 256)}},
	}
	for _, tt := range tests {
		var ve *ValidationError
		if err := tt.p.Normalize(); !errors.As(err, &ve) || ve.Field != tt.field {
			t.Errorf("%s: got %v", tt.field, err)
		}
	}
}

func TestParsePriority(t *testing.T) {
	for s, want := range map[string]Priority{"": PriorityNormal, "critical": PriorityCritical, "low": PriorityLow} {
		if got, err := ParsePriority(s); err != nil || got != want {
			t.Errorf("ParsePriority(%q) = %v, %v", s, got, err)
		}
	}
	if _, err := ParsePriority("urgent"); err == nil {
		t.Error("expected error for unknown priority")
	}
	if b, _ := PriorityHigh.MarshalText(); string(b) != "high" {
		t.Errorf("MarshalText = %s", b)
	}
}
