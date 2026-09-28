package jobs

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	MaxPayloadBytes   = 256 << 10
	MaxMetadataBytes  = 16 << 10
	MaxAttemptsLimit  = 25
	MaxTimeoutSeconds = 24 * 60 * 60
)

// EnqueueParams is a request to create a job. Zero values mean "use the default".
type EnqueueParams struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	Metadata       json.RawMessage
	Priority       Priority
	MaxAttempts    int
	TimeoutSeconds int
	RunAt          *time.Time // nil = now; a future time creates a scheduled job
	IdempotencyKey string     // "" = none
	RequestID      string     // recorded on the enqueued event for correlation
}

// ValidationError reports an invalid enqueue field.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Message }

// Normalize applies defaults and validates the params.
func (p *EnqueueParams) Normalize() error {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage(`{}`)
	}
	if len(p.Metadata) == 0 {
		p.Metadata = json.RawMessage(`{}`)
	}
	if p.Priority == 0 {
		p.Priority = PriorityNormal
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.TimeoutSeconds == 0 {
		p.TimeoutSeconds = 300
	}

	switch {
	case !ValidName(p.Type):
		return invalid("type", "must be 1-128 characters of letters, digits, '.', '_', ':' or '-'")
	case !ValidName(p.Queue) || len(p.Queue) > 64:
		return invalid("queue", "must be 1-64 characters of letters, digits, '.', '_', ':' or '-'")
	case !json.Valid(p.Payload):
		return invalid("payload", "must be valid JSON")
	case len(p.Payload) > MaxPayloadBytes:
		return invalid("payload", fmt.Sprintf("must be at most %d bytes", MaxPayloadBytes))
	case !isJSONObject(p.Metadata):
		return invalid("metadata", "must be a JSON object")
	case len(p.Metadata) > MaxMetadataBytes:
		return invalid("metadata", fmt.Sprintf("must be at most %d bytes", MaxMetadataBytes))
	case !p.Priority.valid():
		return invalid("priority", "must be critical, high, normal or low")
	case p.MaxAttempts < 1 || p.MaxAttempts > MaxAttemptsLimit:
		return invalid("max_attempts", fmt.Sprintf("must be between 1 and %d", MaxAttemptsLimit))
	case p.TimeoutSeconds < 1 || p.TimeoutSeconds > MaxTimeoutSeconds:
		return invalid("timeout_seconds", fmt.Sprintf("must be between 1 and %d", MaxTimeoutSeconds))
	case len(p.IdempotencyKey) > 255:
		return invalid("idempotency_key", "must be at most 255 characters")
	}
	return nil
}

// ValidName reports whether s is a valid job type or queue name.
func ValidName(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}

func isJSONObject(b json.RawMessage) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal(b, &m) == nil && m != nil
}

func invalid(field, msg string) error { return &ValidationError{Field: field, Message: msg} }
