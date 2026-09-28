package jobs

import "fmt"

// Priority orders claims: lower values are claimed first.
// The zero value means "unset" and normalizes to PriorityNormal.
type Priority int16

const (
	PriorityCritical Priority = 1
	PriorityHigh     Priority = 2
	PriorityNormal   Priority = 3
	PriorityLow      Priority = 4
)

var priorityNames = [...]string{"critical", "high", "normal", "low"}

// ParsePriority parses a priority name; "" means normal.
func ParsePriority(s string) (Priority, error) {
	if s == "" {
		return PriorityNormal, nil
	}
	for i, name := range priorityNames {
		if s == name {
			return Priority(i + 1), nil
		}
	}
	return 0, &ValidationError{Field: "priority", Message: "must be critical, high, normal or low"}
}

func (p Priority) valid() bool { return p >= PriorityCritical && p <= PriorityLow }

func (p Priority) String() string {
	if !p.valid() {
		return fmt.Sprintf("priority(%d)", p)
	}
	return priorityNames[p-1]
}

func (p Priority) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *Priority) UnmarshalText(b []byte) error {
	v, err := ParsePriority(string(b))
	*p = v
	return err
}
