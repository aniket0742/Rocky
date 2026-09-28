package worker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/aniket0742/rocky/internal/jobs"
)

// Handler executes one job. Returning nil completes the job; an error fails the
// attempt and is retried with backoff unless wrapped with NonRetryable.
// ctx is cancelled on timeout, lease loss or shutdown; handlers must respect it.
type Handler func(ctx context.Context, payload []byte) error

// NonRetryable marks err as permanent: the job dead-letters instead of retrying.
func NonRetryable(err error) error { return nonRetryable{err} }

type nonRetryable struct{ err error }

func (e nonRetryable) Error() string { return e.err.Error() }
func (e nonRetryable) Unwrap() error { return e.err }

func isNonRetryable(err error) bool { return errors.As(err, new(nonRetryable)) }

// Registry maps job types to handlers.
type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register adds a handler for jobType. It panics on an invalid type, a nil
// handler or a duplicate, since each is a programming error.
func (r *Registry) Register(jobType string, h Handler) {
	switch {
	case !jobs.ValidName(jobType):
		panic(fmt.Sprintf("worker: invalid job type %q", jobType))
	case h == nil:
		panic(fmt.Sprintf("worker: nil handler for %q", jobType))
	case r.handlers[jobType] != nil:
		panic(fmt.Sprintf("worker: duplicate handler for %q", jobType))
	}
	r.handlers[jobType] = h
}

// Types returns the registered job types, sorted.
func (r *Registry) Types() []string { return slices.Sorted(maps.Keys(r.handlers)) }
