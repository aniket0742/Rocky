package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aniket0742/rocky/internal/worker"
)

// demoHandlers are safe, predefined job types for local use (and the future hosted demo).
func demoHandlers() *worker.Registry {
	reg := worker.NewRegistry()
	reg.Register("noop", func(context.Context, []byte) error { return nil })
	reg.Register("sleep", sleep)
	reg.Register("fail", func(context.Context, []byte) error { return errors.New("this job always fails") })
	return reg
}

// sleep waits for {"seconds": n}, stopping early if its timeout fires.
func sleep(ctx context.Context, payload []byte) error {
	var in struct {
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal(payload, &in); err != nil {
		return worker.NonRetryable(fmt.Errorf("invalid payload: %w", err)) // retrying won't fix it
	}
	select {
	case <-time.After(time.Duration(in.Seconds * float64(time.Second))):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
