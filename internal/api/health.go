package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Check reports whether a dependency is reachable.
type Check func(context.Context) error

// Dependency is a component reported by /v1/health.
type Dependency struct {
	Name     string
	Check    Check // nil means not configured
	Required bool  // down+required => unavailable (503); down+optional => degraded (200)
}

const checkTimeout = 2 * time.Second

type health struct {
	log  *slog.Logger
	deps []Dependency
}

type healthResponse struct {
	Status string            `json:"status"` // ok | degraded | unavailable
	Checks map[string]string `json:"checks"` // ok | down | disabled
}

func (h *health) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
	defer cancel()

	errs := make([]error, len(h.deps))
	var wg sync.WaitGroup
	for i, d := range h.deps {
		if d.Check != nil {
			wg.Go(func() { errs[i] = d.Check(ctx) })
		}
	}
	wg.Wait()

	resp := healthResponse{Status: "ok", Checks: make(map[string]string, len(h.deps))}
	for i, d := range h.deps {
		switch {
		case d.Check == nil:
			resp.Checks[d.Name] = "disabled"
		case errs[i] == nil:
			resp.Checks[d.Name] = "ok"
		default:
			// Details are logged, not returned, so connection info never leaks to clients.
			h.log.Warn("health check failed", "dependency", d.Name, "err", errs[i])
			resp.Checks[d.Name] = "down"
			if d.Required {
				resp.Status = "unavailable"
			} else if resp.Status == "ok" {
				resp.Status = "degraded"
			}
		}
	}

	code := http.StatusOK
	if resp.Status == "unavailable" {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}
