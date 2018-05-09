package test

import (
	"net/http"
	"time"
)

// SlowHandler slows down a request by delay
type SlowHandler struct {
	handler http.Handler
	delay   time.Duration
}

// NewSlowHandler creates a new slow handler
func NewSlowHandler(handler http.Handler, delay time.Duration) *SlowHandler {
	return &SlowHandler{handler: handler, delay: delay}
}

// ServeHTTP implements http.handler
func (h *SlowHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(h.delay)
	h.handler.ServeHTTP(w, r)
}
