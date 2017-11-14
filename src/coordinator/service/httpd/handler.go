package httpd

import (
	"log"
	"os"
	"go.uber.org/zap"
	"github.com/gorilla/mux"
	"github.com/m3db/m3coordinator/service/handler"
	"fmt"
	"net/http"
	"time"
)

// Handler represents an HTTP handler.
type Handler struct {
	Logger    *zap.Logger
	Router    *mux.Router
	CLFLogger *log.Logger
}

// NewHandler returns a new instance of handler with routes.
func NewHandler() *Handler {
	r := mux.NewRouter()
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any

	h := &Handler{
		Logger:    logger,
		CLFLogger: log.New(os.Stderr, "[httpd] ", 0),
		Router:    r,
	}
	return h
}

func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging
	h.Router.HandleFunc("/api/v1/prom/read", logged(handler.NewPromReadHandler()).ServeHTTP).Methods("POST")
}

func withResponseTimeLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		next.ServeHTTP(w, r)
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			fmt.Fprintf(os.Stdout, "time=%v, RESPONSE [%0.3f] %s\n", endTime, d.Seconds(), r.URL.RequestURI())
		}
	})
}

