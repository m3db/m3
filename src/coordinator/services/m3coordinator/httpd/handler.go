package httpd

import (
	"log"
	"os"
	"fmt"
	"net/http"
	"time"

	"github.com/m3db/m3coordinator/services/m3coordinator/handler"

	"go.uber.org/zap"
	"github.com/gorilla/mux"
)

// Handler represents an HTTP handler.
type Handler struct {
	Logger    *zap.Logger
	Router    *mux.Router
	CLFLogger *log.Logger
}

// NewHandler returns a new instance of handler with routes.
func NewHandler() (*Handler, error) {
	r := mux.NewRouter()
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	defer logger.Sync() // flushes buffer, if any

	h := &Handler{
		Logger:    logger,
		CLFLogger: log.New(os.Stderr, "[httpd] ", 0),
		Router:    r,
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging
	h.Router.HandleFunc("/api/v1/prom/read", logged(handler.NewPromReadHandler(), h.Logger).ServeHTTP).Methods("POST")
}

func withResponseTimeLogging(next http.Handler, log *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		next.ServeHTTP(w, r)
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			log.Info(fmt.Sprintf("time=%v, RESPONSE [%0.3f] %s\n", endTime, d.Seconds(), r.URL.RequestURI()))
		}
	})
}

