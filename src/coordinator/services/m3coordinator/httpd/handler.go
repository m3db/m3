package httpd

import (
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	pprofURL = "/debug/pprof/profile"
)

// Handler represents an HTTP handler.
type Handler struct {
	Router    *mux.Router
	CLFLogger *log.Logger
	storage   storage.Storage
	engine    *executor.Engine
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(storage storage.Storage, engine *executor.Engine) (*Handler, error) {
	r := mux.NewRouter()
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	defer logger.Sync() // flushes buffer, if any
	h := &Handler{
		CLFLogger: log.New(os.Stderr, "[httpd] ", 0),
		Router:    r,
		storage:   storage,
		engine:    engine,
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging
	h.Router.HandleFunc(handler.PromReadURL, logged(handler.NewPromReadHandler(h.engine)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(handler.PromWriteURL, logged(handler.NewPromWriteHandler(h.storage)).ServeHTTP).Methods("POST")
	h.registerProfileEndpoints()
}

// Endpoints useful for profiling the service
func (h *Handler) registerProfileEndpoints() {
	h.Router.HandleFunc(pprofURL, pprof.Profile)
}

func withResponseTimeLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		rqCtx := logging.NewContextWithGeneratedID(r.Context())
		logger := logging.WithContext(rqCtx)

		// Propagate the context with the reqId
		next.ServeHTTP(w, r.WithContext(rqCtx))
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			logger.Info("finished handling request", zap.Time("time", endTime), zap.Duration("response", d), zap.String("url", r.URL.RequestURI()))
		}
	})
}
