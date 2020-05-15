package prom

import (
	"math"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
)

// All of this is taken from prometheus to ensure we have consistent return/error
// formats with prometheus.
// https://github.com/prometheus/prometheus/blob/43acd0e2e93f9f70c49b2267efa0124f1e759e86/web/api/v1/api.go#L1097

const (
	queryParam   = "query"
	startParam   = "start"
	endParam     = "end"
	stepParam    = "step"
	timeoutParam = "timeout"
)

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
	errorNotFound    errorType = "not_found"
)

type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func respond(w http.ResponseWriter, data interface{}, warnings promstorage.Warnings) {
	statusMessage := statusSuccess
	var warningStrings []string
	for _, warning := range warnings {
		warningStrings = append(warningStrings, warning.Error())
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status:   statusMessage,
		Data:     data,
		Warnings: warningStrings,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func respondError(w http.ResponseWriter, err error, code int) {
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(&response{
		Status: statusError,
		Error:  err.Error(),
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}
