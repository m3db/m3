package native

import (
	"time"
	"net/http"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler/prometheus"
)

// RequestParams are the arguments to a call
type RequestParams struct {
	Start   time.Time
	End     time.Time
	Timeout time.Duration
}

// ParseRequestParams parses the input request parameters and provides useful defaults
func ParseRequestParams(r *http.Request) (RequestParams, error) {
	param := RequestParams{}
	timeout, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		return param, err
	}

	param.Timeout = timeout


}


