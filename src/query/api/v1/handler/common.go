// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3db/src/query/util/logging"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// RoutePrefixV1 is the v1 prefix for all coordinator routes
	RoutePrefixV1 = "/api/v1"

	durationSuffix = "Duration"
	nanosSuffix    = "Nanos"
)

var (
	errDurationType = errors.New("invalid duration type")
)

// WriteJSONResponse writes generic data to the ResponseWriter
func WriteJSONResponse(w http.ResponseWriter, data interface{}, logger *zap.Logger) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("unable to marshal json", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

// WriteProtoMsgJSONResponse writes a protobuf message to the ResponseWriter. This uses jsonpb
// for json marshalling, which encodes fields with default values, even with the omitempty tag.
func WriteProtoMsgJSONResponse(w http.ResponseWriter, data proto.Message, logger *zap.Logger) {
	marshaler := jsonpb.Marshaler{EmitDefaults: true}

	w.Header().Set("Content-Type", "application/json")
	err := marshaler.Marshal(w, data)
	if err != nil {
		logger.Error("unable to marshal json", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
	}
}

// WriteUninitializedResponse writes a protobuf message to the ResponseWriter
func WriteUninitializedResponse(w http.ResponseWriter, logger *zap.Logger) {
	logger.Warn("attempted call before M3DB is fully initialized")
	http.Error(w, "Unable to perform action before M3DB is fully initialized", http.StatusConflict)
}

// CloseWatcher watches for CloseNotify and context timeout. It is best effort and may sometimes not close the channel relying on gc
func CloseWatcher(ctx context.Context, w http.ResponseWriter) (<-chan bool, <-chan bool) {
	closing := make(chan bool)
	logger := logging.WithContext(ctx)
	var doneChan <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		done := make(chan bool)

		notify := notifier.CloseNotify()
		go func() {
			// Wait for either the request to finish
			// or for the client to disconnect
			select {
			case <-done:
			case <-notify:
				logger.Warn("connection closed by client")
				close(closing)
			case <-ctx.Done():
				// We only care about the time out case and not other cancellations
				if ctx.Err() == context.DeadlineExceeded {
					logger.Warn("request timed out")
				}
				close(closing)
			}
			close(done)
		}()
		doneChan = done
	}

	return doneChan, closing
}

// DurationToNanosBytes transforms a json byte slice with Duration keys into Nanos
func DurationToNanosBytes(r io.Reader) ([]byte, error) {
	var dict map[string]interface{}
	d := json.NewDecoder(r)
	d.UseNumber()
	if err := d.Decode(&dict); err != nil {
		return nil, err
	}

	ret, err := DurationToNanosMap(dict)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ret)
}

// DurationToNanosMap transforms keys with a Duration into Nanos
func DurationToNanosMap(input map[string]interface{}) (map[string]interface{}, error) {
	dictTranslated := make(map[string]interface{}, len(input))

	for k, v := range input {
		if strings.HasSuffix(k, durationSuffix) {
			newKey := strings.Replace(k, durationSuffix, nanosSuffix, 1)

			switch vv := v.(type) {
			case string:
				duration, err := time.ParseDuration(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[newKey] = duration.Nanoseconds()
			case json.Number:
				// json.Number when using a json decoder with UseNumber()
				// Assume given number is in nanos
				vvNum, err := vv.Int64()
				if err != nil {
					return nil, err
				}

				dictTranslated[newKey] = vvNum
			case float64:
				// float64 when unmarshaling without UseNumber()
				// Assume given number is in nanos
				dictTranslated[newKey] = int64(vv)
			default:
				// Has Duration suffix, but is not string or number
				return nil, errDurationType
			}
		} else {
			switch vv := v.(type) {
			case map[string]interface{}:
				durMap, err := DurationToNanosMap(vv)
				if err != nil {
					return nil, err
				}

				dictTranslated[k] = durMap
			default:
				dictTranslated[k] = vv
			}
		}
	}

	return dictTranslated, nil
}
