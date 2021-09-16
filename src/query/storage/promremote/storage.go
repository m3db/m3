// Copyright (c) 2021  Uber Technologies, Inc.
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

package promremote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const metricsScope = "prom_remote_storage"

// NewStorage returns new Prometheus remote write compatible storage
func NewStorage(opts Options) (storage.Storage, error) {
	client := xhttp.NewHTTPClient(opts.httpOptions)
	scope := opts.scope.SubScope(metricsScope)
	s := &promStorage{opts: opts, client: client, endpointMetrics: initEndpointMetrics(opts.endpoints, scope)}
	return s, nil
}

type promStorage struct {
	opts            Options
	client          *http.Client
	endpointMetrics map[string]*instrument.MethodMetrics
}

func (p *promStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	encoded, err := encodeWriteQuery(query)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	multiErr := xerrors.NewMultiError()
	var errLock sync.Mutex
	for _, endpoint := range p.opts.endpoints {
		endpoint := endpoint
		if endpoint.resolution == query.Attributes().Resolution &&
			endpoint.retention == query.Attributes().Retention {
			metrics := p.endpointMetrics[endpoint.name]
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := p.writeSingle(ctx, metrics, endpoint.address, bytes.NewBuffer(encoded))
				if err != nil {
					errLock.Lock()
					multiErr = multiErr.Add(err)
					errLock.Unlock()
					return
				}
			}()
		}
	}

	wg.Wait()

	if !multiErr.Empty() {
		return multiErr
	}
	return nil
}

func (p *promStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}

func (p *promStorage) Close() error {
	p.client.CloseIdleConnections()
	return nil
}

func (p *promStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorFail
}

func (p *promStorage) Name() string {
	return "prom-remote"
}

func (p *promStorage) writeSingle(
	ctx context.Context,
	metrics *instrument.MethodMetrics,
	address string,
	encoded io.Reader,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, encoded)
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)

	start := time.Now()
	resp, err := p.client.Do(req)
	methodDuration := time.Since(start)
	if err != nil {
		metrics.ReportError(methodDuration)
		return err
	}
	metrics.ReportSuccess(methodDuration)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		metrics.ReportError(methodDuration)
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			response = []byte(fmt.Sprintf("error reading body: %v", err))
		}
		return fmt.Errorf("expected status code 2XX: actual=%v, address=%v, resp=%s",
			resp.StatusCode, address, response)
	}
	return nil
}

func initEndpointMetrics(endpoints []EndpointOptions, scope tally.Scope) map[string]*instrument.MethodMetrics {
	metrics := make(map[string]*instrument.MethodMetrics, len(endpoints))
	for _, endpoint := range endpoints {
		endpointScope := scope.Tagged(map[string]string{"endpoint_name": endpoint.name})
		methodMetrics := instrument.NewMethodMetrics(endpointScope, "writeSingle", instrument.TimerOptions{
			Type:             instrument.HistogramTimerType,
			HistogramBuckets: tally.DefaultBuckets,
		})
		metrics[endpoint.name] = &methodMetrics
	}
	return metrics
}

var _ storage.Storage = &promStorage{}
