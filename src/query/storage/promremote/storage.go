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

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// NewStorage returns new Prometheus remote write compatible storage
func NewStorage(opts Options) (storage.Storage, error) {
	client := xhttp.NewHTTPClient(opts.HTTPClientOptions())
	scope := opts.scope.SubScope("prom_remote_storage")
	s := &promStorage{opts: opts, client: client, endpointMetrics: initEndpointMetrics(opts.endpoints, scope)}
	return s, nil
}

type endpointMetrics struct {
	successCount tally.Counter
	errCount     tally.Counter
	totalCount   tally.Counter
	latency      tally.Histogram
}

type promStorage struct {
	opts            Options
	client          *http.Client
	endpointMetrics map[string]endpointMetrics
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
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := p.writeSingle(ctx, endpoint.address, bytes.NewBuffer(encoded))
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

func (p *promStorage) writeSingle(ctx context.Context, address string, encoded io.Reader) error {
	p.endpointMetrics[address].totalCount.Inc(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, encoded)
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set("content-type", "application/x-protobuf")
	latencyStopwatch := p.endpointMetrics[address].latency.Start()
	resp, err := p.client.Do(req)
	latencyStopwatch.Stop()
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		p.endpointMetrics[address].errCount.Inc(1)
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			response = []byte(fmt.Sprintf("error reading body: %v", err))
		}
		return fmt.Errorf("expected status code 2XX: actual=%v, address=%v, resp=%s",
			resp.StatusCode, address, response)
	}
	p.endpointMetrics[address].successCount.Inc(1)
	return nil
}

func initEndpointMetrics(endpoints []EndpointOptions, scope tally.Scope) map[string]endpointMetrics {
	metrics := make(map[string]endpointMetrics, len(endpoints))
	for _, endpoint := range endpoints {
		endpointScope := scope.SubScope("endpoint").
			Tagged(map[string]string{"endpoint_address": endpoint.address})
		metrics[endpoint.address] = endpointMetrics{
			successCount: endpointScope.Counter("success"),
			errCount:     endpointScope.Counter("error"),
			totalCount:   endpointScope.Counter("requests"),
			latency:      endpointScope.Histogram("request_latency", tally.DefaultBuckets),
		}
	}
	return metrics
}

var _ storage.Storage = &promStorage{}
