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
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const metricsScope = "prom_remote_storage"

var errorReadingBody = []byte("error reading body")

var errNoEndpoints = errors.New("write did not match any of known endpoints")

// NewStorage returns new Prometheus remote write compatible storage
func NewStorage(opts Options) (storage.Storage, error) {
	client := xhttp.NewHTTPClient(opts.httpOptions)
	scope := opts.scope.SubScope(metricsScope)
	s := &promStorage{
		opts:            opts,
		client:          client,
		endpointMetrics: initEndpointMetrics(opts.endpoints, scope),
		droppedWrites:   scope.Counter("dropped_writes"),
		logger:          opts.logger,
		queryQueue:      make(chan *storage.WriteQuery, opts.queueSize),
		workerPool:      xsync.NewWorkerPool(opts.poolSize),
		pendingQuery:    make(map[queryOpts][]*storage.WriteQuery),
	}
	s.StartAsync()
	return s, nil
}

type promStorage struct {
	unimplementedPromStorageMethods
	opts            Options
	client          *http.Client
	endpointMetrics map[string]instrument.MethodMetrics
	droppedWrites   tally.Counter
	logger          *zap.Logger
	queryQueue      chan *storage.WriteQuery
	workerPool      xsync.WorkerPool
	pendingQuery    map[queryOpts][]*storage.WriteQuery
}

type queryOpts struct {
	attributes storagemetadata.Attributes
	headers    string
}

func buildQueryOpts(query *storage.WriteQuery) queryOpts {
	b := new(bytes.Buffer)
	for k, v := range query.Options().KeptHeaders {
		fmt.Fprintf(b, "%s=%s,", k, v)
	}
	if b.Len() > 0 {
		b.Truncate(b.Len() - 1)
	}
	return queryOpts{
		attributes: query.Attributes(),
		headers:    b.String(),
	}
}

func (p *promStorage) StartAsync() {
	p.logger.Info("Start prometheus remote write storage async job",
		zap.Int("poolSize", p.opts.poolSize))
	p.workerPool.Init()
	go func() {
		for {
			ctx := context.Background()
			select {
			case query := <-p.queryQueue:
				qOpts := buildQueryOpts(query)
				if _, ok := p.pendingQuery[qOpts]; !ok {
					p.pendingQuery[qOpts] = make([]*storage.WriteQuery, 0, p.opts.queueSize)
				}
				p.pendingQuery[qOpts] = append(p.pendingQuery[qOpts], query)
				if len(p.pendingQuery[qOpts]) >= p.opts.queueSize {
					retain := p.pendingQuery[qOpts]
					p.pendingQuery[qOpts] = nil
					p.workerPool.Go(func() {
						p.writeBatch(ctx, &qOpts, retain)
					})
				}
				// TODO: add a timer to flush pending queries periodically
			}
		}
	}()
}

func (p *promStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	if query != nil {
		p.queryQueue <- query
	}
	return nil
}

func (p *promStorage) writeBatch(ctx context.Context, qOpts *queryOpts, queries []*storage.WriteQuery) error {
	p.logger.Debug("async write batch",
		zap.String("attributes", qOpts.attributes.String()),
		zap.String("headers", qOpts.headers),
		zap.Int("size", len(queries)))
	encoded, err := convertAndEncodeWriteQuery(queries)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	multiErr := xerrors.NewMultiError()
	var errLock sync.Mutex
	atLeastOneEndpointMatched := false
	for _, endpoint := range p.opts.endpoints {
		endpoint := endpoint
		if endpoint.attributes.Resolution != qOpts.attributes.Resolution ||
			endpoint.attributes.Retention != qOpts.attributes.Retention {
			continue
		}

		metrics := p.endpointMetrics[endpoint.name]
		atLeastOneEndpointMatched = true

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.writeSingle(ctx, metrics, endpoint, qOpts.headers, bytes.NewReader(encoded))
			if err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
				return
			}
		}()
	}

	wg.Wait()

	if !atLeastOneEndpointMatched {
		p.droppedWrites.Inc(1)
		multiErr = multiErr.Add(errNoEndpoints)
		p.logger.Warn(
			"write did not match any of known endpoints",
			zap.Duration("retention", qOpts.attributes.Retention),
			zap.Duration("resolution", qOpts.attributes.Resolution),
		)
	}
	return multiErr.FinalError()
}

func (p *promStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}

func (p *promStorage) Close() error {
	close(p.queryQueue)
	ctx := context.Background()
	var wg sync.WaitGroup
	for qOpts, queries := range p.pendingQuery {
		wg.Add(1)
		p.workerPool.Go(func() {
			p.writeBatch(ctx, &qOpts, queries)
			wg.Done()
		})
	}
	wg.Wait()
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
	metrics instrument.MethodMetrics,
	endpoint EndpointOptions,
	headers string,
	encoded io.Reader,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.address, encoded)
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)
	if endpoint.headers != nil && len(endpoint.headers) > 0 {
		for k, v := range endpoint.headers {
			// set headers defined in remote endpoint options
			req.Header.Set(k, v)
		}
	} else if len(headers) > 0 {
		for _, header := range strings.Split(headers, ",") {
			// set headers from upstream remote write request
			kv := strings.Split(header, "=")
			req.Header.Set(kv[0], kv[1])
		}
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	methodDuration := time.Since(start)
	if err != nil {
		metrics.ReportError(methodDuration)
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		metrics.ReportError(methodDuration)
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			p.logger.Error("error reading body", zap.Error(err))
			response = errorReadingBody
		}
		genericError := fmt.Errorf(
			"expected status code 2XX: actual=%v, address=%v, resp=%s",
			resp.StatusCode, endpoint.address, response,
		)
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return xerrors.NewInvalidParamsError(genericError)
		}
		return genericError
	}
	metrics.ReportSuccess(methodDuration)
	return nil
}

func initEndpointMetrics(endpoints []EndpointOptions, scope tally.Scope) map[string]instrument.MethodMetrics {
	metrics := make(map[string]instrument.MethodMetrics, len(endpoints))
	for _, endpoint := range endpoints {
		endpointScope := scope.Tagged(map[string]string{"endpoint_name": endpoint.name})
		methodMetrics := instrument.NewMethodMetrics(endpointScope, "writeSingle", instrument.TimerOptions{
			Type:             instrument.HistogramTimerType,
			HistogramBuckets: tally.DefaultBuckets,
		})
		metrics[endpoint.name] = methodMetrics
	}
	return metrics
}

var _ storage.Storage = &promStorage{}

type unimplementedPromStorageMethods struct{}

func (p *unimplementedPromStorageMethods) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, unimplementedError("FetchProm")
}

func (p *unimplementedPromStorageMethods) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, unimplementedError("FetchBlocks")
}

func (p *unimplementedPromStorageMethods) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, unimplementedError("FetchCompressed")
}

func (p *unimplementedPromStorageMethods) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, unimplementedError("SearchSeries")
}

func (p *unimplementedPromStorageMethods) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, unimplementedError("CompleteTags")
}

func (p *unimplementedPromStorageMethods) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, unimplementedError("QueryStorageMetadataAttributes")
}

func unimplementedError(name string) error {
	return fmt.Errorf("promStorage: %s method is not supported", name)
}
