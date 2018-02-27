package execution

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Request is input for parallel execution
type Request interface {
	Process(ctx context.Context) error
}

// RequestResponse is used to combine both request and response in output
type RequestResponse struct {
	Request  Request
	Response *Response
}

// Response is returned from parallel execution
type Response struct {
	Value interface{}
	Err   error
}

// ExecuteParallel executes a slice of requests in parallel and returns unordered results
func ExecuteParallel(ctx context.Context, requests []Request) error {
	return processParallel(ctx, requests)
}

// Process the requests in parallel and stop on first error
func processParallel(ctx context.Context, requests []Request) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, req := range requests {
		// Need to use a separate func since g.Go doesn't take input
		func(req Request) {
			g.Go(func() error {
				return req.Process(ctx)
			})
		}(req)
	}

	return g.Wait()
}
