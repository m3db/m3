package promremotewrite

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

func NewAppender(opts Options) (storage.Appender, error) {
	return &appender{
		opts:   opts,
		client: xhttp.NewHTTPClient(opts.HTTPClientOptions()),
	}, nil
}

type appender struct {
	opts   Options
	client *http.Client
}

func (a *appender) Write(ctx context.Context, query *storage.WriteQuery) error {
	encoded, err := encodeWriteQuery(query)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.opts.endpoint, bytes.NewBuffer(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set("content-type", "application/x-protobuf")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}

	// TODO smth more sophisticated?
	if resp.StatusCode != 200 {
		return fmt.Errorf("remote write endpoint returned non 200 response: %d", resp.StatusCode)
	}
	return nil
}

var _ storage.Appender = &appender{}
