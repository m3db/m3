package promremotewrite

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/query/storage"
)

type Options struct {
	endpoint string
}

func NewAppender(opts Options) (storage.Appender, error) {
	return &appender{opts: opts}, nil
}

type appender struct {
	opts Options
}

func (a *appender) Write(ctx context.Context, query *storage.WriteQuery) error {
	encoded, err := encodeWriteQuery(query)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", a.opts.endpoint, bytes.NewBuffer(encoded))
	if err != nil {
		return err
	}
	// TODO which client should I use?
	resp, err := http.DefaultClient.Do(req)
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
