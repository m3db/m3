package promremotewrite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

func NewStorage(opts Options) (storage.Storage, func(), error) {
	client := xhttp.NewHTTPClient(opts.HTTPClientOptions())
	s := &promStorage{
		opts:   opts,
		client: client,
	}
	return s, func() { client.CloseIdleConnections() }, nil
}

type promStorage struct {
	opts   Options
	client *http.Client
}

func (p *promStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	encoded, err := encodeWriteQuery(query)
	if err != nil {
		return err
	}

	// TODO accumulate errors but try best effort to send everywhere
	// TODO in parallel
	for _, endpoint := range p.opts.endpoints {
		if endpoint.storageMetadata.MetricsType == storagemetadata.UnaggregatedMetricsType ||
			endpoint.storageMetadata.Resolution == query.Attributes().Resolution &&
				endpoint.storageMetadata.Retention == query.Attributes().Retention {
			err = p.sendWrite(ctx, endpoint.address, bytes.NewBuffer(encoded))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *promStorage) sendWrite(ctx context.Context, address string, encoded io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, encoded)
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set("content-type", "application/x-protobuf")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// TODO smth more sophisticated?
	if resp.StatusCode != 200 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		bodyString := string(bodyBytes)
		return fmt.Errorf("remote write endpoint returned non 200 response: %d, %s", resp.StatusCode, bodyString)
	}
	return nil
}

var _ storage.Storage = &promStorage{}
