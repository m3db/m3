package local

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"

	"github.com/m3db/m3db/client"
	xtime "github.com/m3db/m3x/time"
)

const (
	initRawFetchAllocSize = 32
)

type localStorage struct {
	session        client.Session
	namespace      string
	policyResolver resolver.PolicyResolver
}

// NewStorage creates a new local Storage instance.
func NewStorage(session client.Session, namespace string, policyResolver resolver.PolicyResolver) storage.Storage {
	return &localStorage{session: session, namespace: namespace, policyResolver: policyResolver}
}

func (s *localStorage) Fetch(ctx context.Context, query *models.ReadQuery) (*storage.FetchResult, error) {
	fetchReqs, err := s.policyResolver.Resolve(ctx, query.TagMatchers, query.Start, query.End)
	if err != nil {
		return nil, err
	}

	req := fetchReqs[0]
	reqRange := req.Ranges[0]
	iter, err := s.session.Fetch(s.namespace, req.ID, reqRange.Start, reqRange.End)
	if err != nil {
		return nil, err
	}

	defer iter.Close()

	result := make([]ts.Datapoint, 0, initRawFetchAllocSize)
	for iter.Next() {
		dp, _, _ := iter.Current()
		result = append(result, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
	}

	millisPerStep := int(reqRange.StoragePolicy.Resolution().Window / time.Millisecond)
	values := ts.NewValues(ctx, millisPerStep, len(result))

	// TODO: Figure out consolidation here
	for i, v := range result {
		values.SetValueAt(i, v.Value)
	}

	// TODO: Get the correct metric name
	series := ts.NewSeries(ctx, query.TagMatchers.ID(), reqRange.Start, values)
	seriesList := make([]*ts.Series, 1)
	seriesList[0] = series
	return &storage.FetchResult{
		SeriesList: seriesList,
	}, nil
}

func (s *localStorage) Write(tags models.Tags, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return nil
}
