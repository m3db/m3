package local

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"

	xtime "github.com/m3db/m3x/time"
)

type localStorage struct {
}

// NewStorage creates a new local Storage instance.
func NewStorage() storage.Storage {
	return &localStorage{}
}

func (s *localStorage) Fetch(ctx context.Context, tagMatchers []*models.Matcher, start time.Time,
	end time.Time) (*storage.FetchResult, error) {
	return nil, nil
}

func (s *localStorage) Write(tags models.Tags, t time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return nil
}
