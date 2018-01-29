package resolver

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/tsdb"

	"github.com/m3db/m3metrics/policy"
)

type staticResolver struct {
	sp policy.StoragePolicy
}

// NewStaticResolver creates a static policy resolver.
func NewStaticResolver(sp policy.StoragePolicy) PolicyResolver {
	return &staticResolver{sp: sp}
}

func (r *staticResolver) Resolve(
	ctx context.Context,
	tagMatchers models.Matchers,
	startTime, endTime time.Time,
) ([]tsdb.FetchRequest, error) {
	ranges := tsdb.NewSingleRangeRequest("", startTime, endTime, r.sp).Ranges
	requests := make([]tsdb.FetchRequest, 1)
	tags, err := tagMatchers.ToTags()
	if err != nil {
		return nil, err
	}
	requests[0] = tsdb.FetchRequest{
		ID:     tags.ID(),
		Ranges: ranges,
	}

	return requests, nil
}
