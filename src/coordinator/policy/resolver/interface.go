package resolver

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/tsdb"
)

// PolicyResolver resolves policy for a query.
type PolicyResolver interface {
	// Resolve will resolve each metric ID to a FetchRequest with a list of FetchRanges.
	// The list of ranges is guaranteed to cover the full [startTime, endTime). The best
	// storage policy will be picked for the range with configured strategy, but there
	// may still be no data retained for the range in the given storage policy.
	Resolve(
		ctx context.Context,
		tagMatchers models.Matchers,
		startTime, endTime time.Time,
	) ([]tsdb.FetchRequest, error)
}
