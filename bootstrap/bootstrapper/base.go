package bootstrapper

import (
	"sync"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/bootstrap"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

const (
	baseBootstrapperName = "base"
)

// baseBootstrapper provides a skeleton for the interface methods.
type baseBootstrapper struct {
	s      memtsdb.Source
	dbOpts memtsdb.DatabaseOptions
	next   memtsdb.Bootstrapper
}

// NewBaseBootstrapper creates a new base bootstrapper.
func NewBaseBootstrapper(
	s memtsdb.Source,
	dbOpts memtsdb.DatabaseOptions,
	next memtsdb.Bootstrapper,
) memtsdb.Bootstrapper {
	bs := next
	if next == nil {
		bs = defaultNoOpBootstrapper
	}
	return &baseBootstrapper{s: s, dbOpts: dbOpts, next: bs}
}

// Bootstrap performs bootstrapping for the given shards and the associated time ranges.
func (bsb *baseBootstrapper) Bootstrap(shard uint32, targetRanges xtime.Ranges) (memtsdb.ShardResult, xtime.Ranges) {
	if xtime.IsEmpty(targetRanges) {
		return nil, nil
	}

	availableRanges := bsb.s.GetAvailability(shard, targetRanges)
	remainingRanges := targetRanges.RemoveRanges(availableRanges)

	var (
		wg                              sync.WaitGroup
		curResult, nextResult           memtsdb.ShardResult
		curUnfulfilled, nextUnfulfilled xtime.Ranges
	)

	go func() {
		defer wg.Done()
		nextResult, nextUnfulfilled = bsb.next.Bootstrap(shard, remainingRanges)
	}()

	curResult, curUnfulfilled = bsb.s.ReadData(shard, availableRanges)
	wg.Wait()

	mergedResults := bsb.mergeResults(curResult, nextResult)

	// If there are some time ranges the current bootstrapper can't fulfill,
	// pass it along to the next bootstrapper.
	if !xtime.IsEmpty(curUnfulfilled) {
		curResult, curUnfulfilled = bsb.next.Bootstrap(shard, curUnfulfilled)
		mergedResults = bsb.mergeResults(mergedResults, curResult)
	}

	mergedUnfulfilled := mergeTimeRanges(curUnfulfilled, nextUnfulfilled)
	return mergedResults, mergedUnfulfilled
}

func (bsb *baseBootstrapper) mergeResults(results ...memtsdb.ShardResult) memtsdb.ShardResult {
	final := bootstrap.NewShardResult(bsb.dbOpts)
	for _, result := range results {
		final.AddResult(result)
	}
	return final
}

func mergeTimeRanges(ranges ...xtime.Ranges) xtime.Ranges {
	final := xtime.NewRanges()
	for _, tr := range ranges {
		final = final.AddRanges(tr)
	}
	return final
}

// String returns the name of the bootstrapper.
func (bsb *baseBootstrapper) String() string {
	return baseBootstrapperName
}
