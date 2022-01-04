package index

// resultsUtilizationStats tracks utilization of results object in order to detect and prevent
// unbounded growth of reusable object which may otherwise stay in the object pool indefinitely.
type resultsUtilizationStats struct {
	resultsMapCapacity            int
	consecutiveTimesUnderCapacity int
}

const (
	consecutiveTimesUnderCapacityThreshold = 10
)

// updateAndCheck returns true iff the underlying object is supposed to be returned
// to the appropriate object pool for reuse.
func (r *resultsUtilizationStats) updateAndCheck(totalDocsCount int) bool {
	if totalDocsCount < r.resultsMapCapacity {
		r.consecutiveTimesUnderCapacity++
	} else {
		r.resultsMapCapacity = totalDocsCount
		r.consecutiveTimesUnderCapacity = 0
	}

	// If the size of reusable result object is not fully utilized for a long enough time,
	// we want to drop it from the pool and prevent holding on to excessive memory indefinitely.
	return r.consecutiveTimesUnderCapacity < consecutiveTimesUnderCapacityThreshold
}
