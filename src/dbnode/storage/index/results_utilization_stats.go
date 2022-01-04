package index

type resultsUtilizationStats struct {
	resultsMapCapacity            int
	consecutiveTimesUnderCapacity int
}

const (
	consecutiveTimesUnderCapacityThreshold = 10
)

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
