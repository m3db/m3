package storage

import "sync"

type Throttler struct {
	sync.Mutex

	totalInflightWeight      int
	inflightWeightByKey      map[string]int
	outstandingRequestsByKey map[string][]OutstandingRequest
	// Each entry in the queue should be unique to avoid unfair access
	globalQueue []string
	globalLimit int
}

type OutstandingRequest struct {
	waitingRequest chan struct{}
	requestWeight  int
}

// Each client of acquire is performing the equivalent amount of work
func (t *Throttler) Acquire(key string, weight int) {
	t.Lock()
	defer t.Unlock()

	// check if user is under dynamic limit
	// TODO: needs to consider waiting and in-flight keys
	dynamicLimit := t.globalLimit / len(t.inflightWeightByKey)

	// if new weight would keep the key under its dynamic limit, proceed
	if t.inflightWeightByKey[key]+weight < dynamicLimit {
		t.inflightWeightByKey[key] += weight
		t.totalInflightWeight += weight
	} else {
		waitingChan := make(chan struct{}, 0)
		t.outstandingRequestsByKey[key] = append(t.outstandingRequestsByKey[key], OutstandingRequest{
			waitingRequest: waitingChan,
			requestWeight:  weight,
		})
		// block until enough weight is supported
		<-waitingChan
	}
}

func (t *Throttler) Release(key string, weight int) {
	// lock
	t.Lock()
	defer t.Unlock()

	t.totalInflightWeight -= weight
	t.inflightWeightByKey[key] -= weight

	// calculate dynamic limit
	dynamicLimit := t.globalLimit / len(t.globalQueue)

	// dequeue from global queue until the key
	for {
		nextRequest := t.globalQueue

		// case a: user above limit
		nextKeyInflightWeight := t.inflightWeightByKey[nextRequest]
		if nextKeyInflightWeight > dynamicLimit {
			// reenqueue to back
		} else {
			// check if next request in outstanding requests is satisfied by remaining weight
			nextOutstandingRequest := t.outstandingRequestsByKey[nextRequest][0]

			// if the next request puts us over the global limit, then don't do anything
			if t.totalInflightWeight+nextOutstandingRequest.requestWeight > t.globalLimit {
				return
			}

			// if the next request puts us over the user's limit, then re-enqueue the user
			resultingInflightWeight := nextKeyInflightWeight + nextOutstandingRequest.requestWeight
			if resultingInflightWeight > dynamicLimit {
				// re-enqueue
			} else {
				// otherwise, unblock the next request (this should be non-blocking) and remove it from the queue
				nextOutstandingRequest.waitingRequest <- struct{}{}
				t.inflightWeightByKey[nextRequest] += nextOutstandingRequest.requestWeight
				t.totalInflightWeight += nextOutstandingRequest.requestWeight
				return
			}
		}
	}
}
