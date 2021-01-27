package storage

import (
	"sync"
)

// Throttler controls fair access to limited resources.
type Throttler struct {
	sync.Mutex

	keyState map[string]*keyContext
	// Each entry in the queue should be unique to avoid unfair access
	keyQueue []string

	globalCurrentWeight int
	globalMaxWeight     int
}

type keyContext struct {
	// Requests for weight to be granted which are waiting.
	waiting []request
	// Currently granted weight for this key.
	currentWeight int
}

type request struct {
	blockCh chan struct{}
	weight  int
}

// Acquire blocks until the requested weight is granted to the specified key.
func (t *Throttler) Acquire(key string, weight int) error {
	blockCh, err := t.tryAcquire(key, weight)
	if err != nil {
		return err
	}

	if blockCh != nil {
		<-blockCh
	}

	return nil
}

func (t *Throttler) tryAcquire(key string, weight int) (chan struct{}, error) {
	t.Lock()
	defer t.Unlock()

	maxWeightPerKey := t.maxWeightPerKey()

	currentKey, alreadyExists := t.keyState[key]
	if !alreadyExists {
		t.keyState[key] = &keyContext{
			currentWeight: 0,
			waiting:       make([]request, 0),
		}
	}

	// If new weight would keep the key under the per-key limit then grant.
	// Otherwise enqueue and block.
	if currentKey.currentWeight+weight < maxWeightPerKey {
		currentKey.currentWeight += weight
		t.globalCurrentWeight += weight
	} else {
		blockCh := make(chan struct{}, 0)
		currentKey.waiting = append(currentKey.waiting, request{
			blockCh: blockCh,
			weight:  weight,
		})

		// If this is first request by the key, then enqueue it for releasing.
		if !alreadyExists {
			t.keyQueue = append(t.keyQueue, key)
		}

		// Return the chan the caller should block on since we cannot acquire yet.
		return blockCh, nil
	}

	// Can acquire directly so return nil chan to wait on.
	return nil, nil
}

// Release frees the weight associated with the key to be available for others.
func (t *Throttler) Release(key string, weight int) {
	t.Lock()
	defer t.Unlock()

	currentKey := t.keyState[key]

	// Reduce granted weight associated with this key.
	currentKey.currentWeight -= weight
	t.globalCurrentWeight -= weight

	// calculate dynamic limit
	maxWeightPerKey := t.maxWeightPerKey()

	// Cycle through the queue of keys waiting for resources to determine
	// the first which could make use of the newly available weight.
	for i := 0; i < len(t.keyQueue); i++ {
		nextKey := t.keyQueue[0]
		t.keyQueue = t.keyQueue[1:]
		nextKeyState := t.keyState[nextKey]

		// (A) Key would exceed per-key limit if we granted the current weight, so we skip
		// and add to back of queue to allow for it to release more resources first.
		if nextKeyState.currentWeight+weight > maxWeightPerKey {
			t.keyQueue = append(t.keyQueue, nextKey)
			continue
		}

		nextRequest := nextKeyState.waiting[0]

		// (B) Key would exceed the global limit, so we return to wait for another
		// Release but keep the key's position in the queue to ensure it goes next.
		if t.globalCurrentWeight+nextRequest.weight > t.globalMaxWeight {
			return
		}

		// (C) below both global + per-key limits so unblock the next
		// request and remove it from the queue.
		nextRequest.blockCh <- struct{}{}
		nextKeyState.currentWeight += nextRequest.weight
		t.globalCurrentWeight += nextRequest.weight
		nextKeyState.waiting = nextKeyState.waiting[1:]

		// If there are more requests, then re-enqueue.
		if len(nextKeyState.waiting) != 0 {
			t.keyQueue = append(t.keyQueue, nextKey)
		}
	}
}

func (t *Throttler) maxWeightPerKey() int {
	// Limit per key such that each key gets an equal
	// allocation of the total max weight available.
	m := t.globalMaxWeight / len(t.keyQueue)
	if m <= 1 {
		return 1
	}
	return m
}
