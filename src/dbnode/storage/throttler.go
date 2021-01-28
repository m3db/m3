package storage

import (
	"container/list"
	"fmt"
	"sync"
)

// Throttler controls fair access to limited resources.
type Throttler struct {
	sync.Mutex

	keyState map[string]*keyContext
	// Each entry in the queue should be unique to avoid unfair access
	keyQueue *list.List

	globalCurrentClaims int
	globalMaxClaims     int
}

// Claim is a claim to a throttled resource that has
// been granted and must be released.
type Claim struct {
	key       string
	throttler *Throttler
}

type keyContext struct {
	// Requests for weight to be granted which are waiting.
	waiting []chan struct{}
	// Currently granted weight for this key.
	currentClaims int
}

// Release releases the current claim.
func (c *Claim) Release() {
	c.throttler.Release(c.key)
}

// Acquire blocks until the request for a claim is granted for the specified key.
func (t *Throttler) Acquire(key string) (*Claim, error) {
	blockCh, err := t.tryAcquire(key)
	if err != nil {
		return nil, err
	}

	if blockCh != nil {
		<-blockCh
	}

	return &Claim{key: key, throttler: t}, nil
}

func (t *Throttler) tryAcquire(key string) (chan struct{}, error) {
	t.Lock()
	defer t.Unlock()

	maxClaimsPerKey := t.maxClaimsPerKey()

	currentKey, alreadyExists := t.keyState[key]
	if !alreadyExists {
		currentKey = &keyContext{
			currentClaims: 0,
			waiting:       make([]chan struct{}, 0),
		}
		t.keyState[key] = currentKey
	}

	// If below both the per-key and global max claims, then grant the claim.
	if currentKey.currentClaims < maxClaimsPerKey && currentKey.currentClaims < t.globalMaxClaims {
		currentKey.currentClaims++
		t.globalCurrentClaims++
		return nil, nil
	}

	// Otherwise, enqueue this key and block acquisition.
	blockCh := make(chan struct{}, 1)
	currentKey.waiting = append(currentKey.waiting, blockCh)

	// If this is first request by the key, then enqueue it for releasing.
	if !alreadyExists {
		t.keyQueue.PushBack(key)
	}

	// Return the chan the caller should block on since we cannot acquire yet.
	return blockCh, nil
}

// Release frees a claim.
func (t *Throttler) Release(key string) {
	t.Lock()
	defer t.Unlock()

	currentKey := t.keyState[key]

	// Reduce granted weight associated with this key.
	currentKey.currentClaims--
	t.globalCurrentClaims--

	// calculate dynamic limit
	maxClaimsPerKey := t.maxClaimsPerKey()

	// Cycle through the queue of keys waiting for resources to determine
	// the first which could make use of the newly available weight.
	for i := 0; i < t.keyQueue.Len(); i++ {
		nextElement := t.keyQueue.Front()
		nextKey := nextElement.Value.(string)
		nextKeyState := t.keyState[nextKey]
		nextWaiting := nextKeyState.waiting[0]

		fmt.Println("release", i, nextKey)

		// (A) If key is above it's per-key limit, then skip and continue to
		// a different key to grant.
		if nextKeyState.currentClaims >= maxClaimsPerKey {
			t.keyQueue.MoveToBack(nextElement)
			continue
		}

		// (B) If above the global limit then just return to wait for a future release.
		// Keep the current key's queue position though since it should go next.
		if t.globalCurrentClaims >= t.globalMaxClaims {
			return
		}

		// (C) Below both global + per-key limits so unblock the next
		// request and remove it from the queue.
		nextWaiting <- struct{}{}
		nextKeyState.currentClaims++
		t.globalCurrentClaims++
		nextKeyState.waiting = nextKeyState.waiting[1:]

		// If there are more requests, then re-enqueue, otherwise remove.
		if len(nextKeyState.waiting) != 0 {
			t.keyQueue.MoveToBack(nextElement)
		} else {
			t.keyQueue.Remove(nextElement)
		}
	}

	fmt.Println("released", key, t.globalCurrentClaims)
}

func (t *Throttler) maxClaimsPerKey() int {
	// Limit per key such that each key gets an equal
	// share of concurrent grants to claims.
	s := t.keyQueue.Len()
	if s == 0 {
		return t.globalMaxClaims
	}

	m := t.globalMaxClaims / s
	if m <= 1 {
		return 1
	}

	return m
}
