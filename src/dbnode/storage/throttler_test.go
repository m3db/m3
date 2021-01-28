package storage

import (
	"container/list"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestThrottler(t *testing.T) {
	throttler := &Throttler{
		keyState:        make(map[string]*keyContext, 0),
		keyQueue:        list.New(),
		globalMaxClaims: 10,
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			// Simulate distinct small user.
			u := fmt.Sprintf("user_%d", i)
			claim, err := throttler.Acquire(u)
			require.NoError(t, err)
			claim.Release()
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			// Simulate distinct small user.
			u := "user_bad"
			claim, err := throttler.Acquire(u)
			require.NoError(t, err)
			claim.Release()
			wg.Done()
		}()
	}

	wg.Wait()
}
