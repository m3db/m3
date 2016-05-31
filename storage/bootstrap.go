package storage

import (
	"fmt"
	"sync"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapping
	bootstrapped
)

func analyze(bs bootstrapState, entity string) (bool, error) {
	switch bs {
	case bootstrapped:
		return false, nil
	case bootstrapping:
		return false, fmt.Errorf("%s is being bootstrapped", entity)
	default:
		return true, nil
	}
}

// tryBootstrap attempts to start the bootstrap process. It returns
// 1. (true, nil) if the attempt succeeds;
// 2. (false, nil) if the target has already bootstrapped;
// 3. (false, error) if the target is currently being bootstrapped.
func tryBootstrap(l *sync.RWMutex, s *bootstrapState, entity string) (bool, error) {
	l.RLock()
	bs := *s
	l.RUnlock()
	if success, err := analyze(bs, entity); !success {
		return success, err
	}

	l.Lock()
	// bootstrap state changed during RLock -> WLock promotion
	if success, err := analyze(bs, entity); !success {
		l.Unlock()
		return success, err
	}
	*s = bootstrapping
	l.Unlock()

	return true, nil
}
