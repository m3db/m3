package storage

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
)

type bootstrapSourceEndHook struct {
	shards []databaseShard
}

func newBootstrapSourceEndHook(shards []databaseShard) bootstrap.Hook {
	return &bootstrapSourceEndHook{shards: shards}
}

func (h *bootstrapSourceEndHook) Run() error {
	var wg sync.WaitGroup
	for _, shard := range h.shards {
		shard := shard
		wg.Add(1)
		go func() {
			shard.UpdateFlushStates()
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}
