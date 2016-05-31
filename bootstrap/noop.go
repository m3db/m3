package bootstrap

import (
	"time"

	"code.uber.internal/infra/memtsdb"
)

type noOpBootstrapProcess struct {
	opts memtsdb.DatabaseOptions
}

// NewNoOpBootstrapProcess creates a no-op bootstrap process.
func NewNoOpBootstrapProcess(opts memtsdb.DatabaseOptions) memtsdb.Bootstrap {
	return &noOpBootstrapProcess{opts: opts}
}

func (b *noOpBootstrapProcess) Run(writeStart time.Time, shard uint32) (memtsdb.ShardResult, error) {
	return NewShardResult(b.opts), nil
}
