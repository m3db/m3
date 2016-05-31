package bootstrapper

import (
	"code.uber.internal/infra/memtsdb"

	xtime "code.uber.internal/infra/memtsdb/x/time"
)

const (
	// noOpBootstrapper is the name of the no-op bootstrapper
	noOpBootstrapperName = "noop"
)

var (
	defaultNoOpBootstrapper = &noOpBootstrapper{}
)

type noOpBootstrapper struct{}

// Bootstrap performs bootstrapping for the given shards and the associated time ranges.
func (noop *noOpBootstrapper) Bootstrap(shard uint32, targetRanges xtime.Ranges) (memtsdb.ShardResult, xtime.Ranges) {
	return nil, targetRanges
}

// String returns the name of the boostrapper.
func (noop *noOpBootstrapper) String() string {
	return noOpBootstrapperName
}
