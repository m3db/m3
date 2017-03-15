package coordinator

import (
	"time"

	"github.com/m3db/m3nsch"
	"github.com/m3db/m3x/instrument"
)

var (
	defaultRPCTimeout  = time.Minute
	defaultParallelOps = true
)

type coordinatorOpts struct {
	iopts    instrument.Options
	timeout  time.Duration
	parallel bool
}

// NewOptions creates a new options struct.
func NewOptions(
	iopts instrument.Options,
) m3nsch.CoordinatorOptions {
	return &coordinatorOpts{
		iopts:    iopts,
		timeout:  defaultRPCTimeout,
		parallel: defaultParallelOps,
	}
}

func (mo *coordinatorOpts) SetInstrumentOptions(iopts instrument.Options) m3nsch.CoordinatorOptions {
	mo.iopts = iopts
	return mo
}

func (mo *coordinatorOpts) InstrumentOptions() instrument.Options {
	return mo.iopts
}

func (mo *coordinatorOpts) SetTimeout(d time.Duration) m3nsch.CoordinatorOptions {
	mo.timeout = d
	return mo
}

func (mo *coordinatorOpts) Timeout() time.Duration {
	return mo.timeout
}

func (mo *coordinatorOpts) SetParallelOperations(f bool) m3nsch.CoordinatorOptions {
	mo.parallel = f
	return mo
}

func (mo *coordinatorOpts) ParallelOperations() bool {
	return mo.parallel
}
