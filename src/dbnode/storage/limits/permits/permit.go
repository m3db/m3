package permits

import (
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/instrument"
)

// permit tracks the quota used by a caller and provides basic sanity checks that a caller
// correctly uses the permit.
type permit struct {
	// immutable state
	quota int64
	iOpts instrument.Options

	// mutable state
	quotaUsed int64
	refCount  atomic.Int32
}

// NewPermit constructs a new Permit with the provided quota.
func NewPermit(quota int64, iOpts instrument.Options) Permit {
	return &permit{
		quota: quota,
		iOpts: iOpts,
	}
}

func (p *permit) Release() {
	if p.iOpts != nil && p.refCount.Dec() != 0 {
		instrument.EmitAndLogInvariantViolation(p.iOpts, func(l *zap.Logger) {
			l.Error("permit released more than once")
		})
	}
}

func (p *permit) Acquire() {
	if p.iOpts != nil && p.refCount.Inc() != 1 {
		instrument.EmitAndLogInvariantViolation(p.iOpts, func(l *zap.Logger) {
			l.Error("permit acquired more than once")
		})
	}
	p.quotaUsed = 0
}

// AllowedQuota is the amount of quota the caller can use with this Permit.
func (p *permit) AllowedQuota() int64 {
	return p.quota
}

// QuotaRemaining is the amount of remaining quota for this Permit. Can be negative if the caller used more quota
// than they were allowed.
func (p *permit) QuotaRemaining() int64 {
	return p.quota - p.quotaUsed
}

// Use adds the quota to the total used quota.
func (p *permit) Use(quota int64) {
	p.quotaUsed += quota
}
