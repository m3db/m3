package clock

import (
	"time"
)

// OffsetClock represents offset clock which returns current time from the initial seed time value.
type OffsetClock struct {
	timeDelta time.Duration
	nowFn     NowFn
}

// NewOffsetClock returns new offset clock.
func NewOffsetClock(offsetTime time.Time, nowFn NowFn) OffsetClock {
	return OffsetClock{nowFn: nowFn, timeDelta: offsetTime.Sub(nowFn())}
}

// Now returns current time from the initial seed time value.
func (c OffsetClock) Now() time.Time {
	now := c.nowFn()
	return now.Add(c.timeDelta)
}
