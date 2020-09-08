package checked

import "go.uber.org/atomic"

// FinalizeableOnce implements the logics needed to enforce single Finalize on pool.ObjectPool.
type FinalizeableOnce struct {
	finalized     atomic.Bool
}

// Finalized returns true iff the object has been finalized.
func (c *FinalizeableOnce) Finalized() bool {
	return c.finalized.Load()
}

// SetFinalized sets the flag of object finalize.
func (c *FinalizeableOnce) SetFinalized(f bool) {
	c.finalized.Store(f)
}
