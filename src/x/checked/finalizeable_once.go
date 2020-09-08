package checked

import "go.uber.org/atomic"

type FinalizeableOnce struct {
	finalized     atomic.Bool
}

func (c *FinalizeableOnce) Finalized() bool {
	return c.finalized.Load()
}

func (c *FinalizeableOnce) SetFinalized(f bool) {
	c.finalized.Store(f)
}
