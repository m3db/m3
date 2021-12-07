package consolidators

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type concatIterator struct {
	first, second encoding.SeriesIterator
	inSecond      bool
}

func newConcatIterator(first, second encoding.SeriesIterator) encoding.SeriesIterator {
	return &concatIterator{first: first, second: second}
}

func (c *concatIterator) Next() bool {
	if c.first.Next() {
		return true
	}
	c.inSecond = true
	return c.second.Next()
}

func (c *concatIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	if c.inSecond {
		return c.second.Current()
	}
	return c.first.Current()
}

func (c *concatIterator) Err() error {
	if err := c.first.Err(); err != nil {
		return err
	}
	return c.second.Err()
}

func (c *concatIterator) Close() {
	c.first.Close()
	c.second.Close()
}

func (c *concatIterator) ID() ident.ID {
	return c.first.ID()
}

func (c *concatIterator) Namespace() ident.ID {
	return c.first.Namespace()
}

func (c *concatIterator) Start() xtime.UnixNano {
	return c.first.Start()
}

func (c *concatIterator) End() xtime.UnixNano {
	return c.second.End()
}

func (c *concatIterator) FirstAnnotation() ts.Annotation {
	return c.first.FirstAnnotation()
}

func (c *concatIterator) Reset(opts encoding.SeriesIteratorOptions) {
	c.first.Reset(opts)
	c.second.Reset(opts)
}

func (c *concatIterator) SetIterateEqualTimestampStrategy(strategy encoding.IterateEqualTimestampStrategy) {
	c.first.SetIterateEqualTimestampStrategy(strategy)
	c.second.SetIterateEqualTimestampStrategy(strategy)
}

func (c *concatIterator) Stats() (encoding.SeriesIteratorStats, error) {
	firstStats, err := c.first.Stats()
	if err != nil {
		return firstStats, err
	}

	secondStats, err := c.second.Stats()
	if err != nil {
		return secondStats, err
	}

	return firstStats.Combine(secondStats), nil
}

func (c *concatIterator) Replicas() ([]encoding.MultiReaderIterator, error) {
	firstReplicas, err := c.first.Replicas()
	if err != nil {
		return firstReplicas, err
	}

	secondReplicas, err := c.second.Replicas()
	if err != nil {
		return secondReplicas, err
	}

	return append(firstReplicas, secondReplicas...), nil
}

func (c *concatIterator) Tags() ident.TagIterator {
	return c.first.Tags()
}
