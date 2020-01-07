package bootstrapper

import "github.com/m3db/m3/src/m3ninx/index/segment"

// Segment wraps an index segment so we can easily determine whether or not the segment is persisted to disk.
type Segment struct {
	persisted bool
	segment.Segment
}

// NewSegment returns an index segment w/ persistence metadata.
func NewSegment(segment segment.Segment, persisted bool) *Segment {
	return &Segment{
		persisted: persisted,
		Segment:   segment,
	}
}

// IsPersisted returns whether or not the underlying segment was persisted to disk.
func (s *Segment) IsPersisted() bool {
	return s.persisted
}
