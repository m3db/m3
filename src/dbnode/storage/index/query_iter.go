package index

import (
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/context"
)

type queryIter struct {
	// immutable state
	docIter doc.QueryDocIterator

	// mutable state
	seriesCount, docCount int
}

var _ QueryIterator = &queryIter{}

// NewQueryIter wraps the provided QueryDocIterator as a QueryIterator
func NewQueryIter(docIter doc.QueryDocIterator) QueryIterator {
	return &queryIter{
		docIter: docIter,
	}
}

func (q *queryIter) Done() bool {
	return q.docIter.Done()
}

func (q *queryIter) Next(_ context.Context) bool {
	return q.docIter.Next()
}

func (q *queryIter) Err() error {
	return q.docIter.Err()
}

func (q *queryIter) SearchDuration() time.Duration {
	return q.docIter.SearchDuration()
}

func (q *queryIter) Close() error {
	return q.docIter.Close()
}

func (q *queryIter) AddSeries(count int) {
	q.seriesCount += count
}

func (q *queryIter) AddDocs(count int) {
	q.docCount += count
}

func (q *queryIter) Counts() (series, docs int) {
	return q.seriesCount, q.docCount
}

func (q *queryIter) Current() doc.Document {
	return q.docIter.Current()
}
