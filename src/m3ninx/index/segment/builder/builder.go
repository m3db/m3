// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package builder

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/util"
	"go.uber.org/atomic"

	"github.com/cespare/xxhash"
)

var (
	errDocNotFound = errors.New("doc not found")
)

const (
	// Slightly buffer the work to avoid blocking main thread.
	indexQueueSize = 2 << 7
)

type indexJob struct {
	id    postings.ID
	field doc.Field

	shard    int
	idx      int
	batchErr *index.BatchPartialError
}

type builder struct {
	sync.Mutex

	opts      Options
	newUUIDFn util.NewUUIDFn

	offset postings.ID

	batchSizeOne index.Batch
	docs         []doc.Document
	idSet        *IDsMap
	fields       *fieldsMap
	uniqueFields [][][]byte

	wg          sync.WaitGroup
	indexQueues []chan indexJob
	closed      *atomic.Bool
}

// NewBuilderFromDocuments returns a builder from documents, it is
// not thread safe and is optimized for insertion speed and a
// final build step when documents are indexed.
func NewBuilderFromDocuments(opts Options) (segment.CloseableDocumentsBuilder, error) {
	concurrency := runtime.NumCPU()
	b := &builder{
		opts:      opts,
		newUUIDFn: opts.NewUUIDFn(),
		batchSizeOne: index.Batch{
			Docs: make([]doc.Document, 1),
		},
		idSet: NewIDsMap(IDsMapOptions{
			InitialSize: opts.InitialCapacity(),
		}),
		fields: newFieldsMap(fieldsMapOptions{
			InitialSize: opts.InitialCapacity(),
		}),
		uniqueFields: make([][][]byte, 0, concurrency),
		indexQueues:  make([]chan indexJob, 0, concurrency),
		closed:       atomic.NewBool(false),
	}

	for i := 0; i < concurrency; i++ {
		indexQueue := make(chan indexJob, indexQueueSize)
		b.indexQueues = append(b.indexQueues, indexQueue)
		go b.indexWorker(indexQueue)

		// Give each shard a fraction of the configured initial capacity.
		shardInitialCapcity := opts.InitialCapacity()
		if shardInitialCapcity > 0 {
			shardInitialCapcity /= concurrency
		}
		shardUniqueFields := make([][]byte, 0, shardInitialCapcity)
		b.uniqueFields = append(b.uniqueFields, shardUniqueFields)
	}

	return b, nil
}

func (b *builder) Reset(offset postings.ID) {
	b.offset = offset

	// Reset the documents slice.
	var empty doc.Document
	for i := range b.docs {
		b.docs[i] = empty
	}
	b.docs = b.docs[:0]

	// Remove all entries in the ID set.
	b.idSet.Reset()

	// Keep fields around, just reset the terms set for each one.
	for _, entry := range b.fields.Iter() {
		entry.Value().reset()
	}

	// Reset the unique fields slice
	for i, shardUniqueFields := range b.uniqueFields {
		for i := range shardUniqueFields {
			shardUniqueFields[i] = nil
		}
		b.uniqueFields[i] = shardUniqueFields[:0]
	}
}

func (b *builder) Insert(d doc.Document) ([]byte, error) {
	// Use a preallocated slice to make insert able to avoid alloc
	// a slice to call insert batch with.
	b.batchSizeOne.Docs[0] = d
	err := b.InsertBatch(b.batchSizeOne)
	if err != nil {
		return nil, err
	}
	last := b.docs[len(b.docs)-1]
	return last.ID, nil
}

func (b *builder) InsertBatch(batch index.Batch) error {
	// NB(r): This is all kept in a single method to make the
	// insertion path fast.
	batchErr := index.NewBatchPartialError()
	for i, d := range batch.Docs {
		// Validate doc
		if err := d.Validate(); err != nil {
			batchErr.Add(index.BatchError{Err: err, Idx: i})
			continue
		}

		// Generate ID if needed.
		if !d.HasID() {
			id, err := b.newUUIDFn()
			if err != nil {
				batchErr.Add(index.BatchError{Err: err, Idx: i})
				continue
			}

			d.ID = id

			// Update the document in the batch since we added an ID to it.
			batch.Docs[i] = d
		}

		// Avoid duplicates.
		if _, ok := b.idSet.Get(d.ID); ok {
			batchErr.Add(index.BatchError{Err: index.ErrDuplicateID, Idx: i})
			continue
		}

		// Write to document set.
		b.idSet.SetUnsafe(d.ID, struct{}{}, IDsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})

		// Every new document just gets the next available id.
		postingsListID := len(b.docs)
		b.docs = append(b.docs, d)

		// Index the terms.
		for _, f := range d.Fields {
			b.index(postings.ID(postingsListID), f, i, batchErr)
		}
		b.index(postings.ID(postingsListID), doc.Field{
			Name:  doc.IDReservedFieldName,
			Value: d.ID,
		}, i, batchErr)
	}

	// Wait for all the concurrent indexing jobs to finish.
	b.wg.Wait()

	if !batchErr.IsEmpty() {
		return batchErr
	}
	return nil
}

func (b *builder) index(
	id postings.ID,
	f doc.Field,
	i int,
	batchErr *index.BatchPartialError,
) {
	// Do-nothing if we are already closed to avoid send on closed panics.
	if b.closed.Load() {
		return
	}
	b.wg.Add(1)
	// NB(bodu): To avoid locking inside of the terms, we shard the work
	// by field name.
	shard := int(xxhash.Sum64(f.Name) % uint64(len(b.indexQueues)))
	b.indexQueues[shard] <- indexJob{
		id:       id,
		field:    f,
		shard:    shard,
		idx:      i,
		batchErr: batchErr,
	}
}

func (b *builder) indexWorker(indexQueue chan indexJob) {
	for job := range indexQueue {
		terms, ok := b.fields.Get(job.field.Name)
		if !ok {
			b.Lock()
			// NB(bodu): Check again within the lock to make sure we aren't making concurrent map writes.
			terms, ok = b.fields.Get(job.field.Name)
			if !ok {
				terms = newTerms(b.opts)
				b.fields.SetUnsafe(job.field.Name, terms, fieldsMapSetUnsafeOptions{
					NoCopyKey:     true,
					NoFinalizeKey: true,
				})
			}
			b.Unlock()
		}

		// If empty field, track insertion of this key into the fields
		// collection for correct response when retrieving all fields.
		newField := terms.size() == 0
		// NB(bodu): Bulk of the cpu time during insertion is spent inside of terms.post().
		if err := terms.post(job.field.Value, job.id); err != nil {
			job.batchErr.AddWithLock(index.BatchError{Err: err, Idx: job.idx})
		}
		if newField {
			b.uniqueFields[job.shard] = append(b.uniqueFields[job.shard], job.field.Name)
		}
		b.wg.Done()
	}
}

func (b *builder) AllDocs() (index.IDDocIterator, error) {
	rangeIter := postings.NewRangeIterator(b.offset,
		b.offset+postings.ID(len(b.docs)))
	return index.NewIDDocIterator(b, rangeIter), nil
}

func (b *builder) Doc(id postings.ID) (doc.Document, error) {
	idx := int(id - b.offset)
	if idx < 0 || idx >= len(b.docs) {
		return doc.Document{}, errDocNotFound
	}

	return b.docs[idx], nil
}

func (b *builder) Docs() []doc.Document {
	return b.docs
}

func (b *builder) FieldsIterable() segment.FieldsIterable {
	return b
}

func (b *builder) TermsIterable() segment.TermsIterable {
	return b
}

func (b *builder) Fields() (segment.FieldsIterator, error) {
	return NewOrderedBytesSliceIter(b.uniqueFields), nil
}

func (b *builder) Terms(field []byte) (segment.TermsIterator, error) {
	terms, ok := b.fields.Get(field)
	if !ok {
		return nil, fmt.Errorf("field not found: %s", string(field))
	}

	// NB(r): Ensure always sorted so can be used to build an FST which
	// requires in order insertion.
	terms.sortIfRequired()

	return newTermsIter(terms.uniqueTerms), nil
}

func (b *builder) Close() error {
	b.closed.Store(true)
	for _, q := range b.indexQueues {
		close(q)
	}
	return nil
}
