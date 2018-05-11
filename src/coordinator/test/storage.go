package test

import (
	"context"
	"time"

	"github.com/m3db/m3coordinator/storage"

	"github.com/m3db/m3db/encoding"
	m3ts "github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
)

// slowStorage slows down a request by delay
type slowStorage struct {
	storage storage.Storage
	delay   time.Duration
}

// NewSlowStorage creates a new slow storage
func NewSlowStorage(storage storage.Storage, delay time.Duration) storage.Storage {
	return &slowStorage{storage: storage, delay: delay}
}

func (s *slowStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	time.Sleep(s.delay)
	return s.storage.Fetch(ctx, query, options)
}

func (s *slowStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	time.Sleep(s.delay)
	return s.storage.FetchTags(ctx, query, options)
}

func (s *slowStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	time.Sleep(s.delay)
	return s.storage.Write(ctx, query)
}

func (s *slowStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func (s *slowStorage) Close() error {
	return nil
}

func (s *slowStorage) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.BlockResult, error) {
	time.Sleep(s.delay)
	return s.storage.FetchBlocks(ctx, query, options)
}

// GenerateSingleSampleTagIterator generates a new tag iterator
func GenerateSingleSampleTagIterator(ctrl *gomock.Controller, tag ident.Tag) ident.TagIterator {
	mockTagIterator := ident.NewMockTagIterator(ctrl)
	mockTagIterator.EXPECT().Remaining().Return(1)
	mockTagIterator.EXPECT().Next().Return(true).MaxTimes(1)
	mockTagIterator.EXPECT().Current().Return(tag)
	mockTagIterator.EXPECT().Next().Return(false)
	mockTagIterator.EXPECT().Err().Return(nil)
	mockTagIterator.EXPECT().Close()

	return mockTagIterator
}

// GenerateTag generates a new tag
func GenerateTag() ident.Tag {
	return ident.Tag{
		Name:  ident.StringID("foo"),
		Value: ident.StringID("bar"),
	}
}

// NewMockSeriesIters generates a new mock series iters
func NewMockSeriesIters(ctrl *gomock.Controller, tags ident.Tag) encoding.SeriesIterators {
	mockIter := encoding.NewMockSeriesIterator(ctrl)
	mockIter.EXPECT().Next().Return(true).MaxTimes(2)
	mockIter.EXPECT().Next().Return(false)
	mockIter.EXPECT().Current().Return(m3ts.Datapoint{Timestamp: time.Now(), Value: 10}, xtime.Millisecond, nil)
	mockIter.EXPECT().Current().Return(m3ts.Datapoint{Timestamp: time.Now(), Value: 10}, xtime.Millisecond, nil)
	mockIter.EXPECT().ID().Return(ident.StringID("foo"))
	mockIter.EXPECT().Tags().Return(GenerateSingleSampleTagIterator(ctrl, tags))

	mockIter.EXPECT().Close()

	mockIters := encoding.NewMockSeriesIterators(ctrl)
	mockIters.EXPECT().Iters().Return([]encoding.SeriesIterator{mockIter})
	mockIters.EXPECT().Len().Return(1)
	mockIters.EXPECT().Close()

	return mockIters
}
