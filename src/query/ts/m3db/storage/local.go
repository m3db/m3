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

package storage

import (
	"context"
	"io"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/local"
	m3block "github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

type localStorage struct {
	clusters   local.Clusters
	workerPool pool.ObjectPool
}

var (
	iterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}

	emptySeriesMap map[ident.ID][]m3block.SeriesBlocks
)

// nolint: deadcode
func newStorage(clusters local.Clusters, workerPool pool.ObjectPool) *localStorage {
	return &localStorage{clusters: clusters, workerPool: workerPool}
}

// nolint: unparam
func (s *localStorage) fetchRaw(
	namespace local.ClusterNamespace,
	query index.Query,
	opts index.QueryOptions,
) (encoding.SeriesIterators, bool, error) {
	namespaceID := namespace.NamespaceID()
	session := namespace.Session()
	return session.FetchTagged(namespaceID, query, opts)
}

// todo(braskin): merge this with Fetch()
func (s *localStorage) fetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (map[ident.ID][]m3block.SeriesBlocks, error) {

	m3query, err := storage.FetchQueryToM3Query(query)
	if err != nil {
		return emptySeriesMap, err
	}

	opts := storage.FetchOptionsToM3Options(options, query)

	// todo(braskin): figure out how to deal with multiple namespaces
	namespaces := s.clusters.ClusterNamespaces()
	// todo(braskin): figure out what to do with second return argument
	seriesIters, _, err := s.fetchRaw(namespaces[0], m3query, opts)
	if err != nil {
		return emptySeriesMap, err
	}

	seriesBlockList, err := m3block.ConvertM3DBSeriesIterators(seriesIters, iterAlloc)
	if err != nil {
		return emptySeriesMap, err
	}

	// NB/todo(braskin): because we are only support querying one namespace now, we can just create
	// a multiNamespaceSeriesList with one element. However, once we support querying multiple namespaces,
	// we will need to append each namespace seriesBlockList to the multiNamespaceSeriesList
	namespaceSeriesList := m3block.NamespaceSeriesList{
		Namespace:  namespaces[0].NamespaceID().String(),
		SeriesList: seriesBlockList,
	}

	multiNamespaceSeriesList := []m3block.NamespaceSeriesList{namespaceSeriesList}
	multiNamespaceSeries := fromNamespaceListToSeriesList(multiNamespaceSeriesList)
	return multiNamespaceSeries, nil
}

func (s *localStorage) Close() error {
	return nil
}

func fromNamespaceListToSeriesList(nsList []m3block.NamespaceSeriesList) map[ident.ID][]m3block.SeriesBlocks {
	seriesList := make(map[ident.ID][]m3block.SeriesBlocks)

	for _, ns := range nsList {
		for _, series := range ns.SeriesList {
			if _, ok := seriesList[series.ID]; !ok {
				seriesList[series.ID] = []m3block.SeriesBlocks{
					{
						Blocks:    series.Blocks,
						ID:        series.ID,
						Tags:      series.Tags,
						Namespace: series.Namespace,
					},
				}
			} else {
				seriesList[series.ID] = append(seriesList[series.ID], series)
			}
		}
	}
	return seriesList
}
