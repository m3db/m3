// +build big
//
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

package commitlog

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/os"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
	"github.com/stretchr/testify/require"
)

const (
	defaultTestFlushInterval = time.Millisecond
)

type seriesWritesAndReadPosition struct {
	writes       []generatedWrite
	readPosition int
}

func TestCommitLogReadWrite(t *testing.T) {
	baseTestPath, err := ioutil.TempDir("", "commit-log-test-base-dir")
	require.NoError(t, err)
	defer os.RemoveAll(baseTestPath)

	opts := NewOptions().SetStrategy(StrategyWriteBehind)
	fsOpts := opts.FilesystemOptions().SetFilePathPrefix(baseTestPath)
	opts = opts.SetFilesystemOptions(fsOpts).SetFlushInterval(time.Millisecond)

	cl, err := NewCommitLog(opts)
	require.NoError(t, err)
	require.NoError(t, cl.Open())

	params := gopter.DefaultGenParameters()
	writeResult := gen.SliceOfN(100, genWrite())(params)
	writesInterface, ok := writeResult.Retrieve()
	require.True(t, ok)
	writes, ok := writesInterface.([]generatedWrite)
	require.True(t, ok)

	ctx := context.NewContext()
	for _, w := range writes {
		require.NoError(t, cl.Write(ctx, w.series, w.datapoint, w.unit, w.annotation))
	}
	ctx.Close()
	require.NoError(t, cl.Close())

	i := 0
	iterOpts := IteratorOpts{
		CommitLogOptions:      opts,
		FileFilterPredicate:   ReadAllPredicate(),
		SeriesFilterPredicate: ReadAllSeriesPredicate(),
	}
	iter, err := NewIterator(iterOpts)
	require.NoError(t, err)
	defer iter.Close()

	// Convert the writes to be in-order, but keyed by series ID because the
	// commitlog reader only guarantees the same order on disk within a
	// given series
	writesBySeries := map[string]seriesWritesAndReadPosition{}
	for _, write := range writes {
		seriesWrites := writesBySeries[write.series.ID.String()]
		if seriesWrites.writes == nil {
			seriesWrites.writes = []generatedWrite{}
		}
		seriesWrites.writes = append(seriesWrites.writes, write)
		writesBySeries[write.series.ID.String()] = seriesWrites
	}

	for ; iter.Next(); i++ {
		series, datapoint, _, _ := iter.Current()
		require.NoError(t, iter.Err())

		seriesWrites := writesBySeries[series.ID.String()]
		write := seriesWrites.writes[seriesWrites.readPosition]

		require.Equal(t, write.series.ID.String(), series.ID.String())
		require.Equal(t, write.series.Namespace.String(), series.Namespace.String())
		require.Equal(t, write.series.Shard, series.Shard)
		require.Equal(t, write.datapoint.Value, datapoint.Value)
		require.True(t, write.datapoint.Timestamp.Equal(datapoint.Timestamp))

		seriesWrites.readPosition++
		writesBySeries[series.ID.String()] = seriesWrites
	}
	require.Equal(t, len(writes), i)
}

// TestCommitLogPropTest property tests the commitlog by performing various
// operations (Open, Write, Close) in various orders, and then ensuring that
// all the data can be read back. In addition, in some runs it will arbitrarily
// (based on a randomly generate probability) corrupt any bytes written to disk by
// the commitlog to ensure that the commitlog reader is resilient to arbitrarily
// corrupted files and will not deadlock / panic.
func TestCommitLogPropTest(t *testing.T) {
	// Temporarily reduce size of buffered channels to increase chance of
	// catching deadlock issues.
	var (
		oldDecoderInBufChanSize  = decoderInBufChanSize
		oldDecoderOutBufChanSize = decoderOutBufChanSize
	)
	defer func() {
		decoderInBufChanSize = oldDecoderInBufChanSize
		decoderOutBufChanSize = oldDecoderOutBufChanSize
	}()
	decoderInBufChanSize = 0
	decoderOutBufChanSize = 0

	basePath, err := ioutil.TempDir("", "commit-log-tests")
	require.NoError(t, err)
	defer os.RemoveAll(basePath)

	var (
		seed       = time.Now().Unix()
		parameters = gopter.DefaultTestParametersWithSeed(seed)
		reporter   = gopter.NewFormatedReporter(true, 160, os.Stdout)
	)
	parameters.MinSuccessfulTests = 20
	properties := gopter.NewProperties(parameters)

	comms := clCommandFunctor(t, basePath, seed)
	properties.Property("CommitLog System", commands.Prop(comms))
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

// clCommandFunctor is a var which implements the command.Commands interface,
// i.e. is responsible for creating/destroying the system under test and generating
// commands and initial states (clState)
func clCommandFunctor(t *testing.T, basePath string, seed int64) *commands.ProtoCommands {
	return &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			return initialState
		},
		DestroySystemUnderTestFunc: func(sut commands.SystemUnderTest) {
			state := sut.(*clState)
			if cl := state.cLog; cl != nil {
				cl.Close()
			}
			os.RemoveAll(state.opts.FilesystemOptions().FilePathPrefix())
		},
		InitialStateGen: genState(t, basePath, seed),
		InitialPreConditionFunc: func(state commands.State) bool {
			if state == nil {
				return false
			}
			_, ok := state.(*clState)
			return ok
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return gen.OneGenOf(genOpenCommand, genCloseCommand, genWriteBehindCommand)
		},
	}
}

// operations on *clState

var genOpenCommand = gen.Const(&commands.ProtoCommand{
	Name: "Open",
	PreConditionFunc: func(state commands.State) bool {
		return !state.(*clState).open
	},
	RunFunc: func(q commands.SystemUnderTest) commands.Result {
		var err error
		s := q.(*clState)
		s.cLog, err = NewCommitLog(s.opts)
		if err != nil {
			return err
		}

		if s.shouldCorrupt {
			cLog := s.cLog.(*commitLog)
			cLog.newCommitLogWriterFn = func(flushFn flushFn, opts Options) commitLogWriter {
				wIface := newCommitLogWriter(flushFn, opts)
				w := wIface.(*writer)
				w.chunkWriter = newCorruptingChunkWriter(
					w.chunkWriter.(*fsChunkWriter),
					s.corruptionProbability,
					s.seed,
				)
				return w
			}
		}
		return s.cLog.Open()
	},
	NextStateFunc: func(state commands.State) commands.State {
		s := state.(*clState)
		s.open = true
		return s
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		if result == nil {
			return &gopter.PropResult{Status: gopter.PropTrue}
		}
		return &gopter.PropResult{
			Status: gopter.PropFalse,
			Error:  result.(error),
		}
	},
})

var genCloseCommand = gen.Const(&commands.ProtoCommand{
	Name: "Close",
	PreConditionFunc: func(state commands.State) bool {
		return state.(*clState).open
	},
	RunFunc: func(q commands.SystemUnderTest) commands.Result {
		s := q.(*clState)
		return s.cLog.Close()
	},
	NextStateFunc: func(state commands.State) commands.State {
		s := state.(*clState)
		s.open = false
		s.cLog = nil
		return s
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		if result != nil {
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error:  result.(error),
			}
		}
		s := state.(*clState)
		err := s.writesArePresent(s.pendingWrites...)
		if err != nil {
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error:  err.(error),
			}
		}
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
})

var genWriteBehindCommand = gen.SliceOfN(10, genWrite()).
	Map(func(writes []generatedWrite) commands.Command {
		return &commands.ProtoCommand{
			Name: "WriteBehind",
			PreConditionFunc: func(state commands.State) bool {
				return state.(*clState).open
			},
			RunFunc: func(q commands.SystemUnderTest) commands.Result {
				s := q.(*clState)
				ctx := context.NewContext()
				defer ctx.Close()

				for _, w := range writes {
					err := s.cLog.Write(ctx, w.series, w.datapoint, w.unit, w.annotation)
					if err != nil {
						return err
					}
				}

				return nil
			},
			NextStateFunc: func(state commands.State) commands.State {
				s := state.(*clState)
				s.pendingWrites = append(s.pendingWrites, writes...)
				return s
			},
			PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
				if result == nil {
					return &gopter.PropResult{Status: gopter.PropTrue}
				}
				return &gopter.PropResult{
					Status: gopter.PropFalse,
					Error:  result.(error),
				}
			},
		}
	})

// clState holds the expected state (i.e. its the commands.State), and we use it as the SystemUnderTest
type clState struct {
	basePath      string
	opts          Options
	open          bool
	cLog          CommitLog
	pendingWrites []generatedWrite
	// Whether the test should corrupt the commit log.
	shouldCorrupt bool
	// If the test should corrupt the commit log, what is the probability of
	// corruption for any given write.
	corruptionProbability float64
	// Seed for use with all RNGs so that runs are reproducible.
	seed int64
}

// generator for commit log write
func genState(t *testing.T, basePath string, seed int64) gopter.Gen {
	return gopter.CombineGens(gen.Identifier(), gen.Bool(), gen.Float64Range(0, 1)).
		MapResult(func(r *gopter.GenResult) *gopter.GenResult {
			iface, ok := r.Retrieve()
			if !ok {
				return gopter.NewEmptyResult(reflect.PtrTo(reflect.TypeOf(clState{})))
			}
			p, ok := iface.([]interface{})
			if !ok {
				return gopter.NewEmptyResult(reflect.PtrTo(reflect.TypeOf(clState{})))
			}

			var (
				initPath              = path.Join(basePath, p[0].(string))
				shouldCorrupt         = p[1].(bool)
				corruptionProbability = p[2].(float64)
				result                = newInitState(
					t, initPath, shouldCorrupt, corruptionProbability, seed)
			)
			return gopter.NewGenResult(result, gopter.NoShrinker)
		})
}

func newInitState(
	t *testing.T,
	dir string,
	shouldCorrupt bool,
	corruptionProbability float64,
	seed int64,
) *clState {
	opts := NewOptions().
		SetStrategy(StrategyWriteBehind).
		SetFlushInterval(defaultTestFlushInterval).
		// Need to set this to a relatively low value otherwise the test will
		// time out because its allocating so much memory for the byte pools
		// in the commit log reader.
		SetFlushSize(1024)
	fsOpts := opts.FilesystemOptions().SetFilePathPrefix(dir)
	opts = opts.SetFilesystemOptions(fsOpts)
	return &clState{
		basePath:              dir,
		opts:                  opts,
		shouldCorrupt:         shouldCorrupt,
		corruptionProbability: corruptionProbability,
		seed: seed,
	}
}

func (s *clState) writesArePresent(writes ...generatedWrite) error {
	writesOnDisk := make(map[string]map[xtime.UnixNano]generatedWrite)
	iterOpts := IteratorOpts{
		CommitLogOptions:      s.opts,
		FileFilterPredicate:   ReadAllPredicate(),
		SeriesFilterPredicate: ReadAllSeriesPredicate(),
	}
	iter, err := NewIterator(iterOpts)
	if err != nil {
		if s.shouldCorrupt {
			return nil
		}
		return err
	}

	defer iter.Close()

	count := 0
	for iter.Next() {
		series, datapoint, unit, annotation := iter.Current()
		idString := series.ID.String()
		seriesMap, ok := writesOnDisk[idString]
		if !ok {
			seriesMap = make(map[xtime.UnixNano]generatedWrite)
			writesOnDisk[idString] = seriesMap
		}
		seriesMap[xtime.ToUnixNano(datapoint.Timestamp)] = generatedWrite{
			series:     series,
			datapoint:  datapoint,
			unit:       unit,
			annotation: annotation,
		}
		count++
	}

	if s.shouldCorrupt {
		return nil
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed after reading %d datapoints: %v", count, err)
	}

	missingErr := fmt.Errorf("writesOnDisk: %+v, writes: %+v", writesOnDisk, writes)
	for _, w := range writes {
		idString := w.series.ID.String()
		seriesMap, ok := writesOnDisk[idString]
		if !ok {
			return missingErr
		}
		gw, ok := seriesMap[xtime.ToUnixNano(w.datapoint.Timestamp)]
		if !ok {
			return missingErr
		}
		if !gw.series.Namespace.Equal(w.series.Namespace) ||
			gw.series.Shard != w.series.Shard ||
			gw.datapoint.Value != w.datapoint.Value {
			return missingErr
		}
	}
	return nil
}

type generatedWrite struct {
	series     Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

func (w generatedWrite) String() string {
	return fmt.Sprintf("ID = %v, Datapoint = %+v", w.series.ID.String(), w.datapoint)
}

// generator for commit log write
func genWrite() gopter.Gen {
	return gopter.CombineGens(
		gen.Identifier(),
		gen.TimeRange(time.Now(), 15*time.Minute),
		gen.Float64(),
		gen.Identifier(),
		gen.UInt32(),
	).Map(func(val []interface{}) generatedWrite {
		id := val[0].(string)
		t := val[1].(time.Time)
		v := val[2].(float64)
		ns := val[3].(string)
		shard := val[4].(uint32)

		return generatedWrite{
			series: Series{
				ID:          ident.StringID(id),
				Namespace:   ident.StringID(ns),
				Shard:       shard,
				UniqueIndex: uniqueID(ns, id),
			},
			datapoint: ts.Datapoint{
				Timestamp: t,
				Value:     v,
			},
			unit: xtime.Nanosecond,
		}
	})
}

type globalMetricIdx struct {
	sync.Mutex

	idx uint64
	// NB(prateek): we use a map from ns -> series id (string) -> unique uint64, to
	// ensure we assign a unique value to each series/ns combination. Further, we
	// also ensure the value is consistent. i.e. repeated generations of the same
	// id/ns retrieve the same uint64 as earlier.
	idToIdx map[string]map[string]uint64
}

var metricIdx = globalMetricIdx{
	idToIdx: make(map[string]map[string]uint64),
}

func uniqueID(ns, s string) uint64 {
	metricIdx.Lock()
	defer metricIdx.Unlock()

	nsMap, ok := metricIdx.idToIdx[ns]
	if !ok {
		nsMap = make(map[string]uint64)
		metricIdx.idToIdx[ns] = nsMap
	}

	i, ok := nsMap[s]
	if ok {
		return i
	}

	idx := metricIdx.idx
	metricIdx.idx++

	nsMap[s] = idx
	return idx
}

// corruptingChunkWriter implements the chunkWriter interface and can corrupt all writes issued
// to it based on a configurable probability.
type corruptingChunkWriter struct {
	chunkWriter           *fsChunkWriter
	corruptionProbability float64
	seed                  int64
}

func newCorruptingChunkWriter(
	chunkWriter *fsChunkWriter,
	corruptionProbability float64,
	seed int64,
) chunkWriter {
	return &corruptingChunkWriter{
		chunkWriter:           chunkWriter,
		corruptionProbability: corruptionProbability,
		seed: seed,
	}
}

func (c *corruptingChunkWriter) reset(f xos.File) {
	c.chunkWriter.fd = xtest.NewCorruptingFile(
		f, c.corruptionProbability, c.seed)
}

func (c *corruptingChunkWriter) Write(p []byte) (int, error) {
	return c.chunkWriter.Write(p)
}

func (c *corruptingChunkWriter) close() error {
	return c.chunkWriter.close()
}

func (c *corruptingChunkWriter) isOpen() bool {
	return c.chunkWriter.isOpen()
}
