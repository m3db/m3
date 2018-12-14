// Copyright (c) 2017 Uber Technologies, Inc.
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
	"bytes"
	"fmt"
	"math/rand"
	"text/tabwriter"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/golang/mock/gomock"
	"github.com/leanovate/gopter"
	"github.com/uber-go/tally"
)

const (
	commitLogTestRandomSeeed        int64 = 7823434
	commitLogTestMinSuccessfulTests       = 10000
)

func newPropTestCleanupMgr(
	ctrl *gomock.Controller, ropts retention.Options, t time.Time, ns ...databaseNamespace) *cleanupManager {
	var (
		blockSize          = ropts.BlockSize()
		commitLogBlockSize = blockSize
		db                 = NewMockdatabase(ctrl)
		opts               = testDatabaseOptions()
	)
	opts = opts.SetCommitLogOptions(
		opts.CommitLogOptions().
			SetBlockSize(commitLogBlockSize))

	db.EXPECT().Options().Return(opts).AnyTimes()
	db.EXPECT().GetOwnedNamespaces().Return(ns, nil).AnyTimes()
	scope := tally.NoopScope
	cmIface := newCleanupManager(db, newNoopFakeActiveLogs(), scope)
	cm := cmIface.(*cleanupManager)

	var (
		oldest    = retention.FlushTimeStart(ropts, t)
		newest    = retention.FlushTimeEnd(ropts, t)
		n         = numIntervals(oldest, newest, blockSize)
		currStart = oldest
	)
	cm.commitLogFilesFn = func(_ commitlog.Options) ([]persist.CommitlogFile, []commitlog.ErrorWithPath, error) {
		files := make([]persist.CommitlogFile, 0, n)
		for i := 0; i < n; i++ {
			files = append(files, persist.CommitlogFile{
				Start:    currStart,
				Duration: blockSize,
			})
		}
		return files, nil, nil
	}

	return cm
}

func newCleanupMgrTestProperties() *gopter.Properties {
	parameters := gopter.DefaultTestParameters()
	parameters.Rng.Seed(commitLogTestRandomSeeed) // generate reproducible results
	parameters.MinSuccessfulTests = commitLogTestMinSuccessfulTests
	return gopter.NewProperties(parameters)
}

// func TestPropertyCommitLogNotCleanedForUnflushedData(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()

// 	properties := newCleanupMgrTestProperties()
// 	now := time.Now()
// 	timeWindow := time.Hour * 24 * 15

// 	properties.Property("Commit log is retained if one namespace needs to flush", prop.ForAll(
// 		func(cleanupTime time.Time, cRopts retention.Options, ns *generatedNamespace) (bool, error) {
// 			cm := newPropTestCleanupMgr(ctrl, cRopts, now, ns)
// 			filesToCleanup, err := cm.commitLogTimes(cleanupTime)
// 			if err != nil {
// 				return false, err
// 			}
// 			for _, file := range filesToCleanup {
// 				if file.err != nil {
// 					continue
// 				}

// 				var (
// 					f                         = file.f
// 					s, e                      = commitLogNamespaceBlockTimes(f.Start, f.Duration, ns.ropts)
// 					needsFlush                = ns.NeedsFlush(s, e)
// 					isCapturedBySnapshot, err = ns.IsCapturedBySnapshot(s, e, f.Start.Add(f.Duration))
// 				)
// 				require.NoError(t, err)

// 				if needsFlush && !isCapturedBySnapshot {
// 					return false, fmt.Errorf("trying to cleanup commit log at %v, but ns needsFlush; (range: %v, %v)",
// 						f.Start.String(), s.String(), e.String())
// 				}
// 			}
// 			return true, nil
// 		},
// 		gen.TimeRange(now.Add(-timeWindow), 2*timeWindow).WithLabel("cleanup time"),
// 		genCommitLogRetention().WithLabel("commit log retention"),
// 		genNamespace(now).WithLabel("namespace"),
// 	))

// 	properties.TestingRun(t)
// }

// func TestPropertyCommitLogNotCleanedForUnflushedDataMultipleNs(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()

// 	properties := newCleanupMgrTestProperties()
// 	now := time.Now()
// 	timeWindow := time.Hour * 24 * 15

// 	properties.Property("Commit log is retained if any namespace needs to flush", prop.ForAll(
// 		func(cleanupTime time.Time, cRopts retention.Options, nses []*generatedNamespace) (bool, error) {
// 			dbNses := generatedNamespaces(nses).asDatabaseNamespace()
// 			cm := newPropTestCleanupMgr(ctrl, cRopts, now, dbNses...)
// 			filesToCleanup, err := cm.commitLogTimes(cleanupTime)
// 			if err != nil {
// 				return false, err
// 			}
// 			for _, file := range filesToCleanup {
// 				if file.err != nil {
// 					continue
// 				}

// 				f := file.f
// 				for _, ns := range nses {
// 					s, e := commitLogNamespaceBlockTimes(f.Start, f.Duration, ns.ropts)
// 					needsFlush := ns.NeedsFlush(s, e)
// 					isCapturedBySnapshot, err := ns.IsCapturedBySnapshot(s, e, f.Start.Add(f.Duration))
// 					require.NoError(t, err)
// 					if needsFlush && !isCapturedBySnapshot {
// 						return false, fmt.Errorf("trying to cleanup commit log at %v, but ns needsFlush; (range: %v, %v)",
// 							f.Start.String(), s.String(), e.String())
// 					}
// 				}
// 			}
// 			return true, nil
// 		},
// 		gen.TimeRange(now.Add(-timeWindow), 2*timeWindow).WithLabel("cleanup time"),
// 		genCommitLogRetention().WithLabel("commit log retention"),
// 		gen.SliceOfN(3, genNamespace(now)).WithLabel("namespaces"),
// 	))

// 	properties.TestingRun(t)
// }

type generatedNamespaces []*generatedNamespace

func (n generatedNamespaces) asDatabaseNamespace() []databaseNamespace {
	nses := make([]databaseNamespace, 0, len(n))
	for _, ns := range n {
		nses = append(nses, ns)
	}
	return nses
}

// generated namespace struct
type generatedNamespace struct {
	databaseNamespace

	opts                        namespace.Options
	ropts                       *generatedRetention
	blockSize                   time.Duration
	oldestBlock                 time.Time
	newestBlock                 time.Time
	needsFlushMarkers           []bool
	isCapturedBySnapshotMarkers []bool
}

func (ns *generatedNamespace) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("\n\tretention: %v", ns.ropts.String()))

	buf.WriteString(fmt.Sprintf("\n\tneedsFlush: \n"))
	w := new(tabwriter.Writer)
	w.Init(&buf, 5, 0, 1, ' ', 0)
	fmt.Fprintln(w, "blockStart\tneedFlush\t")
	for i := range ns.needsFlushMarkers {
		t := ns.oldestBlock.Add(time.Duration(i) * ns.blockSize)
		fmt.Fprintf(w, "%v\t%v\t\n", t, ns.needsFlushMarkers[i])
	}
	w.Flush()

	return buf.String()
}

func (ns *generatedNamespace) blockIdx(t time.Time) int {
	idx := int(t.Truncate(ns.blockSize).Sub(ns.oldestBlock) / ns.blockSize)
	if idx < 0 {
		return 0
	}
	if idx >= len(ns.needsFlushMarkers) {
		return len(ns.needsFlushMarkers) - 1
	}
	return idx
}

func (ns *generatedNamespace) Options() namespace.Options {
	return ns.opts
}

func (ns *generatedNamespace) NeedsFlush(start, end time.Time) bool {
	if start.Before(ns.oldestBlock) && end.Before(ns.oldestBlock) {
		return false
	}
	if start.After(ns.newestBlock) && end.After(ns.newestBlock) {
		return false
	}
	sIdx, eIdx := ns.blockIdx(start), ns.blockIdx(end)
	for i := sIdx; i <= eIdx; i++ {
		if ns.needsFlushMarkers[i] {
			return true
		}
	}
	return false
}

func (ns *generatedNamespace) IsCapturedBySnapshot(startInclusive, endInclusive, _ time.Time) (bool, error) {
	if startInclusive.Before(ns.oldestBlock) && endInclusive.Before(ns.oldestBlock) {
		return false, nil
	}
	if startInclusive.After(ns.newestBlock) && endInclusive.After(ns.newestBlock) {
		return false, nil
	}

	sIdx, eIdx := ns.blockIdx(startInclusive), ns.blockIdx(endInclusive)
	for i := sIdx; i <= eIdx; i++ {
		if ns.needsFlushMarkers[i] {
			return true, nil
		}
	}
	return false, nil
}

// generator for generatedNamespace
func genNamespace(t time.Time) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		var (
			rng            = genParams.Rng
			ropts          = newRandomRetention(rng)
			oldest         = retention.FlushTimeStart(ropts, t)
			newest         = retention.FlushTimeEnd(ropts, t)
			n              = numIntervals(oldest, newest, ropts.BlockSize())
			flushStates    = make([]bool, n)
			snapshotStates = make([]bool, n)
			nopts          = namespace.NewOptions().SetRetentionOptions(ropts)
		)

		for i := range flushStates {
			flushStates[i] = rng.Float32() > 0.6 // flip a coin to get a bool
			snapshotStates[i] = rng.Float32() > 0.6
		}

		ns := &generatedNamespace{
			opts:                        nopts,
			ropts:                       ropts,
			blockSize:                   ropts.BlockSize(),
			oldestBlock:                 oldest,
			newestBlock:                 newest,
			needsFlushMarkers:           flushStates,
			isCapturedBySnapshotMarkers: snapshotStates,
		}

		genResult := gopter.NewGenResult(ns, gopter.NoShrinker)
		genResult.Sieve = func(v interface{}) bool {
			ns := v.(*generatedNamespace)
			if len(ns.needsFlushMarkers) <= 0 {
				return false
			}
			return ns.ropts.Validate() == nil
		}
		return genResult
	}
}

func newRandomRetention(rng *rand.Rand) *generatedRetention {
	var (
		blockSizeMins    = maxInt(1, rng.Intn(60*12)) // 12 hours
		retentionMins    = maxInt(1, rng.Intn(40)) * blockSizeMins
		bufferPastMins   = maxInt(1, rng.Intn(blockSizeMins))
		bufferFutureMins = maxInt(1, rng.Intn(blockSizeMins))
	)

	return &generatedRetention{retention.NewOptions().
		SetRetentionPeriod(time.Duration(retentionMins) * time.Minute).
		SetBlockSize(time.Duration(blockSizeMins) * time.Minute).
		SetBufferPast(time.Duration(bufferPastMins) * time.Minute).
		SetBufferFuture(time.Duration(bufferFutureMins) * time.Minute)}
}

// generator for retention options
func genRetention() gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		opts := newRandomRetention(genParams.Rng)
		genResult := gopter.NewGenResult(opts, gopter.NoShrinker)
		genResult.Sieve = func(v interface{}) bool {
			return v.(retention.Options).Validate() == nil
		}
		return genResult
	}
}

// generator for commit log retention options
func genCommitLogRetention() gopter.Gen {
	return genRetention()
}

type generatedRetention struct {
	retention.Options
}

func (ro *generatedRetention) String() string {
	return fmt.Sprintf(
		"[ retention-period = %v, block-size = %v, buffer-past = %v, buffer-future = %v ]",
		ro.RetentionPeriod().String(),
		ro.BlockSize().String(),
		ro.BufferPast().String(),
		ro.BufferFuture().String())
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}
