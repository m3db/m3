// Copyright (c) 2020 Uber Technologies, Inc.
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

package migrator

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const workerChannelSize = 256

type worker struct {
	inputCh        chan migrationCandidate
	outputCh       chan completedMigration
	persistManager persist.Manager
	taskOptions    migration.TaskOptions
}

// Migrator is an object responsible for migrating data filesets based on version information in
// the info files.
type Migrator struct {
	migrationTaskFn      MigrationTaskFn
	infoFilesByNamespace fs.InfoFilesByNamespace
	migrationOpts        migration.Options
	fsOpts               fs.Options
	instrumentOpts       instrument.Options
	storageOpts          storage.Options
	log                  *zap.Logger
}

// NewMigrator creates a new Migrator.
func NewMigrator(opts Options) (Migrator, error) {
	if err := opts.Validate(); err != nil {
		return Migrator{}, err
	}
	return Migrator{
		migrationTaskFn:      opts.MigrationTaskFn(),
		infoFilesByNamespace: opts.InfoFilesByNamespace(),
		migrationOpts:        opts.MigrationOptions(),
		fsOpts:               opts.FilesystemOptions(),
		instrumentOpts:       opts.InstrumentOptions(),
		storageOpts:          opts.StorageOptions(),
		log:                  opts.InstrumentOptions().Logger(),
	}, nil
}

// migrationCandidate is the struct we generate when we find a fileset in need of
// migration. It's provided to the workers to perform the actual migration.
type migrationCandidate struct {
	newTaskFn      migration.NewTaskFn
	infoFileResult fs.ReadInfoFileResult
	metadata       namespace.Metadata
	shard          uint32
}

// mergeKey is the unique set of data that identifies an ReadInfoFileResult.
type mergeKey struct {
	metadata   namespace.Metadata
	shard      uint32
	blockStart int64
}

// completedMigration is the updated ReadInfoFileSet after a migration has been performed
// plus the merge key, so that we can properly merge the updated result back into
// infoFilesByNamespace map.
type completedMigration struct {
	key                   mergeKey
	updatedInfoFileResult fs.ReadInfoFileResult
}

// Run runs the migrator
func (m *Migrator) Run(ctx context.Context) error {
	ctx, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperFilesystemSourceMigrator)
	defer span.Finish()

	// Find candidates
	candidates := m.findMigrationCandidates()
	if len(candidates) == 0 {
		m.log.Debug("no filesets to migrate. exiting.")
		return nil
	}

	m.log.Info(fmt.Sprintf("found %d filesets to migrate. fileset migration start", len(candidates)))

	nowFn := m.fsOpts.ClockOptions().NowFn()
	begin := nowFn()

	// Setup workers to perform migrations
	var (
		numWorkers = m.migrationOpts.Concurrency()
		workers    = make([]*worker, 0, numWorkers)
		outputCh   = make(chan completedMigration, len(candidates))
	)

	for i := 0; i < numWorkers; i++ {
		// Give each worker their own persist manager so that we can write files concurrently.
		pm, err := fs.NewPersistManager(m.fsOpts)
		if err != nil {
			return err
		}
		worker := &worker{
			inputCh:        make(chan migrationCandidate, workerChannelSize),
			outputCh:       outputCh,
			persistManager: pm,
			taskOptions: migration.NewTaskOptions().
				SetFilesystemOptions(m.fsOpts).
				SetStorageOptions(m.storageOpts),
		}
		workers = append(workers, worker)
	}
	closedWorkerChannels := false
	closeWorkerChannels := func() {
		if closedWorkerChannels {
			return
		}
		closedWorkerChannels = true
		for _, worker := range workers {
			close(worker.inputCh)
		}
	}
	// NB(nate): Ensure that channels always get closed.
	defer closeWorkerChannels()

	// Start up workers. Intentionally not using sync.WaitGroup so we can know when the last worker
	// is finishing so that we can close the output channel.
	activeWorkers := atomic.NewUint32(0)
	for _, worker := range workers {
		worker := worker
		activeWorkers.Inc()
		go func() {
			m.startWorker(worker)
			if activeWorkers.Dec() == 0 {
				close(worker.outputCh)
			}
		}()
	}

	// Start up goroutine consuming worker output
	var (
		migrationResults = make(map[mergeKey]fs.ReadInfoFileResult, len(candidates))
		resultsConsumed  = make(chan bool, 1)
	)
	go func() {
		for result := range outputCh {
			migrationResults[result.key] = result.updatedInfoFileResult
		}
		close(resultsConsumed)
	}()

	// Enqueue work for workers
	for i, candidate := range candidates {
		worker := workers[i%numWorkers]
		worker.inputCh <- candidate
	}

	// Close channels now that work is enqueued
	closeWorkerChannels()

	// Wait until all workers have finished and migration results have been consumed
	<-resultsConsumed

	m.mergeUpdatedInfoFiles(migrationResults)

	m.log.Info("fileset migration finished", zap.Duration("took", nowFn().Sub(begin)))

	return nil
}

func (m *Migrator) findMigrationCandidates() []migrationCandidate {
	var candidates []migrationCandidate
	for md, resultsByShard := range m.infoFilesByNamespace {
		for shard, results := range resultsByShard {
			for _, info := range results {
				newTaskFn, shouldMigrate := m.migrationTaskFn(info)
				if shouldMigrate {
					candidates = append(candidates, migrationCandidate{
						newTaskFn:      newTaskFn,
						metadata:       md,
						shard:          shard,
						infoFileResult: info,
					})
				}
			}
		}
	}

	return candidates
}

func (m *Migrator) startWorker(worker *worker) {
	for input := range worker.inputCh {
		task, err := input.newTaskFn(worker.taskOptions.
			SetInfoFileResult(input.infoFileResult).
			SetShard(input.shard).
			SetNamespaceMetadata(input.metadata).
			SetPersistManager(worker.persistManager))
		if err != nil {
			m.log.Error("error creating migration task", zap.Error(err))
		}
		infoFileResult, err := task.Run()
		if err != nil {
			m.log.Error("error running migration task", zap.Error(err))
		}
		worker.outputCh <- completedMigration{
			key: mergeKey{
				metadata:   input.metadata,
				shard:      input.shard,
				blockStart: input.infoFileResult.Info.BlockStart,
			},
			updatedInfoFileResult: infoFileResult,
		}
	}
}

// mergeUpdatedInfoFiles takes all ReadInfoFileResults updated by a migration and merges them back
// into the infoFilesByNamespace map. This prevents callers from having to re-read info files to get
// updated in-memory structures.
func (m *Migrator) mergeUpdatedInfoFiles(migrationResults map[mergeKey]fs.ReadInfoFileResult) {
	for md, resultsByShard := range m.infoFilesByNamespace {
		for shard, results := range resultsByShard {
			for i, info := range results {
				if val, ok := migrationResults[mergeKey{
					metadata:   md,
					shard:      shard,
					blockStart: info.Info.BlockStart,
				}]; ok {
					results[i] = val
				}
			}
		}
	}
}
