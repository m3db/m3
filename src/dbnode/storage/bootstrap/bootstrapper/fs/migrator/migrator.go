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
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

type worker struct {
	persistManager persist.Manager
	taskOptions    migration.TaskOptions
}

// Migrator is responsible for migrating data filesets based on version information in
// the info files.
type Migrator struct {
	migrationTaskFn      MigrationTaskFn
	infoFilesByNamespace bootstrap.InfoFilesByNamespace
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

// Run runs the migrator.
func (m *Migrator) Run(ctx context.Context) error {
	ctx, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperFilesystemSourceMigrator)
	defer span.Finish()

	// Find candidates
	candidates := m.findMigrationCandidates()
	if len(candidates) == 0 {
		m.log.Debug("no filesets to migrate. exiting.")
		return nil
	}

	m.log.Info("starting fileset migration", zap.Int("migrations", len(candidates)))

	nowFn := m.fsOpts.ClockOptions().NowFn()
	begin := nowFn()

	// Setup workers to perform migrations
	var (
		numWorkers = m.migrationOpts.Concurrency()
		workers    = make([]*worker, 0, numWorkers)
	)

	baseOpts := migration.NewTaskOptions().
		SetFilesystemOptions(m.fsOpts).
		SetStorageOptions(m.storageOpts)
	for i := 0; i < numWorkers; i++ {
		// Give each worker their own persist manager so that we can write files concurrently.
		pm, err := fs.NewPersistManager(m.fsOpts)
		if err != nil {
			return err
		}
		worker := &worker{
			persistManager: pm,
			taskOptions:    baseOpts,
		}
		workers = append(workers, worker)
	}

	// Start up workers.
	var (
		wg                  sync.WaitGroup
		candidatesPerWorker = len(candidates) / numWorkers
		candidateIdx        = 0

		completedMigrationsLock sync.Mutex
		completedMigrations     = make([]completedMigration, 0, len(candidates))
	)
	for i, worker := range workers {
		endIdx := candidateIdx + candidatesPerWorker
		if i == len(workers)-1 {
			endIdx = len(candidates)
		}

		worker := worker
		startIdx := candidateIdx // Capture current candidateIdx value for goroutine
		wg.Add(1)
		go func() {
			output := m.startWorker(worker, candidates[startIdx:endIdx])

			completedMigrationsLock.Lock()
			completedMigrations = append(completedMigrations, output...)
			completedMigrationsLock.Unlock()

			wg.Done()
		}()

		candidateIdx = endIdx
	}

	// Wait until all workers have finished and completedMigrations has been updated
	wg.Wait()

	migrationResults := make(map[mergeKey]fs.ReadInfoFileResult, len(candidates))
	for _, result := range completedMigrations {
		migrationResults[result.key] = result.updatedInfoFileResult
	}

	m.mergeUpdatedInfoFiles(migrationResults)

	m.log.Info("fileset migration finished", zap.Duration("took", nowFn().Sub(begin)))

	return nil
}

func (m *Migrator) findMigrationCandidates() []migrationCandidate {
	maxCapacity := 0
	for _, resultsByShard := range m.infoFilesByNamespace {
		for _, results := range resultsByShard {
			maxCapacity += len(results)
		}
	}

	candidates := make([]migrationCandidate, 0, maxCapacity)
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

func (m *Migrator) startWorker(worker *worker, candidates []migrationCandidate) []completedMigration {
	output := make([]completedMigration, 0, len(candidates))
	for _, candidate := range candidates {
		task, err := candidate.newTaskFn(worker.taskOptions.
			SetInfoFileResult(candidate.infoFileResult).
			SetShard(candidate.shard).
			SetNamespaceMetadata(candidate.metadata).
			SetPersistManager(worker.persistManager))
		if err != nil {
			m.log.Error("error creating migration task", zap.Error(err))
		}
		// NB(nate): Handling of errors should be re-evaluated as migrations are added. Current migrations
		// do not mutate state in such a way that data can be left in an invalid state in the case of failures. Additionally,
		// we want to ensure that the bootstrap process is always able to continue. If either of these conditions change,
		// error handling at this level AND the migrator level should be reconsidered.
		infoFileResult, err := task.Run()
		if err != nil {
			m.log.Error("error running migration task", zap.Error(err))
		} else {
			output = append(output, completedMigration{
				key: mergeKey{
					metadata:   candidate.metadata,
					shard:      candidate.shard,
					blockStart: candidate.infoFileResult.Info.BlockStart,
				},
				updatedInfoFileResult: infoFileResult,
			})
		}
	}

	return output
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
