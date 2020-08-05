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
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

// workerArgs contains all the information a worker go-routine needs to
// perform a migration
type workerArgs struct {
	candidate migrationCandidate
}

type worker struct {
	inputCh        chan workerArgs
	persistManager persist.Manager
}

const workerChannelSize = 256

// Migrator is an object responsible for migrating data filesets based on version information in
// the info files.
type Migrator struct {
	newMigratorFn        NewMigrationFn
	shouldMigrateFn      ShouldMigrateFn
	infoFilesByNamespace map[namespace.Metadata]fs.ShardsInfoFilesResult
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
		newMigratorFn:        opts.NewMigrationFn(),
		shouldMigrateFn:      opts.ShouldMigrateFn(),
		infoFilesByNamespace: opts.InfoFilesByNamespace(),
		migrationOpts:        opts.MigrationOptions(),
		fsOpts:               opts.FilesystemOptions(),
		instrumentOpts:       opts.InstrumentOptions(),
		storageOpts:          opts.StorageOptions(),
		log:                  opts.InstrumentOptions().Logger(),
	}, nil
}

type migrationCandidate struct {
	infoFileResult fs.ReadInfoFileResult
	metadata       namespace.Metadata
	shard          uint32
}

// Run runs the migrator
func (m *Migrator) Run() error {
	// Find candidates
	candidates := m.findMigrationCandidates()
	if len(candidates) == 0 {
		m.log.Debug("no filesets to migrate. exiting.")
		return nil
	}

	// Setup workers to perform migrations
	var (
		numWorkers = m.migrationOpts.Concurrency()
		workers    = make([]*worker, 0, numWorkers)
	)

	for i := 0; i < numWorkers; i++ {
		// Give each worker their own persist manager so that we can write files concurrently
		pm, err := fs.NewPersistManager(m.fsOpts)
		if err != nil {
			return err
		}
		worker := &worker{
			inputCh:        make(chan workerArgs, workerChannelSize),
			persistManager: pm,
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

	// Start up workers
	var wg sync.WaitGroup
	for _, worker := range workers {
		worker := worker
		wg.Add(1)
		go func() {
			m.startWorker(worker)
			wg.Done()
		}()
	}

	m.log.Info(fmt.Sprintf("found %d filesets to migrate. fileset migration start", len(candidates)))

	// Enqueue work for workers
	for i, candidate := range candidates {
		worker := workers[i%numWorkers]
		worker.inputCh <- workerArgs{
			candidate: candidate,
		}
	}

	// Close channels now that work is enqueued
	closeWorkerChannels()

	// Wait until all workers have finished
	wg.Wait()

	m.log.Info("fileset migration finished")

	return nil
}

func (m *Migrator) findMigrationCandidates() []migrationCandidate {
	var candidates []migrationCandidate
	for md, resultsByShard := range m.infoFilesByNamespace {
		for shard, results := range resultsByShard {
			for _, info := range results {
				if m.shouldMigrateFn(info) {
					candidates = append(candidates, migrationCandidate{
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
		task, err := m.newMigratorFn(migration.NewTaskOptions().
			SetInfoFileResult(input.candidate.infoFileResult).
			SetShard(input.candidate.shard).
			SetNamespaceMetadata(input.candidate.metadata).
			SetPersistManager(worker.persistManager).
			SetFilesystemOptions(m.fsOpts).
			SetStorageOptions(m.storageOpts))
		if err != nil {
			m.log.Error("error creating migration task", zap.Error(err))
		}
		if err := task.Run(); err != nil {
			m.log.Error("error running migration task", zap.Error(err))
		}

	}
}
