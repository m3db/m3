package migrator

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errNewMigratorFnNotSet        = errors.New("newMigratorFn not set")
	errShouldMigrateFnNotSet      = errors.New("shouldMigrateFn not set")
	errInfoFilesByNamespaceNotSet = errors.New("infoFilesByNamespaces not set")
	errMigrationOptsNotSet        = errors.New("migrationOpts not set")
	errInstrumentOptsNotSet       = errors.New("instrumentOpts not set")
	errStorageOptsNotSet          = errors.New("storageOpts not set")
	errFilesystemOptsNotSet       = errors.New("filesystemOpts not set")
)

type options struct {
	newMigratorFn        NewMigrationFn
	shouldMigrateFn      ShouldMigrateFn
	infoFilesByNamespace map[namespace.Metadata]fs.ShardsInfoFilesResult
	migrationOpts        migration.Options
	fsOpts               fs.Options
	instrumentOpts       instrument.Options
	storageOpts          storage.Options
}

// NewOptions return new migration opts
func NewOptions() Options {
	return &options{
		migrationOpts:  migration.NewOptions(),
		fsOpts:         fs.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
		storageOpts:    storage.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.storageOpts == nil {
		return errStorageOptsNotSet
	}
	if err := o.storageOpts.Validate(); err != nil {
		return err
	}
	if o.newMigratorFn == nil {
		return errNewMigratorFnNotSet
	}
	if o.shouldMigrateFn == nil {
		return errShouldMigrateFnNotSet
	}
	if o.infoFilesByNamespace == nil {
		return errInfoFilesByNamespaceNotSet
	}
	if o.migrationOpts == nil {
		return errMigrationOptsNotSet
	}
	if o.instrumentOpts == nil {
		return errInstrumentOptsNotSet
	}
	if o.fsOpts == nil {
		return errFilesystemOptsNotSet
	}
	if err := o.fsOpts.Validate(); err != nil {
		return err
	}
	return nil
}

func (o *options) SetNewMigrationFn(value NewMigrationFn) Options {
	opts := *o
	opts.newMigratorFn = value
	return &opts
}

func (o *options) NewMigrationFn() NewMigrationFn {
	return o.newMigratorFn
}

func (o *options) SetShouldMigrateFn(value ShouldMigrateFn) Options {
	opts := *o
	opts.shouldMigrateFn = value
	return &opts
}

func (o *options) ShouldMigrateFn() ShouldMigrateFn {
	return o.shouldMigrateFn
}

func (o *options) SetInfoFilesByNamespace(value map[namespace.Metadata]fs.ShardsInfoFilesResult) Options {
	opts := *o
	opts.infoFilesByNamespace = value
	return &opts
}

func (o *options) InfoFilesByNamespace() map[namespace.Metadata]fs.ShardsInfoFilesResult {
	return o.infoFilesByNamespace
}

func (o *options) SetMigrationOptions(value migration.Options) Options {
	opts := *o
	opts.migrationOpts = value
	return &opts
}

func (o *options) MigrationOptions() migration.Options {
	return o.migrationOpts
}

func (o *options) SetFilesystemOptions(value fs.Options) Options {
	opts := *o
	opts.fsOpts = value
	return &opts
}

func (o *options) FilesystemOptions() fs.Options {
	return o.fsOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetStorageOptions(value storage.Options) Options {
	opts := *o
	opts.storageOpts = value
	return &opts
}

func (o *options) StorageOptions() storage.Options {
	return o.storageOpts
}
