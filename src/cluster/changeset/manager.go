// Copyright (c) 2016 Uber Technologies, Inc.
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

package changeset

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/cluster/generated/proto/changesetpb"
	"github.com/m3db/m3/src/cluster/kv"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

var (
	// ErrAlreadyCommitted is returned when attempting to commit an already
	// committed ChangeSet
	ErrAlreadyCommitted = errors.New("change list already committed")

	// ErrCommitInProgress is returned when attempting to commit a change set
	// that is already being committed
	ErrCommitInProgress = errors.New("commit in progress")

	// ErrChangeSetClosed is returned when attempting to make a change to a
	// closed (committed / commit in progress) ChangeSet
	ErrChangeSetClosed = errors.New("change set closed")

	// ErrUnknownVersion is returned when attempting to commit a change for
	// a version that doesn't exist
	ErrUnknownVersion = errors.New("unknown version")

	errOptsNotSet       = errors.New("opts must not be nil")
	errKVNotSet         = errors.New("KV must be specified")
	errConfigKeyNotSet  = errors.New("configKey must be specified")
	errConfigTypeNotSet = errors.New("configType must be specified")
	errChangeTypeNotSet = errors.New("changesType must be specified")
)

// ManagerOptions are options used in creating a new ChangeSet Manager
type ManagerOptions interface {
	// KV is the KVStore holding the configuration
	SetKV(kv kv.Store) ManagerOptions
	KV() kv.Store

	// ConfigKey is the key holding the configuration object
	SetConfigKey(key string) ManagerOptions
	ConfigKey() string

	// Logger is the logger to use
	SetLogger(logger *zap.Logger) ManagerOptions
	Logger() *zap.Logger

	// ConfigType is a proto.Message defining the structure of the configuration
	// object.  Clones of this proto will be used to unmarshal configuration
	// instances
	SetConfigType(config proto.Message) ManagerOptions
	ConfigType() proto.Message

	// ChangesType is a proto.Message defining the structure of the changes
	// object.  Clones of this protol will be used to unmarshal change list
	// instances.
	SetChangesType(changes proto.Message) ManagerOptions
	ChangesType() proto.Message

	// Validate validates the options
	Validate() error
}

// NewManagerOptions creates an empty ManagerOptions
func NewManagerOptions() ManagerOptions { return managerOptions{} }

// A ChangeFn adds a change to an existing set of changes
type ChangeFn func(config, changes proto.Message) error

// An ApplyFn applies a set of changes to a configuration, resulting in a new
// configuration
type ApplyFn func(config, changes proto.Message) error

// A Manager manages sets of changes in a version friendly manager.  Changes to
// a given version of a configuration object are stored under
// <key>/_changes/<version>.  Multiple changes can be added, then committed all
// at once.  Committing transforms the configuration according to the changes,
// then writes the configuration back.  CAS operations are used to ensure that
// commits are not applied more than once, and to avoid conflicts on the change
// object itself.
type Manager interface {
	// Change creates a new change against the latest configuration, adding it
	// to the set of pending changes for that configuration
	Change(change ChangeFn) error

	// GetPendingChanges gets the latest uncommitted changes
	GetPendingChanges() (int, proto.Message, proto.Message, error)

	// Commit commits the specified ChangeSet, transforming the configuration on
	// which they are based into a new configuration, and storing that new
	// configuration as a next versions. Ensures that changes are applied as a
	// batch, are not applied more than once, and that new changes are not
	// started while a commit is underway
	Commit(version int, apply ApplyFn) error
}

// NewManager creates a new change list Manager
func NewManager(opts ManagerOptions) (Manager, error) {
	if opts == nil {
		return nil, errOptsNotSet
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	logger := opts.Logger()
	if logger == nil {
		logger = zap.NewNop()
	}

	return manager{
		key:         opts.ConfigKey(),
		kv:          opts.KV(),
		configType:  proto.Clone(opts.ConfigType()),
		changesType: proto.Clone(opts.ChangesType()),
		log:         logger,
	}, nil
}

type manager struct {
	key         string
	kv          kv.Store
	configType  proto.Message
	changesType proto.Message
	log         *zap.Logger
}

func (m manager) Change(change ChangeFn) error {
	for {
		// Retrieve the current configuration, creating an empty config if one does
		// not exist
		config := proto.Clone(m.configType)
		configVersion, err := m.getOrCreate(m.key, config)
		if err != nil {
			return err
		}

		// Retrieve the changes for the current configuration, creating an empty
		// change set if one does not exist
		changeset := &changesetpb.ChangeSet{
			ForVersion: int32(configVersion),
			State:      changesetpb.ChangeSetState_OPEN,
		}

		changeSetKey := fmtChangeSetKey(m.key, configVersion)
		csVersion, err := m.getOrCreate(changeSetKey, changeset)
		if err != nil {
			return err
		}

		// Only allow changes to an open change list
		if changeset.State != changesetpb.ChangeSetState_OPEN {
			return ErrChangeSetClosed
		}

		// Apply the new changes...
		changes := proto.Clone(m.changesType)
		if err := proto.Unmarshal(changeset.Changes, changes); err != nil {
			return err
		}

		if err := change(config, changes); err != nil {
			return err
		}

		changeBytes, err := proto.Marshal(changes)
		if err != nil {
			return err
		}

		// ...and update the stored changes
		changeset.Changes = changeBytes
		if _, err := m.kv.CheckAndSet(changeSetKey, csVersion, changeset); err != nil {
			if err == kv.ErrVersionMismatch {
				// Someone else updated the changes first - try again
				continue
			}

			return err
		}

		return nil
	}
}

func (m manager) GetPendingChanges() (int, proto.Message, proto.Message, error) {
	// Get the current configuration, but don't bother trying to create it if it doesn't exist
	configVal, err := m.kv.Get(m.key)
	if err != nil {
		return 0, nil, nil, err
	}

	// Retrieve the config data so that we can transform it appropriately
	config := proto.Clone(m.configType)
	if err := configVal.Unmarshal(config); err != nil {
		return 0, nil, nil, err
	}

	// Get the change set for the current configuration, again not bothering to
	// create if it doesn't exist
	changeSetKey := fmtChangeSetKey(m.key, configVal.Version())
	changeSetVal, err := m.kv.Get(changeSetKey)
	if err != nil {
		if err == kv.ErrNotFound {
			// It's ok, just means no pending changes
			return configVal.Version(), config, nil, nil
		}

		return 0, nil, nil, err
	}

	var changeset changesetpb.ChangeSet
	if err := changeSetVal.Unmarshal(&changeset); err != nil {
		return 0, nil, nil, err
	}

	// Retrieve the changes
	changes := proto.Clone(m.changesType)
	if err := proto.Unmarshal(changeset.Changes, changes); err != nil {
		return 0, nil, nil, err
	}

	return configVal.Version(), config, changes, nil
}

func (m manager) Commit(version int, apply ApplyFn) error {
	// Get the current configuration, but don't bother trying to create it if it doesn't exist
	configVal, err := m.kv.Get(m.key)
	if err != nil {
		return err
	}

	// Confirm the version does exist...
	if configVal.Version() < version {
		return ErrUnknownVersion
	}

	// ...and that it hasn't already been committed
	if configVal.Version() > version {
		return ErrAlreadyCommitted
	}

	// Retrieve the config data so that we can transform it appropriately
	config := proto.Clone(m.configType)
	if err := configVal.Unmarshal(config); err != nil {
		return err
	}

	// Get the change set for the current configuration, again not bothering to create
	// if it doesn't exist
	changeSetKey := fmtChangeSetKey(m.key, configVal.Version())
	changeSetVal, err := m.kv.Get(changeSetKey)
	if err != nil {
		return err
	}

	var changeset changesetpb.ChangeSet
	if err := changeSetVal.Unmarshal(&changeset); err != nil {
		return err
	}

	// If the change set is not already CLOSED, mark it as such to prevent new
	// changes from being recorded while the commit is underway
	if changeset.State != changesetpb.ChangeSetState_CLOSED {
		changeset.State = changesetpb.ChangeSetState_CLOSED
		if _, err := m.kv.CheckAndSet(changeSetKey, changeSetVal.Version(), &changeset); err != nil {
			if err == kv.ErrVersionMismatch {
				return ErrCommitInProgress
			}

			return err
		}
	}

	// Transform the current configuration according to the change list
	changes := proto.Clone(m.changesType)
	if err := proto.Unmarshal(changeset.Changes, changes); err != nil {
		return err
	}

	if err := apply(config, changes); err != nil {
		return err
	}

	// Save the updated config.  This updates the version number for the config, so
	// attempting to commit the current version again will fail
	if _, err := m.kv.CheckAndSet(m.key, configVal.Version(), config); err != nil {
		if err == kv.ErrVersionMismatch {
			return ErrAlreadyCommitted
		}

		return err
	}

	return nil
}

func (m manager) getOrCreate(k string, v proto.Message) (int, error) {
	for {
		val, err := m.kv.Get(k)
		if err == kv.ErrNotFound {
			// Attempt to create
			version, err := m.kv.SetIfNotExists(k, v)
			if err == nil {
				return version, nil
			}

			if err == kv.ErrAlreadyExists {
				// Someone got there first...try again
				continue
			}

			return 0, err
		}

		if err != nil {
			// Some other error occurred
			return 0, err
		}

		// Unmarshall the current value
		if err := val.Unmarshal(v); err != nil {
			return 0, err
		}

		return val.Version(), nil
	}
}

func fmtChangeSetKey(configKey string, configVers int) string {
	return fmt.Sprintf("%s/_changes/%d", configKey, configVers)
}

type managerOptions struct {
	kv          kv.Store
	logger      *zap.Logger
	configKey   string
	configType  proto.Message
	changesType proto.Message
}

func (opts managerOptions) KV() kv.Store               { return opts.kv }
func (opts managerOptions) Logger() *zap.Logger        { return opts.logger }
func (opts managerOptions) ConfigKey() string          { return opts.configKey }
func (opts managerOptions) ConfigType() proto.Message  { return opts.configType }
func (opts managerOptions) ChangesType() proto.Message { return opts.changesType }

func (opts managerOptions) SetKV(kv kv.Store) ManagerOptions {
	opts.kv = kv
	return opts
}
func (opts managerOptions) SetLogger(logger *zap.Logger) ManagerOptions {
	opts.logger = logger
	return opts
}
func (opts managerOptions) SetConfigKey(k string) ManagerOptions {
	opts.configKey = k
	return opts
}
func (opts managerOptions) SetConfigType(ct proto.Message) ManagerOptions {
	opts.configType = ct
	return opts
}
func (opts managerOptions) SetChangesType(ct proto.Message) ManagerOptions {
	opts.changesType = ct
	return opts
}

func (opts managerOptions) Validate() error {
	if opts.ConfigKey() == "" {
		return errConfigKeyNotSet
	}

	if opts.KV() == nil {
		return errKVNotSet
	}

	if opts.ConfigType() == nil {
		return errConfigTypeNotSet
	}

	if opts.ChangesType() == nil {
		return errChangeTypeNotSet
	}

	return nil
}
