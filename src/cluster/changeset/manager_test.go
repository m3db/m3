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
	"strings"
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/changesetpb"
	"github.com/m3db/m3/src/cluster/generated/proto/changesettest"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	errBadThingsHappened = errors.New("bad things happened")
)

func TestManager_ChangeEmptyInitialConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		config1  = new(configMatcher)
		changes1 = new(changeSetMatcher)
		changes2 = new(changeSetMatcher)
	)

	gomock.InOrder(
		// Get initial config - see no value and create
		s.kv.EXPECT().Get("config").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config", config1).Return(1, nil),

		// Get initial changes - see no value and create
		s.kv.EXPECT().Get("config/_changes/1").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config/_changes/1", changes1).Return(1, nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/1", 1, changes2).Return(2, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	require.Equal(t, "", config1.config().Text)

	require.Equal(t, int32(1), changes1.changeset().ForVersion)
	require.Equal(t, changesetpb.ChangeSetState_OPEN, changes1.changeset().State)
	require.Nil(t, changes1.changeset().Changes)

	require.Equal(t, int32(1), changes2.changeset().ForVersion)
	require.Equal(t, changesetpb.ChangeSetState_OPEN, changes2.changeset().State)
	require.NotNil(t, changes2.changeset().Changes)
	require.Equal(t, []string{"foo", "bar"}, changes2.changes(t).Lines)
}

func TestManager_ChangeInterruptOnCreateOfInitialConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		configVal  = mem.NewValue(2, &changesettest.Config{})
		changesVal = mem.NewValue(12, s.newOpenChangeSet(2, &changesettest.Changes{}))
	)

	gomock.InOrder(
		// Initial attempt to create config - someone else gets there first
		s.kv.EXPECT().Get("config").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config", gomock.Any()).Return(0, kv.ErrAlreadyExists),

		// Will refetch
		s.kv.EXPECT().Get("config").Return(configVal, nil),

		// Fetch corresponding changes
		s.kv.EXPECT().Get("config/_changes/2").Return(changesVal, nil),

		// ...And update
		s.kv.EXPECT().CheckAndSet("config/_changes/2", 12, gomock.Any()).Return(13, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeInterruptOnCreateOfInitialChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changesVal = mem.NewValue(12, s.newOpenChangeSet(13, &changesettest.Changes{}))
	)

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(13, &changesettest.Config{}), nil),

		// Initial attempt to create changes - someone else gets there first
		s.kv.EXPECT().Get("config/_changes/13").Return(nil, kv.ErrNotFound),
		s.kv.EXPECT().SetIfNotExists("config/_changes/13", gomock.Any()).Return(0, kv.ErrAlreadyExists),

		// Will refetch
		s.kv.EXPECT().Get("config/_changes/13").Return(changesVal, nil),

		// ...And update
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, gomock.Any()).Return(13, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorRetrievingConfig(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	// Initial attempt to get changes fails
	s.kv.EXPECT().Get("config").Return(nil, errBadThingsHappened)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorRetrievingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(13, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/13").Return(nil, errBadThingsHappened),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)

	// NB(mmihic): We only care that the expectations are met
}

func TestManager_ChangeErrorUnmarshallingInitialChange(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(13, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/13").Return(mem.NewValue(12, &changesetpb.ChangeSet{
			ForVersion: 13,
			State:      changesetpb.ChangeSetState_OPEN,
			Changes:    []byte("foo"), // Not a valid proto
		}), nil),
	)

	require.Error(t, s.mgr.Change(addLines("foo", "bar")))
}

func TestManager_ChangeErrorUpdatingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		updatedChanges = new(changeSetMatcher)
	)

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(13, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/13").Return(mem.NewValue(12,
			s.newOpenChangeSet(13, &changesettest.Changes{})), nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, updatedChanges).
			Return(0, errBadThingsHappened),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, errBadThingsHappened, err)
}

func TestManager_ChangeVersionMismatchUpdatingChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changes1 = new(changeSetMatcher)
		changes2 = new(changeSetMatcher)
	)

	gomock.InOrder(
		// Version mismatch while updating changeset
		s.kv.EXPECT().Get("config").Return(mem.NewValue(13, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/13").Return(mem.NewValue(12,
			s.newOpenChangeSet(13, &changesettest.Changes{})), nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/13", 12, changes1).
			Return(0, kv.ErrVersionMismatch),

		// Will try again
		s.kv.EXPECT().Get("config").Return(mem.NewValue(14, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/14").Return(mem.NewValue(22,
			s.newOpenChangeSet(14, &changesettest.Changes{
				Lines: []string{"zed"},
			})), nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/14", 22, changes2).Return(23, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))
	require.Equal(t, []string{"zed", "foo", "bar"}, changes2.changes(t).Lines)
}

func TestManager_ChangeSuccess(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	updatedChanges := new(changeSetMatcher)
	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(72, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/72").Return(mem.NewValue(29,
			s.newOpenChangeSet(72, &changesettest.Changes{
				Lines: []string{"ark", "bork"},
			})), nil),
		s.kv.EXPECT().CheckAndSet("config/_changes/72", 29, updatedChanges).Return(23, nil),
	)

	require.NoError(t, s.mgr.Change(addLines("foo", "bar")))
	require.Equal(t, updatedChanges.changes(t).Lines, []string{"ark", "bork", "foo", "bar"})
}

func TestManager_ChangeOnClosedChangeSet(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(72, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/72").Return(mem.NewValue(29,
			s.newChangeSet(72, changesetpb.ChangeSetState_CLOSED,
				&changesettest.Changes{
					Lines: []string{"ark", "bork"},
				})), nil),
	)

	err := s.mgr.Change(addLines("foo", "bar"))
	require.Equal(t, ErrChangeSetClosed, err)
}

func TestManager_ChangeFunctionFails(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.kv.EXPECT().Get("config").Return(mem.NewValue(72, &changesettest.Config{}), nil),
		s.kv.EXPECT().Get("config/_changes/72").Return(mem.NewValue(29,
			s.newOpenChangeSet(72, &changesettest.Changes{
				Lines: []string{"ark", "bork"},
			})), nil),
	)

	err := s.mgr.Change(func(cfg, changes proto.Message) error {
		return errBadThingsHappened
	})
	require.Equal(t, errBadThingsHappened, err)
}

func TestManagerCommit_Success(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		changeSet1 = new(changeSetMatcher)
		config1    = new(configMatcher)

		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newOpenChangeSet(changeSetVersion, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as committing
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, changeSet1).
			Return(changeSetVersion+1, nil),

		// Update the transformed confi
		s.kv.EXPECT().CheckAndSet(s.configKey, committedVersion, config1).
			Return(committedVersion+1, nil),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.NoError(t, err)

	require.Equal(t, changesetpb.ChangeSetState_CLOSED, changeSet1.changeset().State)
	require.Equal(t, "shoop\nwoop\nhoop\nfoo\nbar", config1.config().Text)
}

func TestManagerCommit_ConfigNotFound(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
	)

	// KV service can't find config
	s.kv.EXPECT().Get(s.configKey).Return(nil, kv.ErrNotFound)

	// Commit should fail
	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, kv.ErrNotFound, err)
}

func TestManagerCommit_ConfigGetError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
	)

	// KV service has error retrieving config
	s.kv.EXPECT().Get(s.configKey).Return(nil, errBadThingsHappened)

	// Commit should fail
	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, errBadThingsHappened, err)
}

func TestManagerCommit_ConfigAtEarlierVersion(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
	)

	// KV service returns earlier version
	s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion-1,
		&changesettest.Config{
			Text: "shoop\nwoop\nhoop",
		}), nil)

	// Commit should fail
	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, ErrUnknownVersion, err)
}

func TestManagerCommit_ConfigAtLaterVersion(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
	)

	// KV service returns later version
	s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion+1,
		&changesettest.Config{
			Text: "shoop\nwoop\nhoop",
		}), nil)

	// Commit should fail
	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, ErrAlreadyCommitted, err)
}

func TestManagerCommit_ConfigUnmarshalError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
	)

	// KV service returns invalid data
	s.kv.EXPECT().Get(s.configKey).Return(mem.NewValueWithData(committedVersion+1, []byte("foo")), nil)

	// Commit should fail
	err := s.mgr.Commit(committedVersion, commit)
	require.Error(t, err)
}

func TestManagerCommit_ChangeSetClosing(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		config1 = new(configMatcher)

		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_CLOSED, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// NB(mmihic): Don't re-update the change set

		// Update the transformed config
		s.kv.EXPECT().CheckAndSet(s.configKey, committedVersion, config1).
			Return(committedVersion+1, nil),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.NoError(t, err)
}

func TestManagerCommit_ChangeSetVersionMismatchMarkingAsClosed(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_OPEN, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as closed - fail with version mismatch
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, gomock.Any()).
			Return(0, kv.ErrVersionMismatch),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, ErrCommitInProgress, err)
}

func TestManagerCommit_ChangeSetErrorMarkingAsClosed(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_OPEN, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as committing - fail with version mismatch
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, gomock.Any()).
			Return(0, errBadThingsHappened),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, errBadThingsHappened, err)
}

func TestManagerCommit_CommitFunctionError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_OPEN, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as closed
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, gomock.Any()).
			Return(changeSetVersion+1, nil),
	)

	err := s.mgr.Commit(committedVersion, func(cfg, changes proto.Message) error {
		return errBadThingsHappened
	})
	require.Equal(t, errBadThingsHappened, err)
}

func TestManagerCommit_ConfigUpdateVersionMismatch(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_OPEN, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as closed
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, gomock.Any()).
			Return(changeSetVersion+1, nil),

		// Update with new config - FAIL
		s.kv.EXPECT().CheckAndSet(s.configKey, committedVersion, gomock.Any()).
			Return(0, kv.ErrVersionMismatch),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, ErrAlreadyCommitted, err)
}

func TestManagerCommit_ConfigUpdateError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	var (
		committedVersion = 22
		changeSetVersion = 17
		changeSetKey     = fmtChangeSetKey(s.configKey, committedVersion)
	)

	gomock.InOrder(
		// Retrieve the config value
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(committedVersion,
			&changesettest.Config{
				Text: "shoop\nwoop\nhoop",
			}), nil),

		// Retrieve the change set
		s.kv.EXPECT().Get(changeSetKey).Return(mem.NewValue(changeSetVersion,
			s.newChangeSet(committedVersion, changesetpb.ChangeSetState_OPEN, &changesettest.Changes{
				Lines: []string{"foo", "bar"},
			})), nil),

		// Mark as closed
		s.kv.EXPECT().CheckAndSet(changeSetKey, changeSetVersion, gomock.Any()).
			Return(changeSetVersion+1, nil),

		// Update with new config - FAIL
		s.kv.EXPECT().CheckAndSet(s.configKey, committedVersion, gomock.Any()).
			Return(0, errBadThingsHappened),
	)

	err := s.mgr.Commit(committedVersion, commit)
	require.Equal(t, errBadThingsHappened, err)
}

func TestManager_GetPendingChangesSuccess(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	config := &changesettest.Config{
		Text: "foo\nbar\n",
	}
	changes := &changesettest.Changes{
		Lines: []string{"zed", "brack"},
	}

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(13, config), nil),
		s.kv.EXPECT().Get(fmtChangeSetKey(s.configKey, 13)).Return(mem.NewValue(24, &changesetpb.ChangeSet{
			ForVersion: 13,
			State:      changesetpb.ChangeSetState_OPEN,
			Changes:    s.marshal(changes),
		}), nil),
	)

	vers, returnedConfig, returnedChanges, err := s.mgr.GetPendingChanges()
	require.NoError(t, err)
	require.Equal(t, 13, vers)
	require.Equal(t, *config, *(returnedConfig.(*changesettest.Config)))
	require.Equal(t, *changes, *(returnedChanges.(*changesettest.Changes)))
}

func TestManager_GetPendingChangesGetConfigError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(nil, errBadThingsHappened),
	)

	vers, returnedConfig, returnedChanges, err := s.mgr.GetPendingChanges()
	require.Equal(t, errBadThingsHappened, err)
	require.Equal(t, 0, vers)
	require.Nil(t, returnedConfig)
	require.Nil(t, returnedChanges)
}

func TestManager_GetPendingChangesConfigUnmarshalError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	s.kv.EXPECT().Get(s.configKey).Return(mem.NewValueWithData(13, []byte("foo")), nil)

	_, _, _, err := s.mgr.GetPendingChanges()
	require.Error(t, err)
}

func TestManager_GetPendingChangesGetChangeSetError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	config := &changesettest.Config{
		Text: "foo\nbar\n",
	}

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(13, config), nil),
		s.kv.EXPECT().Get(fmtChangeSetKey(s.configKey, 13)).Return(nil, errBadThingsHappened),
	)

	vers, returnedConfig, returnedChanges, err := s.mgr.GetPendingChanges()
	require.Equal(t, errBadThingsHappened, err)
	require.Equal(t, 0, vers)
	require.Nil(t, returnedConfig)
	require.Nil(t, returnedChanges)
}

func TestManager_GetPendingChangesChangeSetNotFound(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	config := &changesettest.Config{
		Text: "foo\nbar\n",
	}

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(13, config), nil),
		s.kv.EXPECT().Get(fmtChangeSetKey(s.configKey, 13)).Return(nil, kv.ErrNotFound),
	)

	vers, returnedConfig, returnedChanges, err := s.mgr.GetPendingChanges()
	require.NoError(t, err)
	require.Equal(t, 13, vers)
	require.Equal(t, *config, *(returnedConfig.(*changesettest.Config)))
	require.Nil(t, returnedChanges)
}

func TestManager_GetPendingChangesChangeSetUnmarshalError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	config := &changesettest.Config{
		Text: "foo\nbar\n",
	}

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(13, config), nil),
		s.kv.EXPECT().Get(fmtChangeSetKey(s.configKey, 13)).
			Return(mem.NewValueWithData(24, []byte("foo")), nil),
	)

	_, _, _, err := s.mgr.GetPendingChanges()
	require.Error(t, err)
}

func TestManager_GetPendingChangesChangeUnmarshalError(t *testing.T) {
	s := newTestSuite(t)
	defer s.finish()

	config := &changesettest.Config{
		Text: "foo\nbar\n",
	}

	gomock.InOrder(
		s.kv.EXPECT().Get(s.configKey).Return(mem.NewValue(13, config), nil),
		s.kv.EXPECT().Get(fmtChangeSetKey(s.configKey, 13)).Return(mem.NewValue(24, &changesetpb.ChangeSet{
			ForVersion: 13,
			State:      changesetpb.ChangeSetState_OPEN,
			Changes:    []byte("foo"), // invalid proto buf
		}), nil),
	)

	_, _, _, err := s.mgr.GetPendingChanges()
	require.Error(t, err)
}

func TestManagerOptions_Validate(t *testing.T) {
	tests := []struct {
		err  error
		opts ManagerOptions
	}{
		{errConfigKeyNotSet, NewManagerOptions().
			SetConfigType(&changesettest.Config{}).
			SetChangesType(&changesettest.Changes{}).
			SetKV(mem.NewStore())},

		{errConfigTypeNotSet, NewManagerOptions().
			SetConfigKey("foozle").
			SetChangesType(&changesettest.Changes{}).
			SetKV(mem.NewStore())},

		{errChangeTypeNotSet, NewManagerOptions().
			SetConfigKey("bazzle").
			SetConfigType(&changesettest.Config{}).
			SetKV(mem.NewStore())},

		{errKVNotSet, NewManagerOptions().
			SetConfigKey("muzzle").
			SetConfigType(&changesettest.Config{}).
			SetChangesType(&changesettest.Changes{})},
	}

	for _, test := range tests {
		require.Equal(t, test.err, test.opts.Validate())
	}
}

type configMatcher struct {
	CapturingProtoMatcher
}

func (m *configMatcher) config() *changesettest.Config {
	return m.Arg.(*changesettest.Config)
}

type changeSetMatcher struct {
	CapturingProtoMatcher
}

func (m *changeSetMatcher) changeset() *changesetpb.ChangeSet {
	return m.Arg.(*changesetpb.ChangeSet)
}

func (m *changeSetMatcher) changes(t *testing.T) *changesettest.Changes {
	changes := new(changesettest.Changes)
	require.NoError(t, proto.Unmarshal(m.changeset().Changes, changes))
	return changes
}

func addLines(lines ...string) ChangeFn {
	return func(cfgProto, changesProto proto.Message) error {
		changes := changesProto.(*changesettest.Changes)
		changes.Lines = append(changes.Lines, lines...)
		return nil
	}
}

func commit(cfgProto, changesProto proto.Message) error {
	changes := changesProto.(*changesettest.Changes)
	config := cfgProto.(*changesettest.Config)
	if config.Text != "" {
		config.Text = config.Text + "\n"
	}
	config.Text = config.Text + strings.Join(changes.Lines, "\n")
	return nil
}

type testSuite struct {
	t         *testing.T
	kv        *kv.MockStore
	mc        *gomock.Controller
	mgr       Manager
	configKey string
}

func newTestSuite(t *testing.T) *testSuite {
	mc := gomock.NewController(t)
	kvStore := kv.NewMockStore(mc)
	configKey := "config"
	mgr, err := NewManager(NewManagerOptions().
		SetKV(kvStore).
		SetConfigType(&changesettest.Config{}).
		SetChangesType(&changesettest.Changes{}).
		SetConfigKey(configKey))

	require.NoError(t, err)

	return &testSuite{
		t:         t,
		mc:        mc,
		kv:        kvStore,
		configKey: configKey,
		mgr:       mgr,
	}
}

func (t *testSuite) finish() {
	t.mc.Finish()
}

func (t *testSuite) marshal(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	require.NoError(t.t, err)
	return b
}

func (t *testSuite) newOpenChangeSet(forVersion int, changes proto.Message) *changesetpb.ChangeSet {
	return t.newChangeSet(forVersion, changesetpb.ChangeSetState_OPEN, changes)
}

func (t *testSuite) newChangeSet(forVersion int, state changesetpb.ChangeSetState, changes proto.Message,
) *changesetpb.ChangeSet {
	changeSet := &changesetpb.ChangeSet{
		ForVersion: int32(forVersion),
		State:      state,
	}

	cbytes, err := proto.Marshal(changes)
	require.NoError(t.t, err)
	changeSet.Changes = cbytes
	return changeSet
}

type CapturingProtoMatcher struct {
	Arg proto.Message
}

func (m *CapturingProtoMatcher) Matches(arg interface{}) bool {
	msg, ok := arg.(proto.Message)
	if !ok {
		return false
	}

	m.Arg = proto.Clone(msg)
	return true
}

func (m *CapturingProtoMatcher) String() string {
	return "proto-capture"
}
