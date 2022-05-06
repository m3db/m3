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

package integration

import (
	"testing"

	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const (
	maxProducers = 2
	maxRF        = 3
)

func TestSharedConsumer(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestReplicatedConsumer(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestSharedAndReplicatedConsumers(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		for j := 1; j <= maxRF; j++ {
			s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
				{ct: topic.Shared, isSharded: true, instances: 5, replicas: j},
				{ct: topic.Shared, isSharded: false, instances: 5, replicas: j},
				{ct: topic.Replicated, isSharded: true, instances: 5, replicas: j},
			})

			s.Run(t, ctrl)
			s.VerifyConsumers(t)
		}
	}
}

func TestSharedConsumerWithDeadInstance(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.KillInstance(t, 0) },
		)

		s.ScheduleOperations(
			20,
			func() { s.KillInstance(t, 1) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
		testConsumers := s.consumerServices[0].testConsumers
		require.True(t, testConsumers[len(testConsumers)-1].numConsumed() <= s.TotalMessages()*10/100)
		testConsumers = s.consumerServices[1].testConsumers
		require.True(t, testConsumers[len(testConsumers)-1].numConsumed() <= s.TotalMessages()*20/100)
	}
}

func TestSharedConsumerWithDeadConnection(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.KillConnection(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.KillConnection(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestReplicatedConsumerWithDeadConnection(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.KillConnection(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.KillConnection(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestSharedAndReplicatedConsumerWithDeadConnection(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		for j := 1; j <= maxRF; j++ {
			s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
				{ct: topic.Shared, isSharded: true, instances: 5, replicas: j},
				{ct: topic.Shared, isSharded: false, instances: 5, replicas: j},
				{ct: topic.Replicated, isSharded: true, instances: 5, replicas: j},
			})

			s.ScheduleOperations(
				10,
				func() { s.KillConnection(t, 0) },
			)
			s.ScheduleOperations(
				20,
				func() { s.KillConnection(t, 1) },
			)
			s.ScheduleOperations(
				30,
				func() { s.KillConnection(t, 0) },
			)
			s.ScheduleOperations(
				40,
				func() { s.KillConnection(t, 1) },
			)
			s.Run(t, ctrl)
			s.VerifyConsumers(t)
		}
	}
}

func TestSharedConsumerAddInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.AddInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.AddInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestReplicatedConsumerAddInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.AddInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.AddInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestSharedAndReplicatedConsumerAddInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		for j := 1; j <= maxRF; j++ {
			s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
				{ct: topic.Shared, isSharded: true, instances: 5, replicas: j},
				{ct: topic.Shared, isSharded: false, instances: 5, replicas: j},
				{ct: topic.Replicated, isSharded: true, instances: 5, replicas: j},
			})

			s.ScheduleOperations(
				10,
				func() { s.AddInstance(t, 0) },
			)
			s.ScheduleOperations(
				20,
				func() { s.AddInstance(t, 1) },
			)
			s.ScheduleOperations(
				30,
				func() { s.AddInstance(t, 0) },
			)
			s.ScheduleOperations(
				40,
				func() { s.AddInstance(t, 1) },
			)
			s.Run(t, ctrl)
			s.VerifyConsumers(t)
		}
	}
}

func TestSharedConsumerRemoveInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.RemoveInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.RemoveInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestReplicatedConsumerRemoveInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.RemoveInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.RemoveInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestSharedAndReplicatedConsumerRemoveInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		for j := 1; j <= maxRF; j++ {
			s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
				{ct: topic.Shared, isSharded: true, instances: 5, replicas: j},
				{ct: topic.Shared, isSharded: false, instances: 5, replicas: j},
				{ct: topic.Replicated, isSharded: true, instances: 5, replicas: j},
			})

			s.ScheduleOperations(
				10,
				func() { s.RemoveInstance(t, 0) },
			)
			s.ScheduleOperations(
				20,
				func() { s.RemoveInstance(t, 1) },
			)
			s.ScheduleOperations(
				30,
				func() { s.RemoveInstance(t, 0) },
			)
			s.ScheduleOperations(
				40,
				func() { s.RemoveInstance(t, 1) },
			)
			s.Run(t, ctrl)
			s.VerifyConsumers(t)
		}
	}
}

func TestSharedConsumerReplaceInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.ReplaceInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.ReplaceInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestReplicatedConsumerReplaceInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			10,
			func() { s.ReplaceInstance(t, 0) },
		)
		s.ScheduleOperations(
			20,
			func() { s.ReplaceInstance(t, 0) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
	}
}

func TestSharedAndReplicatedConsumerReplaceInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		for j := 1; j <= maxRF; j++ {
			s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
				{ct: topic.Shared, isSharded: true, instances: 5, replicas: j},
				{ct: topic.Shared, isSharded: false, instances: 5, replicas: j},
				{ct: topic.Replicated, isSharded: true, instances: 5, replicas: j},
			})

			s.ScheduleOperations(
				10,
				func() { s.ReplaceInstance(t, 0) },
			)
			s.ScheduleOperations(
				20,
				func() { s.ReplaceInstance(t, 1) },
			)
			s.ScheduleOperations(
				30,
				func() { s.ReplaceInstance(t, 0) },
			)
			s.ScheduleOperations(
				40,
				func() { s.ReplaceInstance(t, 1) },
			)
			s.Run(t, ctrl)
			s.VerifyConsumers(t)
		}
	}
}

func TestRemoveConsumerService(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Shared, isSharded: false, instances: 1, replicas: 1},
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			20,
			func() { s.RemoveConsumerService(t, 2) },
		)
		s.Run(t, ctrl)
		s.VerifyConsumers(t)
		require.Equal(t, msgPerShard*numberOfShards, s.consumerServices[0].numConsumed())
		require.Equal(t, msgPerShard*numberOfShards, s.consumerServices[1].numConsumed())
	}
}

func TestAddConsumerService(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(test.Reporter{t})
	defer ctrl.Finish()

	for i := 1; i <= maxProducers; i++ {
		s := newTestSetup(t, ctrl, i, []consumerServiceConfig{
			{ct: topic.Shared, isSharded: true, instances: 5, replicas: 2},
			{ct: topic.Replicated, isSharded: true, instances: 5, replicas: 2},
		})

		s.ScheduleOperations(
			20,
			func() {
				s.AddConsumerService(t, consumerServiceConfig{ct: topic.Shared, isSharded: false, instances: 1, replicas: 1, lateJoin: true})
			},
		)
		s.Run(t, ctrl)
		require.Equal(t, s.ExpectedNumMessages(), s.consumerServices[0].numConsumed())
		require.Equal(t, s.ExpectedNumMessages(), s.consumerServices[1].numConsumed())
		require.True(t, s.consumerServices[2].numConsumed() <= s.ExpectedNumMessages()*80/100)
	}
}

func TestMain(m *testing.M) {
	// NB: this is flaky as messageWriters are stopped async by consumer writers
	goleak.VerifyTestMain(m, goleak.IgnoreCurrent())
}
