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

package deploy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/aggregator/aggregator"
	xsync "github.com/m3db/m3/src/x/sync"
)

// validator performs validation on a deployment target, returning nil if
// validation succeeds, and an error otherwise.
type validator func() error

// validatorFactory is a factory of validators.
type validatorFactory interface {
	// ValidatorFor generates a validator based on the instance,
	// the instance group, and the instance target type.
	ValidatorFor(
		instance instanceMetadata,
		group *instanceGroup,
		targetType targetType,
	) validator
}

type factory struct {
	client  AggregatorClient
	workers xsync.WorkerPool
}

func newValidatorFactory(client AggregatorClient, workers xsync.WorkerPool) validatorFactory {
	return factory{client: client, workers: workers}
}

func (f factory) ValidatorFor(
	instance instanceMetadata,
	group *instanceGroup,
	targetType targetType,
) validator {
	if targetType == leaderTarget {
		return f.validatorForLeader(instance, group)
	}
	return f.validatorForFollower(instance)
}

func (f factory) validatorForFollower(follower instanceMetadata) validator {
	return func() error {
		status, err := f.client.Status(follower.APIEndpoint)
		if err != nil {
			return err
		}
		electionState := status.FlushStatus.ElectionState
		if electionState != aggregator.FollowerState {
			return fmt.Errorf("election state is %s not follower", electionState.String())
		}
		return nil
	}
}

func (f factory) validatorForLeader(
	leader instanceMetadata,
	group *instanceGroup,
) validator {
	return func() error {
		status, err := f.client.Status(leader.APIEndpoint)
		if err != nil {
			return err
		}
		electionState := status.FlushStatus.ElectionState
		if electionState != aggregator.LeaderState {
			return fmt.Errorf("election state is %s not leader", electionState.String())
		}
		// We also need to verify the followers in the same group are ready to take
		// over the leader role when the leader resigns.
		var (
			found     bool
			wg        sync.WaitGroup
			canLeadCh = make(chan bool, 1)
		)
		for _, instance := range group.All {
			// Skip check if the instance is the leader.
			if instance.PlacementInstanceID == group.LeaderID {
				found = true
				continue
			}
			instance := instance
			wg.Add(1)
			f.workers.Go(func() {
				defer wg.Done()

				status, err := f.client.Status(instance.APIEndpoint)
				if err != nil {
					return
				}
				isFollower := status.FlushStatus.ElectionState == aggregator.FollowerState
				canLead := status.FlushStatus.CanLead
				if isFollower && canLead {
					select {
					case canLeadCh <- true:
					default:
					}
				}
			})
		}

		// Asynchronously close the channel after all goroutines return.
		f.workers.Go(func() {
			wg.Wait()
			close(canLeadCh)
		})

		if !found {
			return fmt.Errorf("instance %s is not in the instance group", leader.PlacementInstanceID)
		}
		if canLead := <-canLeadCh; !canLead {
			return errors.New("no follower instance is ready to lead")
		}
		return nil
	}
}
