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

package downsample

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"
)

// localLeaderService provides a mocked out local leader service so that
// we do not need to rely on using an etcd cluster just to elect a leader
// for aggregation in-process (which doesn't need leader election at all,
// however it is simpler to keep the current aggregator code structured the
// way it is which is most natural for accommodating the distributed and
// non-in-process aggregation use case).
type localLeaderService struct {
	sync.Mutex
	id        services.ServiceID
	elections map[string]chan campaign.Status
	leaders   map[string]string
}

func newLocalLeaderService(id services.ServiceID) services.LeaderService {
	l := &localLeaderService{id: id}
	l.reset()
	return l
}

func (l *localLeaderService) reset() {
	l.Lock()
	defer l.Unlock()
	l.elections = make(map[string]chan campaign.Status)
	l.leaders = make(map[string]string)
}

func (l *localLeaderService) Campaign(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error) {
	l.Lock()
	defer l.Unlock()

	campaignCh, ok := l.elections[electionID]
	if !ok {
		campaignCh = make(chan campaign.Status, 1)
		campaignCh <- campaign.Status{State: campaign.Leader}
		l.elections[electionID] = campaignCh
		l.leaders[electionID] = l.id.String()
	}
	return campaignCh, nil
}

func (l *localLeaderService) push(id string, status campaign.Status) error {
	l.Lock()
	defer l.Unlock()

	campaignCh, ok := l.elections[id]
	if !ok {
		return fmt.Errorf("no such campaign: %s", id)
	}

	campaignCh <- status
	return nil
}

func (l *localLeaderService) Resign(electionID string) error {
	return l.push(electionID, campaign.Status{State: campaign.Follower})
}

func (l *localLeaderService) Leader(electionID string) (string, error) {
	l.Lock()
	defer l.Unlock()

	leader, ok := l.leaders[electionID]
	if !ok {
		return "", fmt.Errorf("no such campaign: %s", electionID)
	}
	return leader, nil
}

func (l *localLeaderService) Observe(electionID string) (<-chan string, error) {
	return nil, errors.New("unimplemented")
}

func (l *localLeaderService) Close() error {
	l.reset()
	return nil
}
