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

package handler

import (
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/sharding"

	"github.com/uber-go/tally"
)

// SharderRouter contains a sharder id and a router.
type SharderRouter struct {
	sharding.SharderID
	common.Router
}

type sharderRouters struct {
	SharderID sharding.SharderID
	Routers   []common.Router
}

type shardedHandler struct {
	routersBySharderID []SharderRouter
	writerOpts         writer.Options
}

// NewShardedHandler creates a new sharded handler.
func NewShardedHandler(srs []SharderRouter, writerOpts writer.Options) aggregator.Handler {
	// Group routers by their sharder ids so that metrics are only encoded once
	// for backends with the same sharding functions.
	var (
		nonShardedRouters  = make([]common.Router, 0, len(srs))
		shardedRoutersByID = make([]sharderRouters, 0, len(srs))
	)
	for _, sr := range srs {
		if sr.SharderID == sharding.NoShardingSharderID {
			nonShardedRouters = append(nonShardedRouters, sr.Router)
		} else {
			found := false
			for i := range shardedRoutersByID {
				if shardedRoutersByID[i].SharderID == sr.SharderID {
					shardedRoutersByID[i].Routers = append(shardedRoutersByID[i].Routers, sr.Router)
					found = true
					break
				}
			}
			if !found {
				shardedRoutersByID = append(shardedRoutersByID, sharderRouters{
					SharderID: sr.SharderID,
					Routers:   []common.Router{sr.Router},
				})
			}
		}
	}

	// If there are both sharded routers and non-sharded routers, the non-sharded
	// routers are merged into one of the sharded router groups so that metrics are
	// only encoded once for both sharded and non-sharded routers, saving CPU cycles.
	// This is okay because non-sharded routers simply forward data to backend queues
	// regardless of which shard the data belongs to.
	if len(shardedRoutersByID) == 0 {
		shardedRoutersByID = append(shardedRoutersByID, sharderRouters{
			SharderID: sharding.NoShardingSharderID,
			Routers:   nonShardedRouters,
		})
	} else {
		shardedRoutersByID[0].Routers = append(shardedRoutersByID[0].Routers, nonShardedRouters...)
	}

	routersBySharderID := make([]SharderRouter, 0, len(shardedRoutersByID))
	for _, sr := range shardedRoutersByID {
		var router common.Router
		if len(sr.Routers) == 1 {
			router = sr.Routers[0]
		} else {
			router = common.NewBroadcastRouter(sr.Routers)
		}
		routersBySharderID = append(routersBySharderID, SharderRouter{
			SharderID: sr.SharderID,
			Router:    router,
		})
	}
	return &shardedHandler{
		routersBySharderID: routersBySharderID,
		writerOpts:         writerOpts,
	}
}

func (h *shardedHandler) NewWriter(scope tally.Scope) (aggregator.Writer, error) {
	instrumentOpts := h.writerOpts.InstrumentOptions()
	writerOpts := h.writerOpts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(scope))
	if len(h.routersBySharderID) == 1 {
		sr := h.routersBySharderID[0]
		return writer.NewShardedWriter(sr.SharderID, sr.Router, writerOpts)
	}
	writers := make([]aggregator.Writer, 0, len(h.routersBySharderID))
	for _, sr := range h.routersBySharderID {
		w, err := writer.NewShardedWriter(sr.SharderID, sr.Router, writerOpts)
		if err != nil {
			return nil, err
		}
		writers = append(writers, w)
	}
	return writer.NewMultiWriter(writers), nil
}

func (h *shardedHandler) Close() {
	for _, sr := range h.routersBySharderID {
		sr.Close()
	}
}
