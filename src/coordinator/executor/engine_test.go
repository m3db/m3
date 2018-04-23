package executor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestExecute(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("dummy"))

	// Results is closed by execute
	results := make(chan *storage.QueryResult)
	closing := make(chan bool)

	engine := NewEngine(local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48))))
	go engine.Execute(context.TODO(), &storage.FetchQuery{}, &EngineOptions{}, closing, results)
	<-results
	assert.Equal(t, len(engine.tracker.queries), 1)
}
