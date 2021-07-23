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

package node

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/m3em/build"
	"github.com/m3db/m3/src/m3em/generated/proto/m3em"
	"github.com/m3db/m3/src/m3em/os/fs"
	xclock "github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/pborman/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	errUnableToSetupInitializedNode = fmt.Errorf("unable to setup node, must be either setup/uninitialized")
	errUnableToTeardownNode         = fmt.Errorf("unable to teardown node, must be either setup/running")
	errUnableToStartNode            = fmt.Errorf("unable to start node, it must be setup")
	errUnableToStopNode             = fmt.Errorf("unable to stop node, it must be running")
	errUnableToTransferFile         = fmt.Errorf("unable to transfer file. node must be setup/running")
)

type svcNode struct {
	sync.Mutex
	placement.Instance
	logger            *zap.Logger
	opts              Options
	status            Status
	currentBuild      build.ServiceBuild
	currentConf       build.ServiceConfiguration
	clientConn        *grpc.ClientConn
	client            m3em.OperatorClient
	listeners         *listenerGroup
	heartbeater       *opHeartbeatServer
	operatorUUID      string
	heartbeatEndpoint string
}

// New returns a new ServiceNode.
func New(
	node placement.Instance,
	opts Options,
) (ServiceNode, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	clientConn, client, err := opts.OperatorClientFn()()
	if err != nil {
		return nil, err
	}

	uuid := uuid.NewRandom()

	var (
		retNode = &svcNode{
			logger:   opts.InstrumentOptions().Logger(),
			opts:     opts,
			Instance: node,
			status:   StatusUninitialized,
		}
		listeners      = newListenerGroup(retNode)
		hbUUID         = uuid.String()
		heartbeater    *opHeartbeatServer
		routerEndpoint string
	)

	if opts.HeartbeatOptions().Enabled() {
		router := opts.HeartbeatOptions().HeartbeatRouter()
		routerEndpoint = router.Endpoint()
		heartbeater = newHeartbeater(listeners, opts.HeartbeatOptions(), opts.InstrumentOptions())
		if err := router.Register(hbUUID, heartbeater); err != nil {
			return nil, fmt.Errorf("unable to register heartbeat server with router: %v", err)
		}
	}

	retNode.listeners = listeners
	retNode.client = client
	retNode.clientConn = clientConn
	retNode.heartbeater = heartbeater
	retNode.heartbeatEndpoint = routerEndpoint
	retNode.operatorUUID = hbUUID
	return retNode, nil
}

func (i *svcNode) String() string {
	i.Lock()
	defer i.Unlock()
	return fmt.Sprintf("ServiceNode %s", i.Instance.String())
}

func (i *svcNode) heartbeatReceived() bool {
	return !i.heartbeater.lastHeartbeatTime().IsZero()
}

func (i *svcNode) Setup(
	bld build.ServiceBuild,
	conf build.ServiceConfiguration,
	token string,
	force bool,
) error {
	i.Lock()
	defer i.Unlock()
	if i.status != StatusUninitialized &&
		i.status != StatusSetup {
		return errUnableToSetupInitializedNode
	}

	i.currentConf = conf
	i.currentBuild = bld

	freq := uint32(i.opts.HeartbeatOptions().Interval().Seconds())
	err := i.opts.Retrier().Attempt(func() error {
		ctx := context.Background()
		_, err := i.client.Setup(ctx, &m3em.SetupRequest{
			OperatorUuid:           i.operatorUUID,
			SessionToken:           token,
			Force:                  force,
			HeartbeatEnabled:       i.opts.HeartbeatOptions().Enabled(),
			HeartbeatEndpoint:      i.heartbeatEndpoint,
			HeartbeatFrequencySecs: freq,
		})
		return err
	})

	if err != nil {
		return fmt.Errorf("unable to setup: %v", err)
	}

	// TODO(prateek): make heartbeat pickup existing agent state

	// Wait till we receive our first heartbeat
	if i.opts.HeartbeatOptions().Enabled() {
		i.logger.Info("waiting until initial heartbeat is received")
		received := xclock.WaitUntil(i.heartbeatReceived, i.opts.HeartbeatOptions().Timeout())
		if !received {
			return fmt.Errorf("did not receive heartbeat response from remote agent within timeout")
		}
		i.logger.Info("initial heartbeat received")

		// start hb monitoring
		if err := i.heartbeater.start(); err != nil {
			return fmt.Errorf("unable to start heartbeat monitor loop: %v", err)
		}
	}

	// transfer build
	if err := i.opts.Retrier().Attempt(func() error {
		iter, err := bld.Iter(i.opts.TransferBufferSize())
		if err != nil {
			return err
		}
		return i.transferFile(transferOpts{
			targets:   []string{bld.ID()},
			fileType:  m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_BINARY,
			overwrite: force,
			iter:      iter,
		})
	}); err != nil {
		return fmt.Errorf("unable to transfer build: %v", err)
	}

	if err := i.opts.Retrier().Attempt(func() error {
		iter, err := conf.Iter(i.opts.TransferBufferSize())
		if err != nil {
			return err
		}
		return i.transferFile(transferOpts{
			targets:   []string{conf.ID()},
			fileType:  m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_CONFIG,
			overwrite: force,
			iter:      iter,
		})
	}); err != nil {
		return fmt.Errorf("unable to transfer config: %v", err)
	}

	i.status = StatusSetup
	return nil
}

// nolint: maligned
type transferOpts struct {
	targets   []string
	fileType  m3em.PushFileType
	iter      fs.FileReaderIter
	overwrite bool
}

func (i *svcNode) transferFile(
	t transferOpts,
) error {
	defer t.iter.Close()
	ctx := context.Background()
	stream, err := i.client.PushFile(ctx)
	if err != nil {
		return err
	}
	chunkIdx := 0
	for ; t.iter.Next(); chunkIdx++ {
		bytes := t.iter.Current()
		request := &m3em.PushFileRequest{
			Type:        t.fileType,
			TargetPaths: t.targets,
			Overwrite:   t.overwrite,
			Data: &m3em.DataChunk{
				Bytes: bytes,
				Idx:   int32(chunkIdx),
			},
		}
		err := stream.Send(request)
		if err != nil {
			stream.CloseSend()
			return err
		}
	}
	if err := t.iter.Err(); err != nil {
		stream.CloseSend()
		return err
	}

	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	if int(response.NumChunksRecvd) != chunkIdx {
		return fmt.Errorf("sent %d chunks, server only received %d of them", chunkIdx, response.NumChunksRecvd)
	}

	if t.iter.Checksum() != response.FileChecksum {
		return fmt.Errorf("expected file checksum: %d, received: %d", t.iter.Checksum(), response.FileChecksum)
	}

	return nil
}

func (i *svcNode) TransferLocalFile(
	srcPath string,
	destPaths []string,
	overwrite bool,
) error {
	i.Lock()
	defer i.Unlock()

	if i.status != StatusSetup && i.status != StatusRunning {
		return errUnableToTransferFile
	}

	if err := i.opts.Retrier().Attempt(func() error {
		iter, err := fs.NewSizedFileReaderIter(srcPath, i.opts.TransferBufferSize())
		if err != nil {
			return err
		}
		return i.transferFile(transferOpts{
			targets:   destPaths,
			fileType:  m3em.PushFileType_PUSH_FILE_TYPE_DATA_FILE,
			overwrite: overwrite,
			iter:      iter,
		})
	}); err != nil {
		return fmt.Errorf("unable to transfer file: %v", err)
	}

	return nil
}

func (i *svcNode) pullRemoteFile(t m3em.PullFileType, fd *os.File) (bool, error) {
	ctx := context.Background()

	// resetting file in case this a retry
	if err := fd.Truncate(0); err != nil {
		return false, err
	}

	// create streaming client
	client, err := i.client.PullFile(ctx, &m3em.PullFileRequest{
		ChunkSize: int64(i.opts.TransferBufferSize()),
		MaxSize:   i.opts.MaxPullSize(),
		FileType:  t,
	})
	if err != nil {
		return false, err
	}

	// iterate through responses
	truncated := false
	for {
		response, err := client.Recv()
		switch err {
		case nil: // this Recv was successful, and we have more to read
			truncated = response.Truncated
			if _, writeErr := fd.Write(response.Data.Bytes); writeErr != nil {
				return truncated, writeErr
			}

		case io.EOF: // no more to read, indicate success
			return truncated, nil

		default: // unexpected error, indicate failure
			return truncated, err
		}
	}
}

func toM3EMPullType(t RemoteOutputType) (m3em.PullFileType, error) {
	switch t {
	case RemoteProcessStderr:
		return m3em.PullFileType_PULL_FILE_TYPE_SERVICE_STDERR, nil

	case RemoteProcessStdout:
		return m3em.PullFileType_PULL_FILE_TYPE_SERVICE_STDOUT, nil

	default:
		return m3em.PullFileType_PULL_FILE_TYPE_UNKNOWN, fmt.Errorf("unknown output type: %v", t)
	}
}

func (i *svcNode) GetRemoteOutput(
	t RemoteOutputType,
	localDest string,
) (bool, error) {
	i.Lock()
	defer i.Unlock()

	if i.status != StatusSetup && i.status != StatusRunning {
		return false, errUnableToTransferFile
	}

	mType, err := toM3EMPullType(t)
	if err != nil {
		return false, err
	}

	// create base directory for specified remote path if it doesn't exist
	base := filepath.Dir(localDest)
	if err := os.MkdirAll(base, os.FileMode(0755)|os.ModeDir); err != nil {
		return false, err
	}

	fd, err := os.OpenFile(localDest, os.O_CREATE|os.O_WRONLY, os.FileMode(0666))
	if err != nil {
		return false, err
	}

	truncated := false
	if retryErr := i.opts.Retrier().Attempt(func() error {
		truncated, err = i.pullRemoteFile(mType, fd)
		return err
	}); retryErr != nil {
		return truncated, fmt.Errorf("unable to get remote output: %v", retryErr)
	}

	return truncated, fd.Close()
}

func (i *svcNode) Teardown() error {
	i.Lock()
	defer i.Unlock()
	if status := i.status; status != StatusRunning &&
		status != StatusSetup &&
		status != StatusError {
		return errUnableToTeardownNode
	}

	// clear any listeners
	i.listeners.clear()

	if err := i.opts.Retrier().Attempt(func() error {
		ctx := context.Background()
		_, err := i.client.Teardown(ctx, &m3em.TeardownRequest{})
		return err
	}); err != nil {
		return err
	}

	if err := i.Close(); err != nil {
		return err
	}

	i.status = StatusUninitialized
	return nil
}

func (i *svcNode) Close() error {
	var err xerrors.MultiError

	if conn := i.clientConn; conn != nil {
		err = err.Add(conn.Close())
		i.clientConn = nil
	}

	if hbServer := i.heartbeater; hbServer != nil {
		hbServer.stop()
		err = err.Add(i.opts.HeartbeatOptions().HeartbeatRouter().Deregister(i.operatorUUID))
		i.heartbeater = nil
		i.operatorUUID = ""
	}

	return err.FinalError()
}

func (i *svcNode) Start() error {
	i.Lock()
	defer i.Unlock()
	if i.status != StatusSetup {
		return errUnableToStartNode
	}

	if err := i.opts.Retrier().Attempt(func() error {
		ctx := context.Background()
		_, err := i.client.Start(ctx, &m3em.StartRequest{})
		return err
	}); err != nil {
		return err
	}

	i.status = StatusRunning
	return nil
}

func (i *svcNode) Stop() error {
	i.Lock()
	defer i.Unlock()
	if i.status != StatusRunning {
		return errUnableToStopNode
	}

	if err := i.opts.Retrier().Attempt(func() error {
		ctx := context.Background()
		_, err := i.client.Stop(ctx, &m3em.StopRequest{})
		return err
	}); err != nil {
		return err
	}

	i.status = StatusSetup
	return nil
}

func (i *svcNode) Status() Status {
	i.Lock()
	defer i.Unlock()
	return i.status
}

func (i *svcNode) RegisterListener(l Listener) ListenerID {
	return ListenerID(i.listeners.add(l))
}

func (i *svcNode) DeregisterListener(token ListenerID) {
	i.listeners.remove(int(token))
}
