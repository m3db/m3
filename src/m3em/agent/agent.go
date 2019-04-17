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

package agent

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/m3em/checksum"
	"github.com/m3db/m3/src/m3em/generated/proto/m3em"
	"github.com/m3db/m3/src/m3em/os/exec"
	"github.com/m3db/m3/src/m3em/os/fs"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	defaultReportInterval              = 5 * time.Second
	defaultTestCanaryPrefix            = "test-canary-file"
	reasonTeardownHeartbeat            = "remote agent received Teardown(), turning off heartbeating"
	reasonSetupInitializeHostResources = "unable to initialize host resources, turning off heartbeating"
)

var (
	errProcessMonitorNotDefined = fmt.Errorf("process monitor not defined")
	errNoValidTargetsSpecified  = fmt.Errorf("no valid target destinations specified")
	errOnlyDataFileMultiTarget  = fmt.Errorf("multiple targets are only supported for data files")
)

type opAgent struct {
	sync.RWMutex
	token               string
	executablePath      string
	configPath          string
	newProcessMonitorFn newProcessMonitorFn
	processMonitor      exec.ProcessMonitor
	heartbeater         *heatbeater

	running            int32
	stopping           int32
	heartbeatTimeoutCh chan struct{}

	opts    Options
	logger  *zap.Logger
	metrics *opAgentMetrics
	doneCh  chan struct{}
	closeCh chan struct{}
}

type newProcessMonitorFn func(exec.Cmd, exec.ProcessListener) (exec.ProcessMonitor, error)

// New creates and returns a new Operator Agent
func New(
	opts Options,
) (Agent, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	if err := canaryWriteTest(opts.WorkingDirectory()); err != nil {
		return nil, err
	}

	agent := &opAgent{
		opts:                opts,
		logger:              opts.InstrumentOptions().Logger(),
		metrics:             newAgentMetrics(opts.InstrumentOptions().MetricsScope()),
		newProcessMonitorFn: exec.NewProcessMonitor,
		doneCh:              make(chan struct{}, 1),
		closeCh:             make(chan struct{}, 1),
	}
	go agent.reportMetrics()
	return agent, nil
}

func (o *opAgent) Close() error {
	o.closeCh <- struct{}{}
	<-o.doneCh
	return nil
}

func canaryWriteTest(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("unable to stat directory, [ err = %v ]", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("path is not a directory")
	}

	fd, err := ioutil.TempFile(dir, defaultTestCanaryPrefix)
	if err != nil {
		return fmt.Errorf("unable to create canary file, [ err = %v ]", err)
	}
	os.Remove(fd.Name())

	return nil
}

func updateBoolGauge(b bool, m tally.Gauge) {
	if b {
		m.Update(1)
	} else {
		m.Update(0)
	}
}

func (o *opAgent) reportMetrics() {
	reportTicker := time.NewTicker(defaultReportInterval)
	for {
		select {
		case <-reportTicker.C:
			state := o.state()
			updateBoolGauge(state.running, o.metrics.running)
			updateBoolGauge(state.executablePath != "", o.metrics.execTransferred)
			updateBoolGauge(state.configPath != "", o.metrics.confTransferred)
		case <-o.closeCh:
			reportTicker.Stop()
			o.doneCh <- struct{}{}
			return
		}
	}
}

func (o *opAgent) Running() bool {
	return atomic.LoadInt32(&o.running) == 1
}

type opAgentState struct {
	running        bool
	executablePath string
	configPath     string
}

func (o *opAgent) state() opAgentState {
	o.RLock()
	defer o.RUnlock()
	return opAgentState{
		running:        o.Running(),
		executablePath: o.executablePath,
		configPath:     o.configPath,
	}
}

func (o *opAgent) Start(ctx context.Context, request *m3em.StartRequest) (*m3em.StartResponse, error) {
	o.logger.Info("received Start()")
	o.Lock()
	defer o.Unlock()

	if o.Running() {
		return nil, grpc.Errorf(codes.FailedPrecondition, "already running")
	}

	if o.executablePath == "" {
		return nil, grpc.Errorf(codes.FailedPrecondition, "agent missing build")
	}

	if o.configPath == "" {
		return nil, grpc.Errorf(codes.FailedPrecondition, "agent missing config")
	}

	if err := o.startWithLock(); err != nil {
		return nil, grpc.Errorf(codes.Internal, "unable to start: %v", err)
	}

	return &m3em.StartResponse{}, nil
}

func (o *opAgent) onProcessTerminate(err error) {
	if err == nil {
		err = fmt.Errorf("test process terminated without error")
	} else {
		err = fmt.Errorf("test process terminated with error: %v", err)
	}
	o.logger.Warn(err.Error())
	if stopping := atomic.LoadInt32(&o.stopping); stopping == 0 && o.heartbeater != nil {
		o.heartbeater.notifyProcessTermination(err.Error())
	}
	atomic.StoreInt32(&o.running, 0)
}

func (o *opAgent) newProcessListener() exec.ProcessListener {
	return exec.NewProcessListener(func() {
		o.onProcessTerminate(nil)
	}, func(err error) {
		o.onProcessTerminate(err)
	})
}

func (o *opAgent) startWithLock() error {
	var (
		path, args = o.opts.ExecGenFn()(o.executablePath, o.configPath)
		osArgs     = append([]string{path}, args...)
		cmd        = exec.Cmd{
			Path:      path,
			Args:      osArgs,
			OutputDir: o.opts.WorkingDirectory(),
			Env:       o.opts.EnvMap(),
		}
		listener = o.newProcessListener()
	)
	pm, err := o.newProcessMonitorFn(cmd, listener)
	if err != nil {
		return err
	}
	o.logger.Info("executing command", zap.Any("command", cmd))
	if err := pm.Start(); err != nil {
		return err
	}
	atomic.StoreInt32(&o.running, 1)
	o.processMonitor = pm
	return nil
}

func (o *opAgent) Stop(ctx context.Context, request *m3em.StopRequest) (*m3em.StopResponse, error) {
	o.logger.Info("received Stop()")
	o.Lock()
	defer o.Unlock()

	if !o.Running() {
		return nil, grpc.Errorf(codes.FailedPrecondition, "not running")
	}

	atomic.StoreInt32(&o.stopping, 1)
	if err := o.stopWithLock(); err != nil {
		return nil, grpc.Errorf(codes.Internal, "unable to stop: %v", err)
	}
	atomic.StoreInt32(&o.stopping, 0)

	return &m3em.StopResponse{}, nil
}

func (o *opAgent) stopWithLock() error {
	if o.processMonitor == nil {
		return errProcessMonitorNotDefined
	}

	if err := o.processMonitor.Stop(); err != nil {
		return err
	}

	o.processMonitor = nil
	atomic.StoreInt32(&o.running, 0)
	return nil
}

func (o *opAgent) resetWithLock(reason string) error {
	var multiErr xerrors.MultiError

	if o.heartbeater != nil {
		o.logger.Info("stopping heartbeating")
		if reason != "" {
			o.heartbeater.notifyOverwrite(reason)
		}
		multiErr = multiErr.Add(o.heartbeater.close())
		o.heartbeater = nil
	}

	if o.heartbeatTimeoutCh != nil {
		close(o.heartbeatTimeoutCh)
		o.heartbeatTimeoutCh = nil
	}

	if o.Running() {
		o.logger.Info("process running, stopping")
		if err := o.stopWithLock(); err != nil {
			o.logger.Warn("unable to stop", zap.Error(err))
			multiErr = multiErr.Add(err)
		}
	}

	o.logger.Info("releasing host resources")
	if err := o.opts.ReleaseHostResourcesFn()(); err != nil {
		o.logger.Info("unable to release host resources", zap.Error(err))
		multiErr = multiErr.Add(err)
	}

	o.token = ""
	o.executablePath = ""
	o.configPath = ""
	atomic.StoreInt32(&o.running, 0)

	return multiErr.FinalError()
}

func (o *opAgent) Teardown(ctx context.Context, request *m3em.TeardownRequest) (*m3em.TeardownResponse, error) {
	o.logger.Info("received Teardown()")
	o.Lock()
	defer o.Unlock()

	if err := o.resetWithLock(reasonTeardownHeartbeat); err != nil {
		return nil, grpc.Errorf(codes.Internal, "unable to teardown: %v", err)
	}

	return &m3em.TeardownResponse{}, nil
}

func (o *opAgent) isSetup() bool {
	o.RLock()
	defer o.RUnlock()
	return o.isSetupWithLock()
}

func (o *opAgent) isSetupWithLock() bool {
	return o.token != ""
}

func (o *opAgent) Setup(ctx context.Context, request *m3em.SetupRequest) (*m3em.SetupResponse, error) {
	o.logger.Info("received Setup()")

	// nil check
	if request == nil || request.SessionToken == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "nil request")
	}

	o.Lock()
	defer o.Unlock()

	if o.token != "" && o.token != request.SessionToken && !request.Force {
		return nil, grpc.Errorf(codes.AlreadyExists, "agent already initialized with token: %s", o.token)
	}

	if o.isSetupWithLock() {
		// reset agent
		msg := fmt.Sprintf("heartbeating being overwritten by new setup request: %+v", *request)
		if err := o.resetWithLock(msg); err != nil {
			return nil, grpc.Errorf(codes.Aborted, "unable to reset: %v", err)
		}
	}

	// remove any files stored in the working directory
	wd := o.opts.WorkingDirectory()
	o.logger.Info("removing contents from working directory", zap.String("dir", wd))
	if err := fs.RemoveContents(wd); err != nil {
		return nil, grpc.Errorf(codes.Internal, "unable to clear working directory: %v", err)
	}

	// initialize any resources needed on the host
	o.logger.Info("initializing host resources")
	if err := o.opts.InitHostResourcesFn()(); err != nil {
		o.resetWithLock(reasonSetupInitializeHostResources) // release any resources
		return nil, grpc.Errorf(codes.Internal, "unable to initialize host resources: %v", err)
	}

	// setup new heartbeating
	if request.HeartbeatEnabled {
		opts := heartbeatOpts{
			operatorUUID: request.OperatorUuid,
			endpoint:     request.HeartbeatEndpoint,
			nowFn:        o.opts.NowFn(),
			timeout:      o.opts.HeartbeatTimeout(),
			timeoutFn:    o.heartbeatingTimeout,
			errorFn:      o.heartbeatInternalError,
		}
		beater, err := newHeartbeater(o, opts, o.opts.InstrumentOptions())
		if err != nil {
			o.resetWithLock(reasonSetupInitializeHostResources) // release any resources
			return nil, grpc.Errorf(codes.Aborted, "unable to start heartbeating process: %v", err)
		}
		o.heartbeater = beater
		o.heartbeater.start(time.Second * time.Duration(request.HeartbeatFrequencySecs))
	}

	o.token = request.SessionToken
	return &m3em.SetupResponse{}, nil
}

func (o *opAgent) heartbeatingTimeout(lastHb time.Time) {
	o.logger.Warn("heartbeat sending timed out, resetting agent")
	o.Lock()
	err := o.resetWithLock("") // "" indicates we don't want to send a heartbeat
	o.Unlock()
	if err == nil {
		o.logger.Info("successfully reset agent")
	} else {
		o.logger.Warn("error while resetting agent", zap.Error(err))
	}
}

func (o *opAgent) heartbeatInternalError(err error) {
	o.logger.Warn("received unknown error whilst heartbeat", zap.Error(err))
	o.logger.Warn("resetting agent")
	o.Lock()
	err = o.resetWithLock(err.Error())
	o.Unlock()
	if err == nil {
		o.logger.Info("successfully reset agent")
	} else {
		o.logger.Warn("error while resetting agent", zap.Error(err))
	}
}

func (o *opAgent) pathsRelativeToWorkingDir(
	targets []string,
) ([]string, error) {
	files := make([]string, 0, len(targets))
	for _, t := range targets {
		if strings.Contains(t, "..") { // i.e. relative path
			return nil, fmt.Errorf("relative paths not allowed: %v", t)
		}
		f := path.Join(o.opts.WorkingDirectory(), t)
		files = append(files, f)
	}
	return files, nil
}

func (o *opAgent) initFile(
	fileType m3em.PushFileType,
	targets []string,
	overwrite bool,
) (*multiWriter, error) {
	if len(targets) < 1 {
		return nil, errNoValidTargetsSpecified
	}

	if len(targets) > 1 && fileType != m3em.PushFileType_PUSH_FILE_TYPE_DATA_FILE {
		return nil, errOnlyDataFileMultiTarget
	}

	paths, err := o.pathsRelativeToWorkingDir(targets)
	if err != nil {
		return nil, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	if overwrite {
		flags = flags | os.O_TRUNC
	}

	fileMode := o.opts.NewFileMode()
	if fileType == m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_BINARY {
		fileMode = os.FileMode(0755)
	}

	dirMode := o.opts.NewDirectoryMode()
	return newMultiWriter(paths, flags, fileMode, dirMode)
}

func (o *opAgent) markFileDone(
	fileType m3em.PushFileType,
	mw *multiWriter,
) error {
	if len(mw.fds) != 1 && (fileType == m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_BINARY || fileType == m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_CONFIG) {
		// should never happen
		return fmt.Errorf("internal error: multiple targets for binary/config")
	}

	for _, fd := range mw.fds {
		o.logger.Info("file transferred",
			zap.Stringer("type", fileType),
			zap.String("path", fd.Name()))
	}

	o.Lock()
	defer o.Unlock()

	if fileType == m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_BINARY {
		o.executablePath = mw.fds[0].Name()
	}

	if fileType == m3em.PushFileType_PUSH_FILE_TYPE_SERVICE_CONFIG {
		o.configPath = mw.fds[0].Name()
	}

	return nil
}

// PullFile receives a file from the caller to be stored locally on the agent
func (o *opAgent) PushFile(stream m3em.Operator_PushFileServer) error {
	o.logger.Info("received PushFile()")
	var (
		checksum     = checksum.NewAccumulator()
		numChunks    = 0
		lastChunkIdx = int32(0)
		fileHandle   *multiWriter
		fileType     = m3em.PushFileType_PUSH_FILE_TYPE_UNKNOWN
		err          error
	)

	for {
		request, streamErr := stream.Recv()
		if streamErr != nil && streamErr != io.EOF {
			return streamErr
		}

		if request == nil {
			break
		}

		if numChunks == 0 {
			// first request with any data in it, log it for visibilty
			o.logger.Info("file transfer initiated",
				zap.Strings("targets", request.GetTargetPaths()),
				zap.Stringer("fileType", request.GetType()),
				zap.Bool("overwrite", request.GetOverwrite()))

			fileType = request.GetType()
			fileHandle, err = o.initFile(fileType, request.GetTargetPaths(), request.GetOverwrite())
			if err != nil {
				return err
			}
			lastChunkIdx = request.GetData().GetIdx() - 1
		}

		chunkIdx := request.GetData().GetIdx()
		if chunkIdx != 1+lastChunkIdx {
			return fmt.Errorf("received chunkIdx: %d after %d", chunkIdx, lastChunkIdx)
		}
		lastChunkIdx = chunkIdx

		numChunks++
		bytes := request.GetData().GetBytes()
		checksum.Update(bytes)

		numWritten, err := fileHandle.write(bytes)
		if err != nil {
			return err
		}

		if numWritten != len(bytes) {
			return fmt.Errorf("unable to write bytes, expected: %d, observed: %d", len(bytes), numWritten)
		}

		if streamErr == io.EOF {
			break
		}
	}

	if fileHandle == nil {
		return fmt.Errorf("multiwriter has not been initialized")
	}

	var me xerrors.MultiError
	me = me.Add(fileHandle.Close())
	me = me.Add(o.markFileDone(fileType, fileHandle))
	if err := me.FinalError(); err != nil {
		return err
	}

	return stream.SendAndClose(&m3em.PushFileResponse{
		FileChecksum:   checksum.Current(),
		NumChunksRecvd: int32(numChunks),
	})
}

func validatePullFileRequest(request *m3em.PullFileRequest) error {
	if request == nil {
		return grpc.Errorf(codes.InvalidArgument, "nil request")
	}

	if request.ChunkSize <= 0 {
		return grpc.Errorf(codes.InvalidArgument, "chunkSize must be a positive integer")
	}

	if request.MaxSize < 0 {
		return grpc.Errorf(codes.InvalidArgument, "maxSize must be a non-negative integer")
	}

	return nil
}

// PullFile sends a local agent file to the caller
func (o *opAgent) PullFile(request *m3em.PullFileRequest, stream m3em.Operator_PullFileServer) error {
	if err := validatePullFileRequest(request); err != nil {
		return err
	}
	o.logger.Info("received PullFile()", zap.Any("request", *request))

	o.RLock()
	defer o.RUnlock()

	if !o.isSetupWithLock() {
		return grpc.Errorf(codes.InvalidArgument, "agent has not been setup, unable to transfer file")
	}

	pm := o.processMonitor
	if pm == nil {
		return grpc.Errorf(codes.InvalidArgument, "no process running, unable to transfer file")
	}

	switch fileType := request.GetFileType(); fileType {
	case m3em.PullFileType_PULL_FILE_TYPE_SERVICE_STDERR:
		return o.sendLocalFileWithRLock(pm.StderrPath(), request.ChunkSize, request.MaxSize, stream)

	case m3em.PullFileType_PULL_FILE_TYPE_SERVICE_STDOUT:
		return o.sendLocalFileWithRLock(pm.StdoutPath(), request.ChunkSize, request.MaxSize, stream)

	default:
		return grpc.Errorf(codes.InvalidArgument, "received unknown pull file: %v", fileType)
	}
}

func (o *opAgent) sendLocalFileWithRLock(localPath string, chunkSize int64, maxBytes int64, stream m3em.Operator_PullFileServer) error {
	fi, err := os.Stat(localPath)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "unable to find file: %v", err)
	}

	fd, err := os.Open(localPath)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "unable to open file: %v", err)
	}

	var (
		reader    = bufio.NewReaderSize(fd, int(chunkSize))
		buf       = make([]byte, chunkSize)
		chunkIdx  = 1
		truncated = false
	)

	// check if we need to seek ahead or if we are sending all the bytes
	if maxBytes > 0 && fi.Size() > maxBytes {
		offset := fi.Size() - maxBytes
		if _, err := fd.Seek(offset, 0 /* relative to start of file */); err != nil {
			return grpc.Errorf(codes.Internal, "unable to seek file: %v", err)
		}
		truncated = true
	}

	for {
		n, err := reader.Read(buf)
		switch err {
		case io.EOF:
			// i.e. streamed through the file, we can indicate we're done
			return nil

		case nil:
			// i.e. this read succeeded, send it and continue as we can read more data
			if streamErr := stream.Send(&m3em.PullFileResponse{
				Data: &m3em.DataChunk{
					Bytes: buf[:n],
					Idx:   int32(chunkIdx),
				},
				Truncated: truncated,
			}); streamErr != nil {
				return grpc.Errorf(codes.Internal, "unable to send chunk: %v", streamErr.Error())
			}

		default:
			// i.e. something broke
			return grpc.Errorf(codes.Unavailable, "unable to read file: %v", err.Error())
		}

		// increment idx
		chunkIdx++
	}

}

type opAgentMetrics struct {
	// TODO(prateek): process monitor opts, metric for process uptime
	running         tally.Gauge
	execTransferred tally.Gauge
	confTransferred tally.Gauge
}

func newAgentMetrics(scope tally.Scope) *opAgentMetrics {
	subscope := scope.SubScope("agent")
	return &opAgentMetrics{
		running:         subscope.Gauge("running"),
		execTransferred: subscope.Gauge("exec_transferred"),
		confTransferred: subscope.Gauge("conf_transferred"),
	}
}
