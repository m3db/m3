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

package util

import (
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"github.com/m3db/m3/src/m3em/node"
	xlog "github.com/m3db/m3x/log"
)

const (
	stderrExtension = "stderr"
	stdoutExtension = "stdout"
)

// NewPanicListener returns a listener that panics on any errors
func NewPanicListener() node.Listener {
	return &panicListener{}
}

type panicListener struct{}

func (pl *panicListener) OnProcessTerminate(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received process termination notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}

func (pl *panicListener) OnHeartbeatTimeout(inst node.ServiceNode, ts time.Time) {
	panic(fmt.Sprintf("agent heartbeating timeout for instanace id = %v, last_heartbeat = %v. Terminating test early.", inst.ID(), ts.String()))
}

func (pl *panicListener) OnOverwrite(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received overwrite notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}

// NewPullLogsAndPanicListener returns a listener that attempts to retrieve logs from the remote
// agent upon OnProcessTerminate invokation, and panics. It does not attempt to retrieve logs for
// neither OnHeartbeatTimeout, nor OnOverwrite.
func NewPullLogsAndPanicListener(l xlog.Logger, baseDir string) node.Listener {
	return &pullLogAndPanicListener{logger: l, dir: baseDir}
}

type pullLogAndPanicListener struct {
	logger xlog.Logger
	dir    string
}

func (p *pullLogAndPanicListener) newLogPath(inst node.ServiceNode, fileExtension string) string {
	return path.Join(p.dir, fmt.Sprintf("%s-%d.%s", inst.ID(), time.Now().UnixNano(), fileExtension))
}

func (p *pullLogAndPanicListener) outputRetrievedFile(instID string, logType string, filePath string, truncated bool) {
	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		p.logger.Errorf("unable to read logs from %s. skipping", filePath)
		return
	}
	stringContents := string(contents)

	var truncationMsg string
	if truncated {
		truncationMsg = "[WARNING] logs are truncated due to size.\n"
	}

	p.logger.Infof("Retrieved %s logs from instance id = %v\n. %s%s", logType, instID, truncationMsg, stringContents)
}

func (p *pullLogAndPanicListener) OnProcessTerminate(inst node.ServiceNode, desc string) {
	logMsg := fmt.Sprintf("Received process termination notification for instanace id = %v, msg = %v.", inst.ID(), desc)
	p.logger.Errorf("%s. Attempting to retrieve logs.", logMsg)

	retrieveAndOutput := func(extension string, outputType node.RemoteOutputType) {
		outputPath := p.newLogPath(inst, extension)
		truncated, err := inst.GetRemoteOutput(outputType, outputPath)
		if err != nil {
			p.logger.Errorf("Unable to retrieve %s logs, err = %v", extension, err)
		} else {
			p.outputRetrievedFile(inst.ID(), extension, outputPath, truncated)
		}
	}

	// retrieve and output: stderr, and stdout
	retrieveAndOutput(stderrExtension, node.RemoteProcessStderr)
	retrieveAndOutput(stdoutExtension, node.RemoteProcessStdout)
	panic(fmt.Sprintf("%v. Terminating test early.", logMsg))
}

func (p *pullLogAndPanicListener) OnHeartbeatTimeout(inst node.ServiceNode, ts time.Time) {
	panic(fmt.Sprintf("agent heartbeating timeout for instanace id = %v, last_heartbeat = %v. Terminating test early.", inst.ID(), ts.String()))
}

func (p *pullLogAndPanicListener) OnOverwrite(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received overwrite notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}
