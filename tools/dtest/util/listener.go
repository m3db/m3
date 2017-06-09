package util

import (
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/log"
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

	// first retrieve stderr, and output it
	stderrPath := p.newLogPath(inst, stderrExtension)
	truncated, err := inst.GetRemoteOutput(node.RemoteProcessStderr, stderrPath)
	if err != nil {
		p.logger.Errorf("Unable to retrieve stderr logs, err = %v", err)
	} else {
		p.outputRetrievedFile(inst.ID(), stderrExtension, stderrPath, truncated)
	}

	// then do the same for stdout
	stdoutPath := p.newLogPath(inst, stdoutExtension)
	truncated, err = inst.GetRemoteOutput(node.RemoteProcessStdout, stdoutPath)
	if err != nil {
		p.logger.Errorf("Unable to retrieve stdout logs, err = %v", err)
	} else {
		p.outputRetrievedFile(inst.ID(), stdoutExtension, stdoutPath, truncated)
	}

	panic(fmt.Sprintf("%v. Terminating test early.", logMsg))
}

func (p *pullLogAndPanicListener) OnHeartbeatTimeout(inst node.ServiceNode, ts time.Time) {
	panic(fmt.Sprintf("agent heartbeating timeout for instanace id = %v, last_heartbeat = %v. Terminating test early.", inst.ID(), ts.String()))
}

func (p *pullLogAndPanicListener) OnOverwrite(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received overwrite notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}
