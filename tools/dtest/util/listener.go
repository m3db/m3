package util

import (
	"fmt"
	"time"

	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/log"
)

// NewFatalListener returns a listener that logs fatal on any errors
func NewFatalListener(logger xlog.Logger) node.Listener {
	return &fatalListener{logger}
}

type fatalListener struct {
	logger xlog.Logger
}

func (fl *fatalListener) OnProcessTerminate(inst node.ServiceNode, desc string) {
	fl.logger.Fatalf("received process termination notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc)
}

func (fl *fatalListener) OnHeartbeatTimeout(inst node.ServiceNode, ts time.Time) {
	fl.logger.Fatalf("agent heartbeating timeout for instanace id = %v, last_heartbeat = %v. Terminating test early.", inst.ID(), ts.String())
}

func (fl *fatalListener) OnOverwrite(inst node.ServiceNode, desc string) {
	fl.logger.Fatalf("received overwrite notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc)
}

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
