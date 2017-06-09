package util

import (
	"fmt"
	"time"

	"github.com/m3db/m3em/node"
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

// NewPanicListener returns a listener that panics on any errors
func NewPullLogsAndPanicListener() node.Listener {
	return &pullLogAndPanicListener{}
}

type pullLogAndPanicListener struct{}

func (pl *pullLogAndPanicListener) OnProcessTerminate(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received process termination notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}

func (pl *pullLogAndPanicListener) OnHeartbeatTimeout(inst node.ServiceNode, ts time.Time) {
	panic(fmt.Sprintf("agent heartbeating timeout for instanace id = %v, last_heartbeat = %v. Terminating test early.", inst.ID(), ts.String()))
}

func (pl *pullLogAndPanicListener) OnOverwrite(inst node.ServiceNode, desc string) {
	panic(fmt.Sprintf("received overwrite notification for instanace id = %v, msg = %v. Terminating test early.", inst.ID(), desc))
}
