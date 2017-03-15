package convert

import (
	"github.com/m3db/m3nsch"
	proto "github.com/m3db/m3nsch/rpc"

	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

// ToProtoStatus converts an API status into a RPC status.
func ToProtoStatus(status m3nsch.Status) proto.Status {
	switch status {
	case m3nsch.StatusUninitialized:
		return proto.Status_UNINITIALIZED
	case m3nsch.StatusInitialized:
		return proto.Status_INITIALIZED
	case m3nsch.StatusRunning:
		return proto.Status_RUNNING
	}
	return proto.Status_UNKNOWN
}

// ToProtoWorkload converts an API Workload into a RPC Workload.
func ToProtoWorkload(mw m3nsch.Workload) proto.Workload {
	var w proto.Workload
	w.BaseTime = &google_protobuf.Timestamp{
		Seconds: mw.BaseTime.Unix(),
		Nanos:   int32(mw.BaseTime.UnixNano()),
	}
	w.Cardinality = int32(mw.Cardinality)
	w.IngressQPS = int32(mw.IngressQPS)
	w.MetricPrefix = mw.MetricPrefix
	w.Namespace = mw.Namespace
	return w
}
