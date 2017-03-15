package convert

import (
	"fmt"
	"time"

	"github.com/m3db/m3nsch"
	proto "github.com/m3db/m3nsch/rpc"

	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
)

func toTimeFromProtoTimestamp(t *google_protobuf.Timestamp) time.Time {
	return time.Unix(int64(t.Seconds), int64(t.Nanos))
}

// ToM3nschWorkload converts a rpc Workload into an equivalent API Workload.
func ToM3nschWorkload(workload *proto.Workload) (m3nsch.Workload, error) {
	if workload == nil {
		return m3nsch.Workload{}, fmt.Errorf("invalid workload")
	}

	return m3nsch.Workload{
		BaseTime:     toTimeFromProtoTimestamp(workload.BaseTime),
		MetricPrefix: workload.MetricPrefix,
		Namespace:    workload.Namespace,
		Cardinality:  int(workload.Cardinality),
		IngressQPS:   int(workload.IngressQPS),
	}, nil
}

// ToM3nschStatus converts a rpc Status into an equivalent API Status.
func ToM3nschStatus(status proto.Status) (m3nsch.Status, error) {
	switch status {
	case proto.Status_UNINITIALIZED:
		return m3nsch.StatusUninitialized, nil
	case proto.Status_INITIALIZED:
		return m3nsch.StatusInitialized, nil
	case proto.Status_RUNNING:
		return m3nsch.StatusRunning, nil
	}
	return m3nsch.StatusUninitialized, fmt.Errorf("invalid status: %s", status.String())
}
