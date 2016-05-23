package tchannelthrift

import (
	"fmt"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/tchannelthrift/thrift/gen-go/rpc"
)

func valueToTime(value int64, timeType rpc.TimeType) (time.Time, error) {
	unit, err := timeTypeToUnit(timeType)
	if err != nil {
		return time.Time{}, err
	}
	return memtsdb.FromNormalizedTime(value, unit), nil
}

func timeToValue(t time.Time, timeType rpc.TimeType) (int64, error) {
	unit, err := timeTypeToUnit(timeType)
	if err != nil {
		return 0, err
	}
	return memtsdb.ToNormalizedTime(t, unit), nil
}

func timeTypeToUnit(timeType rpc.TimeType) (time.Duration, error) {
	switch timeType {
	case rpc.TimeType_UNIX_SECONDS:
		return time.Second, nil
	case rpc.TimeType_UNIX_MICROSECONDS:
		return time.Microsecond, nil
	case rpc.TimeType_UNIX_MILLISECONDS:
		return time.Millisecond, nil
	case rpc.TimeType_UNIX_NANOSECONDS:
		return time.Nanosecond, nil
	}
	return 0, fmt.Errorf("unknown time type")
}
