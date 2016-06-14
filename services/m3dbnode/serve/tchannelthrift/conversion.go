package tchannelthrift

import (
	"errors"
	"time"

	"code.uber.internal/infra/memtsdb/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

var (
	errUnknownTimeType = errors.New("unknown time type")
	timeZero           time.Time
)

func valueToTime(value int64, timeType rpc.TimeType) (time.Time, error) {
	unit, err := timeTypeToDuration(timeType)
	if err != nil {
		return timeZero, err
	}
	return xtime.FromNormalizedTime(value, unit), nil
}

func timeToValue(t time.Time, timeType rpc.TimeType) (int64, error) {
	unit, err := timeTypeToDuration(timeType)
	if err != nil {
		return 0, err
	}
	return xtime.ToNormalizedTime(t, unit), nil
}

func timeTypeToDuration(timeType rpc.TimeType) (time.Duration, error) {
	unit, err := timeTypeToUnit(timeType)
	if err != nil {
		return 0, err
	}
	return unit.Value()
}

func timeTypeToUnit(timeType rpc.TimeType) (xtime.Unit, error) {
	switch timeType {
	case rpc.TimeType_UNIX_SECONDS:
		return xtime.Second, nil
	case rpc.TimeType_UNIX_MILLISECONDS:
		return xtime.Millisecond, nil
	case rpc.TimeType_UNIX_MICROSECONDS:
		return xtime.Microsecond, nil
	case rpc.TimeType_UNIX_NANOSECONDS:
		return xtime.Nanosecond, nil
	}
	return 0, errUnknownTimeType
}
