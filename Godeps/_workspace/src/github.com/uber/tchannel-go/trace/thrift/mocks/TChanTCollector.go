package mocks

import "github.com/uber/tchannel-go/trace/thrift/gen-go/tcollector"
import "github.com/stretchr/testify/mock"

import "github.com/uber/tchannel-go/thrift"

type TChanTCollector struct {
	mock.Mock
}

func (_m *TChanTCollector) GetSamplingStrategy(_ctx thrift.Context, _serviceName string) (*tcollector.SamplingStrategyResponse, error) {
	ret := _m.Called(_ctx, _serviceName)

	var r0 *tcollector.SamplingStrategyResponse
	if rf, ok := ret.Get(0).(func(thrift.Context, string) *tcollector.SamplingStrategyResponse); ok {
		r0 = rf(_ctx, _serviceName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tcollector.SamplingStrategyResponse)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, string) error); ok {
		r1 = rf(_ctx, _serviceName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *TChanTCollector) Submit(_ctx thrift.Context, _span *tcollector.Span) (*tcollector.Response, error) {
	ret := _m.Called(_ctx, _span)

	var r0 *tcollector.Response
	if rf, ok := ret.Get(0).(func(thrift.Context, *tcollector.Span) *tcollector.Response); ok {
		r0 = rf(_ctx, _span)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tcollector.Response)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, *tcollector.Span) error); ok {
		r1 = rf(_ctx, _span)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *TChanTCollector) SubmitBatch(_ctx thrift.Context, _spans []*tcollector.Span) ([]*tcollector.Response, error) {
	ret := _m.Called(_ctx, _spans)

	var r0 []*tcollector.Response
	if rf, ok := ret.Get(0).(func(thrift.Context, []*tcollector.Span) []*tcollector.Response); ok {
		r0 = rf(_ctx, _spans)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*tcollector.Response)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(thrift.Context, []*tcollector.Span) error); ok {
		r1 = rf(_ctx, _spans)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
