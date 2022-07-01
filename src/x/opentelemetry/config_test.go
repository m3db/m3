// Copyright (c) 2021 Uber Technologies, Inc.
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

package opentelemetry

//func TestConfiguration(t *testing.T) {
//	ctx := context.Background()
//	addr := localAddress(t)
//	r := grpcReceiver(t, "receiver", addr, consumertest.NewNop(), consumertest.NewNop())
//	require.NotNil(t, r)
//	require.NoError(t, r.Start(ctx, componenttest.NewNopHost()))
//	defer func() {
//		require.NoError(t, r.Shutdown(ctx))
//	}()
//
//	cfg := Configuration{
//		ServiceName: "foo",
//		Endpoint:    addr,
//		Insecure:    true,
//		Attributes:  map[string]string{"bar": "baz"},
//	}
//
//	tracerProvider, err := cfg.NewTracerProvider(ctx, tally.NoopScope,
//		TracerProviderOptions{})
//	require.NoError(t, err)
//	require.NotNil(t, tracerProvider)
//}
//
//func grpcReceiver(
//	t *testing.T,
//	name, endpoint string,
//	tc consumer.Traces,
//	mc consumer.Metrics,
//) component.Component {
//	factory := otlpreceiver.NewFactory()
//	cfg := factory.CreateDefaultConfig().(*otlpreceiver.Config)
//	cfg.SetIDName(name)
//	cfg.GRPC.NetAddr.Endpoint = endpoint
//	cfg.HTTP = nil
//
//	var (
//		set = componenttest.NewNopReceiverCreateSettings()
//		r   component.Component
//		err error
//	)
//	if tc != nil {
//		r, err = factory.CreateTracesReceiver(context.Background(), set, cfg, tc)
//		require.NoError(t, err)
//	}
//	if mc != nil {
//		r, err = factory.CreateMetricsReceiver(context.Background(), set, cfg, mc)
//		require.NoError(t, err)
//	}
//	return r
//}
//
//func localAddress(t *testing.T) string {
//	ln, err := net.Listen("tcp", "localhost:0")
//	require.NoError(t, err)
//	defer func() { _ = ln.Close() }()
//	return ln.Addr().String()
//}
