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

// mockgen rules for generating mocks for exported interfaces (reflection mode).
//go:generate sh -c "mockgen -package=producer github.com/m3db/m3/src/msg/producer Message,Producer | genclean -pkg github.com/m3db/m3/src/msg/producer -out ../../producer/producer_mock.go"
//go:generate sh -c "mockgen -package=consumer github.com/m3db/m3/src/msg/consumer Message,MessageProcessor | genclean -pkg github.com/m3db/m3/src/msg/consumer -out ../../consumer/consumer_mock.go"
//go:generate sh -c "mockgen -package=proto github.com/m3db/m3/src/msg/protocol/proto Encoder,Decoder | genclean -pkg github.com/m3db/m3/src/msg/protocol/proto -out ../../protocol/proto/proto_mock.go"
//go:generate sh -c "mockgen -package=topic github.com/m3db/m3/src/msg/topic Service | genclean -pkg github.com/m3db/m3/src/msg/topic -out ../../topic/topic_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode).
//go:generate sh -c "mockgen -package=writer -destination=../../producer/writer/consumer_service_writer_mock.go -source=../../producer/writer/consumer_service_writer.go"
//go:generate sh -c "mockgen -package=writer -destination=../../producer/writer/router_mock.go -source=../../producer/writer/router.go"
//go:generate sh -c "mockgen -package=writer -destination=../../producer/writer/shard_writer_mock.go -source=../../producer/writer/shard_writer.go"
//go:generate sh -c "mockgen -package=writer -destination=../../producer/writer/context_dialer_mock_test.go -source=../../producer/writer/consumer_writer_test.go contextDialer"
//go:generate sh -c "mockgen -package=writer -destination=../../producer/writer/keep_alivable_mock_test.go -source=../../producer/writer/consumer_writer.go keepAlivable"
package mocks
