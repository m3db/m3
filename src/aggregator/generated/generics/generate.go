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

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/aggregator/generic_elem.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/aggregator/counter_elem.gen.go -pkg=aggregator gen \"timedAggregation=timedCounter lockedAggregation=*lockedCounter typeSpecificElemBase=counterElemBase genericElemPool=CounterElemPool GenericElem=CounterElem\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/aggregator/generic_elem.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/aggregator/timer_elem.gen.go -pkg=aggregator gen \"timedAggregation=timedTimer lockedAggregation=*lockedTimer typeSpecificElemBase=timerElemBase genericElemPool=TimerElemPool GenericElem=TimerElem\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/aggregator/generic_elem.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/aggregator/gauge_elem.gen.go -pkg=aggregator gen \"timedAggregation=timedGauge lockedAggregation=*lockedGauge typeSpecificElemBase=gaugeElemBase genericElemPool=GaugeElemPool GenericElem=GaugeElem\""

package generics
