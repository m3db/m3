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

package serialize

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/leanovate/gopter/gen"
)

func TestDecoderLifecycle(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 300
	properties := gopter.NewProperties(parameters)

	comms := decoderCommandsFunctor(t)
	properties.Property("Decoder Lifecycle Invariants", commands.Prop(comms))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Fail()
	}
}

var decoderCommandsFunctor = func(t *testing.T) *commands.ProtoCommands {
	return &commands.ProtoCommands{
		NewSystemUnderTestFunc: func(initialState commands.State) commands.SystemUnderTest {
			sut := initialState.(*decoderState)
			d := newTestTagDecoder()
			d.Reset(sut.initBytes)
			if err := d.Err(); err != nil {
				panic(err)
			}
			return &systemUnderTest{
				dec: d,
			}
		},
		DestroySystemUnderTestFunc: func(s commands.SystemUnderTest) {
			sys := s.(*systemUnderTest)
			sys.dec.Finalize()
			for _, dec := range sys.clones {
				dec.Finalize()
			}
		},
		InitialStateGen: newDecoderState(),
		InitialPreConditionFunc: func(s commands.State) bool {
			return s != nil
		},
		GenCommandFunc: func(state commands.State) gopter.Gen {
			return gen.OneGenOf(
				gen.Const(nextCmd),
				gen.Const(errCmd),
				gen.Const(remainingCmd),
				gen.Const(duplicateCmd),
				// gen.Const(duplicateAndSwapSystemCmd),
			)
		},
	}
}

type decoderState struct {
	tags              ident.Tags
	nextHasBeenCalled bool
	idx               int
	initBytes         checked.Bytes
	numRefs           int
}

func (d *decoderState) String() string {
	var tagBuffer bytes.Buffer
	for i, t := range d.tags {
		if i != 0 {
			tagBuffer.WriteString(", ")
		}
		tagBuffer.WriteString(t.Name.String())
		tagBuffer.WriteString("=")
		tagBuffer.WriteString(t.Value.String())
	}

	return fmt.Sprintf("[ nextCalled=%v, idx=%d, numRefs=%d, tags=[%s] ]",
		d.nextHasBeenCalled, d.idx, d.numRefs, tagBuffer.String())
}

func (d *decoderState) numRemaining() int {
	if !d.nextHasBeenCalled {
		return len(d.tags)
	}
	remain := len(d.tags) - (d.idx + 1)
	if remain >= 0 {
		return remain
	}
	return 0
}

func newDecoderState() gopter.Gen {
	return anyAsciiTags().Map(
		func(tags ident.Tags) *decoderState {
			enc := newTestTagEncoder()
			if err := enc.Encode(ident.NewTagSliceIterator(tags)); err != nil {
				return nil
			}

			b, ok := enc.Data()
			if !ok {
				return nil
			}

			data := checked.NewBytes(b.Get(), nil)
			return &decoderState{
				tags:      tags,
				initBytes: data,
				numRefs:   1,
			}
		},
	)
}

type systemUnderTest struct {
	dec    TagDecoder
	clones []TagDecoder
}

type systemAndResult struct {
	system *systemUnderTest
	result commands.Result
}

var errCmd = &commands.ProtoCommand{
	Name: "Err",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		sys := s.(*systemUnderTest)
		d := sys.dec
		return &systemAndResult{
			system: sys,
			result: d.Err(),
		}
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		res := result.(*systemAndResult)
		if res.result == nil {
			return &gopter.PropResult{Status: gopter.PropTrue}
		}

		err := res.result.(error)
		return &gopter.PropResult{
			Status: gopter.PropError,
			Error:  fmt.Errorf("received error [ err = %v, state = [%s] ]", err, state.(*decoderState)),
		}
	},
}

var nextCmd = &commands.ProtoCommand{
	Name: "Next",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		sys := s.(*systemUnderTest)
		d := sys.dec
		return &systemAndResult{
			system: sys,
			result: d.Next(),
		}
	},
	NextStateFunc: func(state commands.State) commands.State {
		s := state.(*decoderState)
		if !s.nextHasBeenCalled {
			s.nextHasBeenCalled = true
			if len(s.tags) > 0 {
				// i.e. only increment tag references the first time we allocate
				s.numRefs += 2 // tagName & tagValue
			}
		} else {
			s.idx++
		}
		// when we have gone past the end, remove references to tagName/tagValue
		if s.numRemaining() == -1 {
			if len(s.tags) > 0 {
				s.numRefs -= 2
			}
		}
		return s
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		res := result.(*systemAndResult)
		decState := state.(*decoderState)
		if decState.numRemaining() > 0 && !res.result.(bool) {
			// ensure we were told the correct value for Next()
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error:  fmt.Errorf("received invalid Next()"),
			}
		}
		// ensure hold the correct number of references for underlying bytes
		dec := res.system.dec.(*decoder)
		if dec.checkedData.NumRef() != decState.numRefs {
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error: fmt.Errorf(
					"received invalid number of refs [ expected = %d observed = %d state = [%s] ]",
					decState.numRefs, dec.checkedData.NumRef(), decState),
			}
		}
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
}

var remainingCmd = &commands.ProtoCommand{
	Name: "Remaining",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		sys := s.(*systemUnderTest)
		d := sys.dec
		return &systemAndResult{
			system: sys,
			result: d.Remaining(),
		}
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		decState := state.(*decoderState)
		remain := result.(*systemAndResult).result.(int)
		if remain != decState.numRemaining() {
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error: fmt.Errorf("received invalid Remain [ expected=%d, observed=%d ]",
					decState.numRemaining(), remain),
			}
		}
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
}

var duplicateCmd = &commands.ProtoCommand{
	Name: "Duplicate",
	RunFunc: func(s commands.SystemUnderTest) commands.Result {
		sys := s.(*systemUnderTest)
		d := sys.dec
		dupe := d.Duplicate().(TagDecoder)
		sys.clones = append(sys.clones, dupe)
		return &systemAndResult{
			system: sys,
			result: dupe,
		}
	},
	NextStateFunc: func(state commands.State) commands.State {
		s := state.(*decoderState)
		if s.numRefs > 0 {
			// i.e. we have a checked bytes still present, so we
			// atleast make another reference to it.
			s.numRefs++
		}
		// if we have any current tags, we should inc ref by 2 because of it
		if s.nextHasBeenCalled && s.numRemaining() >= 0 && len(s.tags) > 0 {
			s.numRefs += 2
		}
		return s
	},
	PostConditionFunc: func(state commands.State, result commands.Result) *gopter.PropResult {
		decState := state.(*decoderState)
		dec := result.(*systemAndResult).system.dec.(*decoder)
		// ensure hold the correct number of references for underlying bytes
		if dec.checkedData.NumRef() != decState.numRefs {
			return &gopter.PropResult{
				Status: gopter.PropError,
				Error: fmt.Errorf(
					"received invalid number of refs [ expected = %d observed = %d state = [%s] ]",
					decState.numRefs, dec.checkedData.NumRef(), decState),
			}
		}
		return &gopter.PropResult{Status: gopter.PropTrue}
	},
}

// Close
// Duplicate
// Finalize

func anyAsciiTag() gopter.Gen {
	return gopter.CombineGens(gen.Identifier(), gen.Identifier()).
		Map(func(values []interface{}) ident.Tag {
			name := values[0].(string)
			value := values[1].(string)
			return ident.StringTag(name, value)
		})
}

func anyAsciiTags() gopter.Gen { return gen.SliceOf(anyAsciiTag()) }
