// Copyright (c) 2019 Uber Technologies, Inc.
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

package native

import (
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/functions/scalar"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromThresholdURL is the url for native prom threshold handler, this  parses
	// out the  query and returns a JSON representation of the execution DAG.
	PromThresholdURL = route.PrefixV1 + "/threshold"

	// PromThresholdHTTPMethod is the HTTP method used with this resource.
	PromThresholdHTTPMethod = http.MethodGet
)

// PromThresholdHandler represents a handler for prometheus threshold endpoint.
type promThresholdHandler struct {
	engine         executor.Engine
	instrumentOpts instrument.Options
}

// NewPromThresholdHandler returns a new instance of handler.
func NewPromThresholdHandler(opts options.HandlerOptions) http.Handler {
	return &promThresholdHandler{
		engine:         opts.Engine(),
		instrumentOpts: opts.InstrumentOpts(),
	}
}

// QueryRepresentation is a JSON representation of a query after attempting to
// extract any threshold-specific parameters.
//
// NB: this is always presented with the threshold value (if present) as
// appearing on the right of the query.
// e.g. `1 > up` will instead be inverted to `up < 1`
type QueryRepresentation struct {
	// Query is the non-threshold part of a query.
	Query FunctionNode `json:"query,omitempty"`
	// Threshold is any detected threshold (top level comparison functions).
	// NB: this is a pointer so it does not have to be present in output if unset.
	Thresold *Threshold `json:"threshold,omitempty"`
}

// Threshold is a JSON representation of a threshold, represented by a top level
// comparison function.
type Threshold struct {
	// Comparator is the threshold comparator.
	Comparator string `json:"comparator"`
	// Value is the threshold value.
	Value float64 `json:"value"`
}

var thresholdNames = []string{">", ">=", "<", "<=", "==", "!="}

// NB: Thresholds are standardized to have the threshold value on the RHS;
// if the scalar is on the left, the operation must be inverted. Strict
// equalities remain untouched.
var invertedNames = []string{"<", "<=", ">", ">=", "==", "!="}

type thresholdInfo struct {
	thresholdIdx int
	queryIdx     int
	comparator   string
}

// isThreshold determines if this function node is a candidate for being a
// threshold function, and returns the index of the child representing the
// threshold, and the index of the child for the rest of the query.
func (n FunctionNode) isThreshold() (bool, thresholdInfo) {
	info := thresholdInfo{-1, -1, ""}
	// NB: only root nodes with two children may be thresholds.
	if len(n.parents) != 0 || len(n.Children) != 2 {
		return false, info
	}

	for i, name := range thresholdNames {
		if n.Name != name {
			continue
		}

		scalarLeft := n.Children[0].node.Op.OpType() == scalar.ScalarType
		scalarRight := n.Children[1].node.Op.OpType() == scalar.ScalarType

		// NB: if both sides are scalars, it's a calculator, not a query.
		if scalarLeft && scalarRight {
			return false, info
		}

		if scalarLeft {
			// NB: Thresholds are standardized to have the threshold value on the RHS;
			// if the scalar is on the left, the operation must be inverted.
			inverted := invertedNames[i]
			return true, thresholdInfo{
				thresholdIdx: 0,
				queryIdx:     1,
				comparator:   inverted,
			}
		}

		if scalarRight {
			return true, thresholdInfo{
				thresholdIdx: 1,
				queryIdx:     0,
				comparator:   name,
			}
		}

		break
	}

	return false, info
}

// QueryRepresentation gives the query representation of the function node.
func (n FunctionNode) QueryRepresentation() (QueryRepresentation, error) {
	isThreshold, thresholdInfo := n.isThreshold()
	if !isThreshold {
		return QueryRepresentation{
			Query: n,
		}, nil
	}

	queryPart := n.Children[thresholdInfo.queryIdx]
	thresholdPart := n.Children[thresholdInfo.thresholdIdx]
	thresholdOp := thresholdPart.node.Op
	scalarOp, ok := thresholdOp.(*scalar.ScalarOp)
	if !ok {
		return QueryRepresentation{}, instrument.InvariantErrorf(
			"operation %s must be a scalar", thresholdOp.String())
	}

	return QueryRepresentation{
		Query: queryPart,
		Thresold: &Threshold{
			Comparator: thresholdInfo.comparator,
			Value:      scalarOp.Value(),
		},
	}, nil
}

func (h *promThresholdHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := h.instrumentOpts.Logger()
	root, err := parseRootNode(r, h.engine, logger)
	if err != nil {
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
		return
	}

	queryRepresentation, err := root.QueryRepresentation()
	if err != nil {
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
		logger.Error("cannot convert to query representation", zap.Error(err))
		return
	}

	xhttp.WriteJSONResponse(w, queryRepresentation, logger)
}
