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
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// PromParseURL is the url for native prom parse handler, this parses out the
	// query and returns a JSON representation of the execution DAG.
	PromParseURL = route.Prefix + "/parse"

	// PromParseHTTPMethod is the HTTP method used with this resource.
	PromParseHTTPMethod = http.MethodGet
)

// PromParseHandler represents a handler for prometheus parse endpoint.
type promParseHandler struct {
	engine         executor.Engine
	instrumentOpts instrument.Options
}

// NewPromParseHandler returns a new instance of handler.
func NewPromParseHandler(opts options.HandlerOptions) http.Handler {
	return &promParseHandler{
		engine:         opts.Engine(),
		instrumentOpts: opts.InstrumentOpts(),
	}
}

// FunctionNode is a JSON representation of a function.
type FunctionNode struct {
	// Name is the name of the function node.
	Name string `json:"name,omitempty"`
	// Children are any children this function node has.
	Children []FunctionNode `json:"children,omitempty"`
	// parents are any parents this node has; this is private since it is only
	// used to construct the function node map.
	parents []FunctionNode
	// node is the actual execution node.
	node parser.Node
}

// String prints the string representation of a function node.
func (n FunctionNode) String() string {
	var sb strings.Builder
	sb.WriteString(n.Name)
	if len(n.Children) > 0 {
		sb.WriteString(": ")
		sb.WriteString(fmt.Sprint(n.Children))
	}

	return sb.String()
}

type nodeMap map[string]FunctionNode

func (n nodeMap) rootNode() (FunctionNode, error) {
	var (
		found bool
		node  FunctionNode
	)

	for _, v := range n {
		if len(v.parents) == 0 {
			if found {
				return node, fmt.Errorf(
					"multiple roots found for map: %s",
					fmt.Sprint(n),
				)
			}

			found = true
			node = v
		}
	}

	if !found {
		return node, fmt.Errorf("no root found for map: %s", fmt.Sprint(n))
	}

	return node, nil
}

func constructNodeMap(nodes parser.Nodes, edges parser.Edges) (nodeMap, error) {
	nodeMap := make(map[string]FunctionNode, len(nodes))
	for _, node := range nodes {
		name := node.Op.OpType()
		nodeMap[string(node.ID)] = FunctionNode{
			Name: name,
			node: node,
		}
	}

	for _, edge := range edges {
		// NB: due to how execution occurs, `parent` and `child` have the opposite
		// meaning in the `edge` context compared to the expected lexical reading,
		// so they should be swapped here.
		parent := string(edge.ChildID)
		child := string(edge.ParentID)
		parentNode, ok := nodeMap[parent]
		if !ok {
			return nil, fmt.Errorf("parent node with ID %s not found", parent)
		}

		childNode, ok := nodeMap[child]
		if !ok {
			return nil, fmt.Errorf("child node with ID %s not found", child)
		}

		parentNode.Children = append(parentNode.Children, childNode)
		childNode.parents = append(childNode.parents, parentNode)
		nodeMap[parent] = parentNode
		nodeMap[child] = childNode
	}

	return nodeMap, nil
}

func parseRootNode(
	r *http.Request,
	engine executor.Engine,
	logger *zap.Logger,
) (FunctionNode, error) {
	query, err := ParseQuery(r)
	if err != nil {
		logger.Error("cannot parse query string", zap.Error(err))
		return FunctionNode{}, err
	}

	parseOpts := engine.Options().ParseOptions()
	parser, err := promql.Parse(query, time.Second,
		models.NewTagOptions(), parseOpts)
	if err != nil {
		logger.Error("cannot parse query PromQL", zap.Error(err))
		return FunctionNode{}, err
	}

	nodes, edges, err := parser.DAG()
	if err != nil {
		logger.Error("cannot extract query DAG", zap.Error(err))
		return FunctionNode{}, err
	}

	funcMap, err := constructNodeMap(nodes, edges)
	if err != nil {
		logger.Error("cannot construct node map", zap.Error(err))
		return FunctionNode{}, err
	}

	root, err := funcMap.rootNode()
	if err != nil {
		logger.Error("cannot fetch root node", zap.Error(err))
		return FunctionNode{}, err
	}

	return root, nil
}

func (h *promParseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := h.instrumentOpts.Logger()
	root, err := parseRootNode(r, h.engine, logger)
	if err != nil {
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
		return
	}

	xhttp.WriteJSONResponse(w, root, logger)
}
