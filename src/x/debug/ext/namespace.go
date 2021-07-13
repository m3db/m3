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

package extdebug

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
)

type namespaceInfoSource struct {
	handler  *namespace.GetHandler
	defaults handleroptions.ServiceNameAndDefaults
}

// NewNamespaceInfoSource returns a Source for namespace information.
func NewNamespaceInfoSource(
	clusterClient clusterclient.Client,
	allDefaults []handleroptions.ServiceNameAndDefaults,
	instrumentOpts instrument.Options,
) (debug.Source, error) {
	var (
		m3dbDefault handleroptions.ServiceNameAndDefaults
		found       bool
	)
	for _, def := range allDefaults {
		if def.ServiceName == handleroptions.M3DBServiceName {
			m3dbDefault = def
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("could not find M3DB service in defaults: %v", allDefaults)
	}

	handler := namespace.NewGetHandler(clusterClient,
		instrumentOpts)

	return &namespaceInfoSource{
		handler:  handler,
		defaults: m3dbDefault,
	}, nil
}

// Write fetches data about the namespace and writes it in the given writer.
// The data is formatted in json.
func (n *namespaceInfoSource) Write(w io.Writer, r *http.Request) error {
	opts := handleroptions.NewServiceOptions(n.defaults, r.Header, nil)
	nsRegistry, err := n.handler.Get(opts)
	if err != nil {
		return err
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	marshaler := jsonpb.Marshaler{EmitDefaults: true}
	buf := new(bytes.Buffer)
	if err := marshaler.Marshal(buf, resp); err != nil {
		return err
	}

	toDuration, err := xhttp.NanosToDurationBytes(buf)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(toDuration)
	if err != nil {
		return err
	}

	_, err = w.Write(jsonData)
	if err != nil {
		return err
	}

	return nil
}
