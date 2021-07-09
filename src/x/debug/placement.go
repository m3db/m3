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

package debug

import (
	"fmt"
	"io"
	"net/http"

	"github.com/m3db/m3/src/cluster/placementhandler"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gogo/protobuf/jsonpb"
)

type placementInfoSource struct {
	getHandler *placementhandler.GetHandler
	service    handleroptions.ServiceNameAndDefaults
}

// NewPlacementInfoSource returns a Source for placement information.
func NewPlacementInfoSource(
	service handleroptions.ServiceNameAndDefaults,
	placementOpts placementhandler.HandlerOptions,
	iopts instrument.Options,
) (Source, error) {
	handler := placementhandler.NewGetHandler(placementOpts)
	return &placementInfoSource{
		getHandler: handler,
		service:    service,
	}, nil
}

// Write fetches data about the placement and writes it in the given writer.
// The data is formatted in json.
func (p *placementInfoSource) Write(w io.Writer, httpReq *http.Request) error {
	placement, err := p.getHandler.Get(p.service, httpReq)
	if err != nil {
		return err
	}

	placementProto, err := placement.Proto()
	if err != nil {
		return fmt.Errorf("unable to get placement protobuf: %v", err)
	}

	resp := &admin.PlacementGetResponse{
		Placement: placementProto,
		Version:   int32(placement.Version()),
	}

	marshaler := jsonpb.Marshaler{EmitDefaults: true}
	return marshaler.Marshal(w, resp)
}
