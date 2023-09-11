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

package xhttp

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

const (
	// HeaderContentType is the HTTP Content Type header.
	HeaderContentType = "Content-Type"

	// ContentTypeJSON is the Content-Type value for a JSON response.
	ContentTypeJSON = "application/json"

	// ContentTypeFormURLEncoded is the Content-Type value for a URL-encoded form.
	ContentTypeFormURLEncoded = "application/x-www-form-urlencoded"

	// ContentTypeHTMLUTF8 is the Content-Type value for UTF8-encoded HTML.
	ContentTypeHTMLUTF8 = "text/html; charset=utf-8"

	// ContentTypeProtobuf is the Content-Type value for a Protobuf message.
	ContentTypeProtobuf = "application/x-protobuf"

	// ContentTypeOctetStream is the Content-Type value for binary data.
	ContentTypeOctetStream = "application/octet-stream"
)

// WriteJSONResponse writes generic data to the ResponseWriter
func WriteJSONResponse(w http.ResponseWriter, data interface{}, logger *zap.Logger) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("unable to marshal json", zap.Error(err))
		WriteError(w, err)
		return
	}

	w.Header().Set(HeaderContentType, ContentTypeJSON)
	w.Write(jsonData)
}

// WriteProtoMsgJSONResponse writes a protobuf message to the ResponseWriter. This uses jsonpb
// for json marshalling, which encodes fields with default values, even with the omitempty tag.
func WriteProtoMsgJSONResponse(w http.ResponseWriter, data proto.Message, logger *zap.Logger) {
	marshaler := jsonpb.Marshaler{EmitDefaults: true}

	w.Header().Set(HeaderContentType, ContentTypeJSON)
	err := marshaler.Marshal(w, data)
	if err != nil {
		logger.Error("unable to marshal json", zap.Error(err))
		WriteError(w, err)
	}
}

// WriteUninitializedResponse writes a protobuf message to the ResponseWriter
func WriteUninitializedResponse(w http.ResponseWriter, logger *zap.Logger) {
	logger.Warn("request received before M3DB session is initialized")
	err := NewError(errors.New("connection to M3DB not yet initialized"),
		http.StatusServiceUnavailable)
	WriteError(w, err)
}
