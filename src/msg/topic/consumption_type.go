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

package topic

import (
	"fmt"
	"strings"

	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
)

var (
	validTypes = []ConsumptionType{
		Shared,
		Replicated,
	}
)

func (t ConsumptionType) String() string {
	return string(t)
}

// NewConsumptionType creates a consumption type from a string.
func NewConsumptionType(str string) (ConsumptionType, error) {
	var validTypeStrings []string
	for _, t := range validTypes {
		validStr := t.String()
		if validStr == str {
			return t, nil
		}
		validTypeStrings = append(validTypeStrings, validStr)
	}
	return Unknown, fmt.Errorf(
		"invalid consumption type '%s', valid types are: %s", str, strings.Join(validTypeStrings, ", "),
	)
}

// NewConsumptionTypeFromProto creates ConsumptionType from a proto.
func NewConsumptionTypeFromProto(ct topicpb.ConsumptionType) (ConsumptionType, error) {
	switch ct {
	case topicpb.ConsumptionType_SHARED:
		return Shared, nil
	case topicpb.ConsumptionType_REPLICATED:
		return Replicated, nil
	}
	return Unknown, fmt.Errorf("invalid consumption type in protobuf: %v", ct)
}

// ConsumptionTypeToProto creates proto from a ConsumptionType.
func ConsumptionTypeToProto(ct ConsumptionType) (topicpb.ConsumptionType, error) {
	switch ct {
	case Shared:
		return topicpb.ConsumptionType_SHARED, nil
	case Replicated:
		return topicpb.ConsumptionType_REPLICATED, nil
	}
	return topicpb.ConsumptionType_UNKNOWN, fmt.Errorf("invalid consumption type: %v", ct)
}
