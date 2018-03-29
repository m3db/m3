// Copyright (c) 2016 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package msgpack

const (
	indexInfoVersion    = 1
	indexEntryVersion   = 1
	indexSummaryVersion = 1
	logInfoVersion      = 1
	logEntryVersion     = 1
	logMetadataVersion  = 1
)

type objectType int

// nolint: deadcode, varcheck, unused
const (
	unknownType objectType = iota
	rootObjectType
	indexInfoType
	indexSummariesInfoType
	indexBloomFilterInfoType
	indexEntryType
	indexSummaryType
	logInfoType
	logEntryType
	logMetadataType
	indexInfoTypeLegacyV1

	// Total number of object types
	numObjectTypes = iota
)

const (
	numRootObjectFields           = 2
	numIndexInfoFields            = 8
	numIndexInfoFieldsLegacyV1    = 6
	numIndexSummariesInfoFields   = 1
	numIndexBloomFilterInfoFields = 2
	numIndexEntryFields           = 5
	numIndexSummaryFields         = 3
	numLogInfoFields              = 3
	numLogEntryFields             = 7
	numLogMetadataFields          = 3
)

var numObjectFields []int

func numFieldsForType(objType objectType) int {
	return numObjectFields[int(objType)-1]
}

func setNumFieldsForType(objType objectType, numFields int) {
	numObjectFields[int(objType)-1] = numFields
}

func init() {
	numObjectFields = make([]int, int(numObjectTypes))

	setNumFieldsForType(rootObjectType, numRootObjectFields)
	setNumFieldsForType(indexInfoType, numIndexInfoFields)
	setNumFieldsForType(indexSummariesInfoType, numIndexSummariesInfoFields)
	setNumFieldsForType(indexBloomFilterInfoType, numIndexBloomFilterInfoFields)
	setNumFieldsForType(indexEntryType, numIndexEntryFields)
	setNumFieldsForType(indexSummaryType, numIndexSummaryFields)
	setNumFieldsForType(logInfoType, numLogInfoFields)
	setNumFieldsForType(logEntryType, numLogEntryFields)
	setNumFieldsForType(logMetadataType, numLogMetadataFields)
	setNumFieldsForType(indexInfoTypeLegacyV1, numIndexInfoFieldsLegacyV1)
}
