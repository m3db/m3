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

// Instructions for adding a new field to an existing object in a backwards
// and forwards compatible way:
// 		1. Do not change the objects type
// 		2. Do not change the objects version
// 		3. Modify the encoder to encode the new fields
// 		4. Increase the constant below for the current (not minimum!) number
// 		   of fields for that object type
// 		5. Modify the decoder to selectively decode the new fields based on
// 		   the actual number of fields that are encoded in the file - See the
// 		   decodeIndexInfo function for an example
// 		6. Write forwards/backwards compatibility tests, below are examples:
// 				- TestIndexInfoRoundTripBackwardsCompatibilityV1
// 				- TestIndexInfoRoundTripForwardsCompatibilityV2

package msgpack

import "fmt"

const (
	// Incrementing any of these values is a backwards-compatible change
	// I.E the new binary will still be able to read old files (as long
	// as the minimum number of fields for a given object is not also
	// increased, see the comment below), but it is not a forwards-compatible
	// change I.E old binaries will not be able to read the new files and just
	// skip over any unrecognized fields (which they will do if this value is
	// not incremented when adding new fields)
	indexInfoVersion    = 1
	indexEntryVersion   = 1
	indexSummaryVersion = 1
	logInfoVersion      = 1
	logEntryVersion     = 1
	logMetadataVersion  = 1
)

type objectType int

// nolint: varcheck, unused
const (
	// Adding any new object types is a backwards-compatible change I.E
	// the new binary will still be able to read old files, but it is
	// not a forwards-compatible change I.E old binaries will not be
	// able to read the new files.
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

	// Total number of object types
	numObjectTypes = iota
)

const (
	// min number of fields specifies the minimum number of fields
	// that an object such that the encoder won't reject it. This value
	// should be equal to the number of fields in objects that were encoded
	// previosly that we still want new binaries to be able to read without
	// complaint. I.E if previous versions of M3DB wrote 4 fields for an object,
	// and an even earlier version only wrote 3 fields, than the minimum number
	// should be 3 if we intend to continue reading files that were written by
	// the version that only encoded 3 fields.
	minNumRootObjectFields           = 2
	minNumIndexInfoFields            = 6
	minNumIndexSummariesInfoFields   = 1
	minNumIndexBloomFilterInfoFields = 2
	minNumIndexEntryFields           = 5
	minNumIndexSummaryFields         = 3
	minNumLogInfoFields              = 3
	minNumLogEntryFields             = 7
	minNumLogMetadataFields          = 3

	// curr number of fields specifies the number of fields that the current
	// version of the M3DB will encode. This is used to ensure that the
	// correct number of fields is encoded into the files. These values need
	// to be incremened whenever we add new fields to an object.
	currNumRootObjectFields           = 2
	currNumIndexInfoFields            = 8
	currNumIndexSummariesInfoFields   = 1
	currNumIndexBloomFilterInfoFields = 2
	currNumIndexEntryFields           = 6
	currNumIndexSummaryFields         = 3
	currNumLogInfoFields              = 3
	currNumLogEntryFields             = 7
	currNumLogMetadataFields          = 3
)

var minNumObjectFields []int
var currNumObjectFields []int

func numFieldsForType(objType objectType) (min, curr int) {
	return minNumObjectFields[int(objType)-1], currNumObjectFields[int(objType)-1]
}

func setMinNumObjectFieldsForType(objType objectType, numFields int) {
	minNumObjectFields[int(objType)-1] = numFields
}

func setCurrNumObjectFieldsForType(objType objectType, numFields int) {
	currNumObjectFields[int(objType)-1] = numFields
}

func init() {
	minNumObjectFields = make([]int, int(numObjectTypes))
	currNumObjectFields = make([]int, int(numObjectTypes))

	setMinNumObjectFieldsForType(rootObjectType, minNumRootObjectFields)
	setMinNumObjectFieldsForType(indexInfoType, minNumIndexInfoFields)
	setMinNumObjectFieldsForType(indexSummariesInfoType, minNumIndexSummariesInfoFields)
	setMinNumObjectFieldsForType(indexBloomFilterInfoType, minNumIndexBloomFilterInfoFields)
	setMinNumObjectFieldsForType(indexEntryType, minNumIndexEntryFields)
	setMinNumObjectFieldsForType(indexSummaryType, minNumIndexSummaryFields)
	setMinNumObjectFieldsForType(logInfoType, minNumLogInfoFields)
	setMinNumObjectFieldsForType(logEntryType, minNumLogEntryFields)
	setMinNumObjectFieldsForType(logMetadataType, minNumLogMetadataFields)

	// Verify all current values are larger than their respective minimum values
	mustBeGreaterThanOrEqual(currNumRootObjectFields, minNumRootObjectFields)
	mustBeGreaterThanOrEqual(currNumIndexInfoFields, minNumIndexInfoFields)
	mustBeGreaterThanOrEqual(currNumIndexSummariesInfoFields, minNumIndexSummariesInfoFields)
	mustBeGreaterThanOrEqual(currNumIndexBloomFilterInfoFields, minNumIndexBloomFilterInfoFields)
	mustBeGreaterThanOrEqual(currNumIndexEntryFields, minNumIndexEntryFields)
	mustBeGreaterThanOrEqual(currNumIndexSummaryFields, minNumIndexSummaryFields)
	mustBeGreaterThanOrEqual(currNumLogInfoFields, minNumLogInfoFields)
	mustBeGreaterThanOrEqual(currNumLogEntryFields, minNumLogEntryFields)
	mustBeGreaterThanOrEqual(currNumLogMetadataFields, minNumLogMetadataFields)

	setCurrNumObjectFieldsForType(rootObjectType, currNumRootObjectFields)
	setCurrNumObjectFieldsForType(indexInfoType, currNumIndexInfoFields)
	setCurrNumObjectFieldsForType(indexSummariesInfoType, currNumIndexSummariesInfoFields)
	setCurrNumObjectFieldsForType(indexBloomFilterInfoType, currNumIndexBloomFilterInfoFields)
	setCurrNumObjectFieldsForType(indexEntryType, currNumIndexEntryFields)
	setCurrNumObjectFieldsForType(indexSummaryType, currNumIndexSummaryFields)
	setCurrNumObjectFieldsForType(logInfoType, currNumLogInfoFields)
	setCurrNumObjectFieldsForType(logEntryType, currNumLogEntryFields)
	setCurrNumObjectFieldsForType(logMetadataType, currNumLogMetadataFields)
}

func mustBeGreaterThanOrEqual(x, y int) {
	if x < y {
		panic(fmt.Sprintf("expected %d to be greater than or equal to %d", x, y))
	}
}
