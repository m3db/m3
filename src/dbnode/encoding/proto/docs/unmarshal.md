# Top Level Scalar Unmarshaler

## Overview

Its recommended that readers familiarize themselves with the [proto3 encoding documentation](https://developers.google.com/protocol-buffers/docs/encoding) before reading the remainder of this document.

The Encoder in this package is responsible for accepting a stream of marshaled Protobuf messages and encoding them into a new compressed stream.

In order to accomplish this, it needs to unmarshal the Protobuf messages so that it can re-encode their values.

Since the schemas for the Protobuf messages are provided dynamically (and thus efficient unmarshaling code can not be generated ahead of time) the easiest way to accomplish the unmarshaling is to rely on a dynamic Protobuf package like `jhump/protoreflect` to perform the heavy lfting.

This is inefficient primarily because unmarshaling into a `*dynamic.Message` is very expensive since it involves a lot of `interface{}` magic as well as allocating a large number of very short-lived objects that are difficult to reuse. Its especially inefficient for Protobuf schemas that are optimized for this package (specifically those that make heavy use of top-level scalar fields).

As a result, this package implements a `topLevelScalarUnmarshaler` that accepts a `[]byte` which constitutes a marshaled Protobuf message and returns an iterator that can be used to iterate through the the fields of the marshaled Protobuf message one at a time ordered by field number.

This iterator is optimized such that top-level scalar fields can be iterated quickly and with zero allocations.

## Implementation

### Overview

The implementation is broken into two parts:

1. The `Buffer` which is almost identical to https://github.com/jhump/protoreflect/blob/master/dynamic/codec.go and provides an interface for iterating a marshaled protobuf message one `<fieldNumber, wiretype, value>` tuple at a time.

2. The `SortedUnmarshalIterator` which wraps the `Buffer` and exposes an iterator interface that returns concrete Go values for each field and ensures that iteration happens in sorted order based on the field number. This iterator only provides efficient iteration over top-level scalar fields in the message, and fallsback to the `jhump/protoreflect` library for any more complex fields (`maps`, `repeated` fields, and nested messages).

### Buffer

The code in the `Buffer` is mostly self explanatory for anyone familiar with [proto3 encoding format](https://developers.google.com/protocol-buffers/docs/encoding).

### SortedUnmarshalIterator

The `SortedUnmarshalIterator` has three primary responsibilities:

1. Provide an interface for efficiently iterating over top-level scalar fields in a marshaled Protobuf message without allocating.
2. Ensure that the iteration described in #1 occurs in sorted order based on field number.
3. Provide an unmarshaled `*dynamic.Message` that contains *only* the fields that could not be iterated efficiently in #1 and does not allocate / expend any resources at all in the case of optimized schemas that don't contain any fields that can't be handled by #1.

When the `SortedUnmarshalIterator` is provided with a new schema and marshaled Protobuf message it begins pre-processing the message by decoding all of the <fieldNumber, wireType> tuple and skipping over the values.

When the pre-processing is complete, the iterator will know the offsets for all of the field numbers it going to iterate over (and as such can do so in sorted order), as well as all the offsets for the fields that is going to "skip" over and return in the form of a `*dynamic.Message`