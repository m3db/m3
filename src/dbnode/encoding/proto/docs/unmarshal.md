# Top Level Scalar Unmarshaler

## Overview

Its recommended that readers familiarize themselves with the [proto3 encoding documentation](https://developers.google.com/protocol-buffers/docs/encoding) before reading the remainder of this document.

The Encoder in this package is responsible for accepting a stream of marshaled Protobuf messages and encoding them into a new compressed stream.

In order to accomplish this, it needs to unmarshal the Protobuf messages so that it can re-encode their values.

Since the schemas for the Protobuf messages are provided dynamically (and thus efficient unmarshaling code can not be generated ahead of time) the easiest way to accomplish the unmarshaling is to rely on a dynamic Protobuf package like `jhump/protoreflect` to perform the heavy lfting.

This is inefficient primarily because unmarshaling into a `*dynamic.Message` is very expensive since it involves a lot of `interface{}` magic as well as allocating a large number of very short-lived objects that are difficult to reuse.
Its especially inefficient for Protobuf schemas that are optimized for this package (specifically those that make heavy use of top-level scalar fields where allocating objects on the heap just to wrap primitive types is especially wasteful).

As a result, this package implements a `customFieldUnmarshaler` that accepts a `[]byte` which constitutes a marshaled Protobuf message and exposes methods for unmarshaling the top-level scalar fields (I.E the fields that the encoder can perform custom compression on) in an efficient and reusable manner such that in the general case there are no allocations.
In addition, it uses the `jhump/protoreflect` library to unmarshal any other fields that it can't unmarshal efficiently (although this has zero overhead if those fields are not present).


## Implementation

### Overview

The implementation is broken into two parts:

1. The `Buffer` which is almost identical to https://github.com/jhump/protoreflect/blob/master/dynamic/codec.go and provides an interface for iterating a marshaled protobuf message one `<fieldNumber, wiretype, value>` tuple at a time.

2. The `customFieldUnmarshaler` which wraps the `Buffer` and exposes an interface for efficiently unmarshaling top-level scalar fields with no allocations, as well as a fallback mechanism that relies on the `jhump/protoreflect` library for any fields that don't have an efficient unmarshaling codepath (`maps`, `repeated` fields, and nested messages, etc.)

### Buffer

The code in the `Buffer` is mostly self explanatory for anyone familiar with [proto3 encoding format](https://developers.google.com/protocol-buffers/docs/encoding).

### CustomFieldUnmarshaler

The `customFieldUnmarshaler` has three primary responsibilities:

1. Provide an interface for efficiently unmarshaling top-level scalar fields in a marshaled Protobuf message without allocating.
2. Ensure that the values unmarshaled in #1 are sorted by field number.
3. Provide a `*dynamic.Message` that contains *only* the fields that could not be unmarshaled efficiently in #1 and does not allocate / expend any resources at all in the case of optimized schemas that don't contain any fields that can't be handled by #1.

The `customFieldUnmarshaler` works by iterating through all the <fieldNumber, wireType, value> tuples in the marshaled Protobuf and checking if they are supported by the efficient code path.
If they are, then it unmarshals the value into an `unmarshalValue` which is a space-optimized type that can be re-used without any allocations.
If the tuple cannot be unmarshaled efficiently then the unmarshaler falls back to the `jhump/protoreflect` library and uses the `UnmarshalMerge` method to unmarshal a single field at a time so that it can be used iteratively without having to rewrite the marshaled message even if custom and non-custom fields are interleaved together.

The end result of the unmarshaling process is that the caller will get a sorted (by field number) slice of `unmarshalValue` (and the slice is reused over and over again so the amortized allocation cost per unmarshal approaches zero) which contains all the values that can be custom encoded and a `*dynamic.Message` that contains all the more complex fields (if any) that cannot be unmarshaled or compressed efficiently.

Note that the `customFieldUnmarshaler` only returns an `unmarshalValue` for fields that were actually encoded into the stream. According to the [Proto3 encoding format](https://developers.google.com/protocol-buffers/docs/encoding) fields set to their default values are left out of the marshaled stream.
This means that callers need to account for the fact that if an `unmarshalValue` is not present for a field that should have had one then the implied value is the default value for that fields type.

