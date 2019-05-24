# Top Level Scalar Unmarshaller

## Overview

It's recommended that readers familiarize themselves with the [proto3 encoding documentation](https://developers.google.com/protocol-buffers/docs/encoding) before reading the remainder of this document.

The Encoder in this package is responsible for encoding an unbuffered stream of marshalled Protobuf messages into a new compressed stream one message at a time.

In order to accomplish this, it needs to unmarshal the Protobuf messages so that it can re-encode their values.

Since the schemas for the Protobuf messages are provided dynamically (and thus efficient unmarshalling code can not be generated ahead of time) the easiest way to accomplish the unmarshalling is to rely on a dynamic Protobuf package like `jhump/protoreflect` to perform the heavy lfting.

For M3DB, a solution like this is prohibitively inefficient: unmarshalling into a `*dynamic.Message` is expensive, as it involves `interface{}` magic and allocates a large number of short-lived objects that are difficult to reuse.
It's especially inefficient for Protobuf schemas that are optimized for this package (specifically those that make heavy use of top-level scalar fields where allocating objects on the heap just to wrap primitive types is particularly wasteful).

As a result, this package implements `customFieldUnmarshaller`, which accepts a marshalled Protobuf message (`[]byte`) and exposes methods for unmarshalling the top-level scalar fields (i.e the fields that the encoder can perform custom compression on) in an efficient and reusable manner such that in the general case there are no allocations.
In addition, it also exposes a `nonCustomFieldValues` method which returns **the marshalled** bytes for every field non top-level scalar field that the unmarshaler couldn't unmarshal.

## Implementation

### Overview

The implementation is broken into two parts:

1. The `buffer`, which is similar to [protoreflect's dynamic codex](https://github.com/jhump/protoreflect/blob/master/dynamic/codec.go) and provides an interface for iterating over a marshalled Protobuf message, one `<fieldNumber, wiretype, value>` tuple at a time.

2. The `customFieldUnmarshaller`, which wraps the `buffer` and exposes an interface for efficiently unmarshalling top-level scalar fields with no allocations, as well as returning the marshalled bytes for any fields that the unmarshaller doesn't have an efficient unmarshalling codepath for (`maps`, `repeated` fields, and nested messages, etc).

### Buffer

The code in the `buffer` is mostly self explanatory for anyone familiar with the [proto3 encoding format](https://developers.google.com/protocol-buffers/docs/encoding).

### CustomFieldUnmarshaller

The `customFieldUnmarshaller` has three primary responsibilities:

1. Provide an interface for efficiently unmarshalling top-level scalar fields in a marshalled Protobuf message without allocating.
2. Ensure that the values unmarshalled in #1 are sorted by field number.
3. Return a slice of `marshalledField` that contains *only* the fields that could not be unmarshalled efficiently in #1. This does not allocate / expend any resources at all in the case of optimized schemas that only contain fields that can be handled by #1.

The `customFieldUnmarshaller` works by iterating through all the `<fieldNumber, wireType, value>` tuples in the marshalled Protobuf and checking if they are supported by the efficient code path.
If they are, it unmarshals the value into an `unmarshalValue` which is a space-optimized type that can be reused without any allocations.
If the tuple cannot be unmarshalled efficiently, the unmarshaller keeps track of the bytes that represent that tuple and will return them as part of the (sorted) `[]marshalledField` when unmarshalling is complete. This approach supports both any combination of custom and non-custom fields, and in any order.

The output of unmarshalling is a slice of `unmarshalValue`s (sorted by field number) containing all custom-encoded values and a `[]marshalledField` containing any complex fields that cannot be unmarshalled or compressed efficiently. This value slice is reused to mitigate allocation costs for subsequent unmarshalling.

Note that the `customFieldUnmarshaller` only returns an `unmarshalValue` for fields that were actually encoded into the stream. According to the [Proto3 encoding format](https://developers.google.com/protocol-buffers/docs/encoding), fields set to their default values are omitted from the marshalled stream.
Thus, if an `unmarshalValue` is not present for a field (and the given field would nominally unmarshal to an `unmarshalValue`), then that value is the type's default value.

