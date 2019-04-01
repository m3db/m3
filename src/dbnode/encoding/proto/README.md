# Protobuf Encoding

## Overview

<!-- TODO somewhere talk about how the physical encoding can be thought of an interleaving of several different logical streams -->

This package contains the encoder/decoder(iterator) for compressing streams of ProtoBuf messages matching a provided schema. All compression is performed in a streaming manner

## Features

1. Compress streams of ProtoBuf messages matching a provided schema by using different forms of compression for each field based on its type.
2. Compress the timestamps of the ProtoBuf messages using [Gorilla style delta of delta encoding](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
3. Support changing the schema of a ProtoBuf message mid-stream (pending).

## Supported Syntax

This package strives to support compressing any ProtoBuf messages specified using syntax version 3, however, only the following have been tested:

1. All [scalar value type](https://developers.google.com/protocol-buffers/docs/proto3#scalar)
2. Nested Messages
3. Repeated fields
4. Map fields
5. Reserved fields

We have explicitly not performed any testing of the following features:

1. `Any` fields are not supported
2. `Oneof` fields are not supported
3. `Options` are not supported

## Compression Limitations

This package will attempt to compress all scalar type fields at the top level of a message, but will not compress any data that is part of a `repeated`, `map` or `nested message` field. The `nested message` restriction may be lifted in the future, but the `repeated` and `map` restrictions are unlikely to change.

## Compression Techniques

1. The timestamps for all of the ProtoBuf messages are compressed using [Gorilla style delta of delta encoding](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
2. Float fields are compressed using [Gorilla style XOR compression](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
3. Integer fields are compressed using M3TSZ significant digit integer compression. We don't currently have any documentation on this compression format.
4. `bytes` and `string` fields are compressed using a custom dictionary based compression scheme which we will refer to as "LRU Dictionary Compression".

### LRU Dictionary Compression

The LRU dictionary compression scheme provides high levels of compression for `bytes` and `string` fields that match any of the following criteria:

1. The value of the field changes infrequently.
2. The value of the field changes frequently, but tends to rotate among a small number of frequently used values.

Similar to `LZ77` and its variants, this compression strategy has an implicit assumption that patterns in the input data occur close together. Data stream that don't satisfy this assumption will compress poorly.

In the future, we may replace this simple algorithm with a more sophisticated dictionary compression scheme such as `LZ77`, `LZ78`, `LZW` (and its variants like `LZMW` and `LZAP`).

#### Algorithm

The encoder maintains a list of recently encoded strings in a per-field LRU. Everytime the encoder is about to encode a new string, it first checks if the string is in the LRU.

If the string *is not* in the LRU, then it encodes the string in its entirety and adds it to the LRU (evicting the least recently encoded string if necessary).

If the string *is* in the LRU, then it encodes the **index** of the string in the LRU which requires much less space. For example, it only takes 2 bits to encode the position of the string in an LRU with a maximum capacity of 4 strings.

For example, imagine compressing the following sequence of strings: ["foo", "bar", "baz", "bar"] with an LRU of size 2. The steps below describe what is written into the stream, as well as what the state of the LRU is at the end:

1. Write the full string "foo" into the stream and add "foo" to the LRU -> ["foo"]
2. Write the full string "bar" into the stream and add "bar" to the LRU -> ["foo", "bar"]
3. Write the full string "baz" into the stream and and evict "foo" from the LRU -> ["bar", "baz"]
4. Encode index 0 in the stream with a single bit (representing the string "bar") and update the LRU -> ["baz", "bar"]

This compression scheme works because the decoder can maintain its own LRU (of the same maximum capacity) and apply the same operations in the same order when its decompressing the stream. As a result, when it encounters an encoded LRU index it can lookup the corresponding string in its own LRU at the same index.

##### Encoding

The LRU dictionary compression scheme uses 3 control bits to encode all the relevant information required to decode the stream.

The first control bit is the "no change" control bit. If this is set to `1` then the value has not changed since its previous value and no further encoding/decoding is require.

The second control bit determines how we will interpret the subsequent bits. If its `0` then we interpret the next `n` bits (where `n` is the number of bits required to encode the highest index of the configured LRU capacity) as the index into the LRU of recently encoded / decoded strings which this value corresponds to.

If the second control bit is set to `1` then the remainder should be interpreted as a `varint` encoding the length of the `bytes` followed by the `bytes` themselves.

## Binary Format

### Header

Every compressed stream begins with a header which includes the following information:

1. encoding scheme version (`varint`)
2. dictionary compression LRU size (`varint`)
3. a list of "custom" encoded fields with their type and field numbers

#3 can be thought of as a list of <fieldNum, fieldType> and is encoded as follows:

1. highest field number (n) that will be described (`varint`)
2. n sets of 3 bits where each combination of three bits corresponds to the "custom type" which is enough information to determine how the field should be compressed / decompressed.

The list only explicitly encodes the custom field type. The Protobuf field number is encoded implicitly by the position of the entry in the list. In other words, the list of custom encoded fields can be thought of as a bitset except instead of using a single bit to encode the value at a given position, we use 3.

For example, if we had the following Protobuf schema:

```protobuf
message Foo {
	reserved 2, 3;
	string query = 1;
	int32 page_number = 4;
}
```

We would begin by encoding `4` as a `varint` since that is the highest non-reserved field number.

Next, we would begin encoding the field numbers and their field types 3 bits at a time where the field number is implied from their position in the list (starting at index 1) and the type is encoded in the 3 bit combination:

`string query = 1;` is encoded as the first value (indicating field number 1) and with the bit combination `111` indicating that it should be treated as `bytes` for compression purposes.

Next, we would encode `000` twice to indicate that we will not be performing any custom compression for field numbers `2` and `3` since they are reserved.

Finally, we'll encode `010` as the fourth item to indicate that field number `4` will be treated as a signed 32 bit integer.

#### Custom Types

0. Not custom encoded - This type means that we won't apply any custom compression to this field and we'll rely on the standard Protobuf marshaling to encode it.
1. Signed 64 bit integer (`int64`, `sint64`)
2. Signed 32 bit integer (`int32`, `sint32`, `enum`)
3. Unsigned 64 bit integer (`uint64`. `fixed64`)
4. Unsigned 32 bit integer (`uint32`, `fixed32`)
5. 64 bit float (`double`)
6. 32 bit float (`float`)
7. bytes (`bytes`, `string`)

### Stream of Writes

After encoding the header, the stream becomes a list of tuples in the form of <control bit, compressed timestamp, compressed protobuf fields>

#### Control Bit

Every write is prefixed with a control bit.

If the control bit is set to `1` then we know that the stream contains another write that we need to decode and we can begin decoding the timestamp.

If the control bit is set to `0` then we have either: a) reached the end of the stream **or** b) encountered a time unit change.

We resolve the ambiguity by reading the next control bit which will be `0` if we've reached the end of the stream or `1` if we should interpret the next 8 bits as the new time unit that we should use for delta of delta timestamp encoding.

We have to

#### Compressed Timestamp

The Protobuf compression scheme reuses the delta of delta timestamp encoding logic that is implemented in the M3TSZ package and decribed in the [Facebook Gorilla paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).


After encoding a control bit with a value of `1` indicating that there is another write in the stream, we encode the delta of delta of the current timestamp and the previous one as described in the Gorilla paper.

Similarly, when we're decoding the stream, we perform the inverse operation and reconstruct the current timestamp based on the previous one and the delta of delta encoded into the stream.

#### Compressed Protobuf Fields

Compressing the protobuf fields is broken into two stages:

1. Custom compressed fields
2. Protobuf marshaled fields

In the first phase we compress any fields for which we're able to apply custom compression as described in the `Compression Limitations` and `Compression Techniques` section.

In the second phase we lean on the Protobuf marshaling format to encode the data for us with the caveat that we compare fields at the top-level and avoid re-encoding them if they haven't changed.

##### Custom Compressed Protobuf Fields

We encode the custom compressed field values similarly how to we encode their types as described in the `Header` section. In fact, they're even encoded in the same order with the caveat that unlike when we're encoding the types, we don't need to encode a null value for non-contiguous field numbers for which we're not performing any compression.

All of the compressed value are encoded one after the other with no separator or control bit inbetween which means that they all need to encode enough information that a decoder can determine where each one ends and the next one begins.

The values themselves are encoded based on the field type and the compression technique that is being applied to it. For example, if we return to our sample Protobuf message from earlier:

```protobuf
message Foo {
	reserved 2, 3;
	string query = 1;
	int32 page_number = 4;
}
```

If we had never seen the `query` `string` being encoded before, we would encode a single control bit indicating that the value had changed since its previous value, another control bit indicating that it was not found in the LRU, followed by a `varint` for the length of the string, and then the `string` bytes themselves.

Next, we would use 6 bits to encode the number of significant digits in the delta between current `page_number` and the previous `page_number`, followed by a control bit indicating if the delta is positive or negative, and then finally the significant bits themselves.

Note that the values we encoded for both fields are "self contained" in that they encode all the information required to determine when we've reached the end.

##### Protobuf Marshaled Fields

We recommend reading the [Protocol Buffers Encoding](https://developers.google.com/protocol-buffers/docs/encoding) section of the official documentation before reading this section. Specifically, understanding how protobuf messages are (basically) encoded as a stream of tuples in the form of <field number, wire type, value> will make understanding this section much easier.

The Protobuf marshaled fields section of the encoding scheme contains all the values that we don't currently support performing custom compression on. For the most part, the output of this section is similar to the result of calling `Marshal()` on a message in which all the custom compressed fields have already been removed and all that remains is the fields for which we wish to rely upon the Protobuf logic for encoding. This is possible because, as described in the protobuf encoding section linked above, the protobuf wire format does not encode **any** data for fields which are not set or are set to a default value, so by "clearing" the fields that we've already encoded on our own, we can prevent them from taking up any space when we marshal the remainder of the Protobuf message.

While we do lean heavily on the Protobuf wire format in this section, we do make the most basic optimization of avoiding re-encoding fields that haven't changed since the previous value where "haven't changed" is defined at the top most level of the message. For example, lets imagine we were trying to encode messages with the following schema:

```protobuf
message Outer {
  message Nested
    message NestedDeeper {
      int64 ival = 1;
      bool  booly = 2;
    }
		int64 outer = 1;
		NestedDeeper deeper = 2;
  }

	Nested nested = 1;
}
```

If none of the values inside nested have changed since the previous message, we don't need to encode the `Nested` field at all. However, if any of the fields have changed, like `nested.deeper.booly` for example, then we need to re-encode the entire `nested` field, including the `outer` field even though only the `deeper` field changed.

We can perform this top-level delta encoding because when we're decoding the stream later, we can reconstruct the original message by merging the previously decoded message with the current "delta" message that only contains the fields that have changed since the previous message.

Only marshaling the fields that have changed since the previous message works for the most part, but there is one important edge case. As we said earlier, the protobuf wire format does not encode **any** data for fields that are set to a default value (zero for integers and floats, empty array for `bytes` and strings, etc).