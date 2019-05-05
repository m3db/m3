# Protobuf Encoding

## Features

1. Lossless compression.
1. Compression of Protobuf message timestamps using [Gorilla-style delta-of-delta encoding](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
2. Compression of Protobuf message streams that match a provided schema by using different forms of compression for each field based on its type.
4. Changing Protobuf message schemas mid-stream (pending).

## Supported Syntax

While this package strives to support the entire [proto3 language spec](https://developers.google.com/protocol-buffers/docs/proto3), only the following features have been tested:

1. [Scalar values](https://developers.google.com/protocol-buffers/docs/proto3#scalar)
2. Nested messages
3. Repeated fields
4. Map fields
5. Reserved fields

The following have not been tested, and thus are not currently officially supported:

1. `Any` fields
2. [`Oneof` fields](https://developers.google.com/protocol-buffers/docs/proto#oneof)
3. Options of any type
4. Custom field types

## Compression Techniques

This package compresses the timestamps for the Protobuf messages using [Gorilla style delta-of-delta encoding](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

Additionally, each field is compressed using a different form of compression that is optimal for its type:

1. Floating point values are compressed using [Gorilla style XOR compression](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
2. Integer values (including fixed-width types) are compressed using M3TSZ Significant Digit Integer Compression (documentation forthcoming).
3. `bytes` and `string` values are compressed using LRU Dictionary Compression, which is described in further detail below.

### LRU Dictionary Compression

LRU Dictionary Compression is a compression scheme that provides high levels of compression for `bytes` and `string` fields that meet any of the following criteria:

1. The value of the field changes infrequently.
2. The value of the field changes frequently, but tends to rotate among a small number of frequently used values which may evolve over time.

For example, the stream: `["value1", "value2", "value3", "value4", "value5", "value6", ...]` will compress poorly, but the stream: `["value1", "value1", "value2", "value1", "value3", "value2", ...]` will compress well.

Similar to `LZ77` and its variants, this compression strategy has an implicit assumption that patterns in the input data occur close together. Data streams that don't satisfy this assumption will compress poorly.

In the future, we may replace this simple algorithm with a more sophisticated dictionary compression scheme such as `LZ77`, `LZ78`, `LZW` or variants like `LZMW` or `LZAP`.

#### Algorithm

The encoder maintains a list of recently encoded strings in a per-field LRU cache.
Everytime the encoder encounters a string it, it checks the cache first.

If the string *is not* in the cache, then it encodes the string in its entirety and adds it to the cache (evicting the least recently encoded string if necessary).

If the string *is* in the cache, then it encodes the **index** of the string in the cache which requires much less space.
For example, it only takes 2 bits to encode the position of the string in a cache with a maximum capacity of 4 strings.

For example, given a sequence of strings: `["foo", "bar", "baz", "bar"]` and an LRU cache of size 2; the algorithm performs the following operations:

1. Check the cache to see if it contains "foo", which it does not, and then write the full string "foo" into the stream and add "foo" to the cache -> `["foo"]`
2. Check the cache to see if it contains "bar", which it does not, and then write the full string "bar" into the stream and add "bar" to the cache -> `["foo", "bar"]`
3. Check the cache to see if it contains "baz", which it does not, and then write the full string "baz" into the stream and and evict "foo" from the cache -> `["bar", "baz"]`
4. Check the cache to see if it contains "bar", which it does, and then encode index 0 (because "bar" was at index 0 in the cache as of the end of step 3) into the stream with a single bit (which represents the string "bar" relative to the state of the cache at the end of step 3) and then update the cache to indicate that "bar" was the most recently encoded string -> `["baz", "bar"]`

This compression scheme works because the decoder can maintain an LRU cache (of the same maximum capacity) and apply the same operations in the same order when its decompressing the stream.
As a result, when it encounters an encoded cache index it can look up the corresponding string in its own LRU cache at the specified index.

##### Encoding

The LRU Dictionary Compression scheme uses 2 control bits to encode all the relevant information required to decode the stream. In order, they are:

1. **The "no change" control bit.** If this bit is set to `1`, the value is unchanged and no further encoding/decoding is required.
2. **The "size" control bit.** If this bit is set to `0`, the size of the LRU cache capacity (N) is used to determine the number of remaining bits that need to be read and interpreted as a cache index that holds the compressed value; otherwise, the remaining bits are treated as a variable-width `length` and corresponding `bytes` pairs. Importantly, if the beginning of the `bytes` sequences is not byte-aligned, it is padded with zeroes up to the next byte boundary. While this isn't a strict requirement of the encoding scheme (in fact, it slightly lowers the compression ratio), it greatly simplifies the implementation because the encoder needs to reference previously-encoded bytes in order to check if the bytes currently being encoded are cached. Alternatively, the encoder could keep track of all the bytes that correspond to each cache entry in memory, but that would be a wasteful use of memory. Instead, it's more efficient if the cache stores offsets into the encoded stream for the beginning and end of the bytes that have already been encoded. These offsets are much easier to track of and compare against if they can be assumed to always correspond to the beginning of a byte boundary. In the future the implementation may be changed to favor wasting fewer bits in exchange for more complex logic.

### Compression Limitations

While this compression applies to all scalar types at the top level of a message, it does not apply to any data that is part of `repeated` fields, `map` fields, or nested messages.
The `nested message` restriction may be lifted in the future, but the `repeated` and `map` restrictions are unlikely to change due to the difficulty of compressing variably sized fields.

## Binary Format

At a high level, compressing Protobuf messages consists of the following:

1. Scanning each schema to identify which fields we can be extracted and compressed as described in the "Compression Techniques" section.
2. Iterating over all fields (as well as the timestamp) for each messages as it arrives and encoding the next value for that field into its corresponding compressed stream.

In practice, a Protbuf message can have any number of different fields; for performance reasons, it's impractical to maintain an independent stream at the field level.
Instead, multiple "logical" streams are interleaved on a per-write basis within one physical stream.
The remainder of this section outlines the binary format used to accomplish this interleaving.

The binary format begins with a stream header and then the remainder of the stream is a sequence of tuples in the form: `<per-write header, compressed timestamp, compressed custom encoded fields, Protobuf marshaled fields>`

### Stream Header

Every compressed stream begins with a header which includes the following information:

1. encoding scheme version (`varint`)
2. dictionary compression LRU cache size (`varint`)

In the future the dictionary compression LRU cache size may be moved to the per-write control bits section so that it can be updated mid stream (as opposed to only being updateable at the beginning of a new stream).

### Per-Write Header

#### Per-Write Control Bits

Every write is prefixed with a header that contains at least one control bit.

If the control bit is set to `1`, then the stream contains another write that needs to be decoded, implying that the timestamp can be decoded as well.

If the control bit is set to `0`, then either **(a)** the end of the stream has been reached *or* **(b)** a time unit and/or schema change has been encountered.

This ambiguity is resolved by reading the next control bit, which will be `0` (if this is the end of the stream) or `1` if a time unit and/or schema change has been encountered.

If the control bit is `1` (meaning this is not the end of the stream), then the next two bits should be interpreted as boolean control bits indicating if there has been a time unit or schema change respectively.

Time unit changes must be tracked manually (instead of deferring to the M3TSZ timestamp delta-of-delta encoder, which can handle this independently) because the M3TSZ encoder relies on a custom marker scheme to indicate time unit changes that is not suitable for the Protbuf encoding format.
The M3TSZ timestamp encoder avoids using a control bit for each write by using a marker which contains a prefix that could not be generated by any possible input, and the decoder frequently "looks ahead" for this marker to see if it needs to decode a time unit change.
The Protobuf encoding scheme has no equivalent "impossible bit combination", so it uses explicit control bits to indicate a time unit change instead.

The table below contains a summary of all the possible per-write control bit combinations. Note that `X` is used to denote control bits that will not be included; so even though the format may look inefficient because it requires a maximum of 4 control bits to encode only 6 different combinations, the most common scenario (where the stream contains at least one more write and neither the time unit or schema has changed) can be encoded with just a single bit.

| Combination | Control Bits | Meaning                                                                                     |
|-------------|--------------|---------------------------------------------------------------------------------------------|
| 1           | 1XXX         | The stream contains at least one more write.                                                |
| 2           | 00XX         | End of stream.                                                                              |
| 3           | 0101         | The stream contains at least one more write and the schema has changed.                     |
| 4           | 0110         | The stream contains at least one more write and the time unit has changed.                  |
| 5           | 0111         | The stream contains at least one more write and both the schema and time unit have changed. |
| 6           | 0100         | Impossible combination.                                                                     |

The header ends immediately after combinations #1 and #2, but combinations #3, #4, and #5 will be followed by an encoded time unit change and/or schema change.

#### Time Unit Encoding

Time unit changes are encoded using a single byte such that every possible time unit has a unique value.

#### Schema Encoding

An encoded schema can be thought of as a sequence of `<fieldNum, fieldType>` and is encoded as follows:

1. highest field number (`N`) that will be described (`varint`)
2. `N` sets of 3 bits where each set corresponds to the "custom type", which is enough information to determine how the field should be compressed / decompressed. This is analogous to a Protobuf [`wire type`](https://developers.google.com/protocol-buffers/docs/encoding) in that it includes enough information to skip over the field if its not present in the schema that is being used to decode the message.

Notably, the list only *explicitly* encodes the custom field type. *Implicitly*, the Protobuf field number is encoded by the position of the entry in the list.
In other words, the list of custom encoded fields can be thought of as a bitset, except that instead of using a single bit to encode the value at a given position, we use 3.

For example, given the following Protobuf schema:

```protobuf
message Foo {
	reserved 2, 3;
	string query = 1;
	int32 page_number = 4;
}
```

Encoding the list of custom compressed fields begins by encoding `4` as a `varint`, since that is the highest non-reserved field number.

Next, the field numbers and their types are encoded, 3 bits at a time, where the field number is implied from their position in the list (starting at index 1 since Protobuf fields numbers start at 1), and the type is encoded in the 3 bit combination:

`string query = 1;` is encoded as the first value (indicating field number 1) with the bit combination `111` indicating that it should be treated as `bytes` for compression purposes.

Next, `000` is encoded twice to indicate that no custom compression will be performed for fields `2` or `3` since they are reserved.

Finally, `010` is encoded as the fourth item to indicate that field number `4` will be treated as a signed 32 bit integer.

Note that only fields that support custom encoding are included in the schema. This is because the Protobuf encoding format will take care of schema changes for any non-custom-encoded fields as long as they are valid updates [according to the Protobuf specification](https://developers.google.com/protocol-buffers/docs/proto3#updating).

##### Custom Types

0. (`000`): Not custom encoded - This type indicates that no custom compression will be applied to this field; instead, the standard Protobuf encoding will be used.
1. (`001`): Signed 64 bit integer (`int64`, `sint64`)
2. (`010`): Signed 32 bit integer (`int32`, `sint32`, `enum`)
3. (`011`): Unsigned 64 bit integer (`uint64`. `fixed64`)
4. (`100`): Unsigned 32 bit integer (`uint32`, `fixed32`)
5. (`101`): 64 bit float (`double`)
6. (`110`): 32 bit float (`float`)
7. (`111`): bytes (`bytes`, `string`)

### Compressed Timestamp

The Protobuf compression scheme reuses the delta-of-delta timestamp encoding logic that is implemented in the M3TSZ package and decribed in the [Facebook Gorilla paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

After encoding a control bit with a value of `1` (indicating that there is another write in the stream), the delta-of-delta of the current and previous timestamp is encoded.

Similarly, when decoding the stream, the inverse operation is performed to reconstruct the current timestamp based on both the previous timestamp and delta-of-delta encoded into the stream.

### Compressed Protobuf Fields

Compressing the Protobuf fields is broken into two stages:

1. Custom compressed fields
2. Protobuf marshaled fields

In the first phase, any eligible custom fields are compressed as described in the "Compression Techniques" section.

In the second phase, the Protobuf marshaling format is used to encode and decode the data, with the caveat that fields are compared at the top level and re-encoding is avoided if they have not changed.

#### Custom Compressed Protobuf Fields

The custom fields' compressed values are encoded similarly to how their types are encoded as described in the "Header" section.
In fact, they're even encoded in the same order with the caveat that unlike when we're encoding the types, we don't need to encode a null value for non-contiguous field numbers for which we're not performing any compression.

All of the compressed values are encoded sequentially and no separator or control bits are placed between them, which means that they must encode enough information such that a decoder can determine where each one ends and the next one begins.

The values themselves are encoded based on the field type and the compression technique that is being applied to it. For example, considering the sample Protobuf message from earlier:

```protobuf
message Foo {
	reserved 2, 3;
	string query = 1;
	int32 page_number = 4;
}
```

If the `string` field `query` had never been encoded before, the following control bits would be encoded: `1` (indicating that the value had changed since its previous empty value), followed by `1` again (indicating that the value was not found in the LRU cache and would be encoded in its entirety with a `varint` length prefix).

Next, 6 bits would be used to encode the number of significant digits in the delta between current `page_number` and the previous `page_number`, followed by a control bit indicating if the delta is positive or negative, and then finally the significant bits themselves.

Note that the values encoded for both fields are "self contained" in that they encode all the information required to determine when the end has been reached.

#### Protobuf Marshaled Fields

We recommend reading the [Protocol Buffers Encoding](https://developers.google.com/protocol-buffers/docs/encoding) section of the official documentation before reading this section.
Specifically, understanding how Protobuf messages are (basically) encoded as a stream of tuples in the form of `<field number, wire type, value>` will make understanding this section much easier.

The Protobuf marshaled fields section of the encoding scheme contains all the values that don't currently support performing custom compression.
For the most part, the output of this section is similar to the result of calling `Marshal()` on a message in which all the custom compressed fields have already been removed, and the only remaining fields are ones for which Protobuf will encode directly.
This is possible because, as described in the Protobuf encoding section linked above, the Protobuf wire format does not encode **any** data for fields which are not set or are set to a default value, so by "clearing" the fields that have already been encoded, they can be omitted when marshaling the remainder of the Protobuf message.

While Protobuf's wire format is leaned upon heavily, there is specific attention given to re-encoding fields that haven't changed since the previous value, where "haven't changed" is defined at the top most level of the message.

For example, consider encoding messages with the following schema:

```protobuf
message Outer {
  message Nested {
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

If none of the values inside `nested` have changed since the previous message, the `nested` field doesn't need to be encoded at all.
However, if any of the fields have changed, like `nested.deeper.booly` for example, then the entire `nested` field must be re-encoded (including the `outer` field, even though only the `deeper` field changed).

This top-level "only if it has changed" delta encoding can be used because, when the stream is decoded later, the original message can be reconstructed by merging the previously-decoded message with the current delta message, which contains only fields that have changed since the previous message.

Only marshaling the fields that have changed since the previous message works for the most part, but there is one important edge case: because the Protobuf wire format does not encode **any** data for fields that are set to a default value (zero for `integers` and `floats`, empty array for `bytes` and `strings`, etc), using the standard Protobuf marshaling format with delta encoding works in every scenario *except* for the case where a field is changed from a non-default value to a default value because (because it is not possible to express explicitly setting a field to its default value).

This issue is mitigated by encoding an additional optional (as in it is only encoded when necessary) bitset which indicates any field numbers that were set to the default value of the field's type.

The bitset encoding is straightforward: it begins with a `varint` that encodes the length (number of bits) of the bitset, and then the remaining `N` bits are interpreted as a 1-indexed bitset (because field numbers start at 1 not 0) where a value of `1` indicates the field was changed to its default value.

##### Protobuf Marshaled Fields Encoding Format

The Protobuf Marshaled Fields section of the encoding begins with a single control bit that indicates whether there have been any changes to the Protobuf encoded portion of the message at all.
If the control bit is set to `1`, then there have been changes and decoding must continue; if it is set to `0`, then there were no changes and the decoder can skip to the next write (or stop, if at the end of the stream).

If the previous control bit was set to `1`, indicating that there have been changes, then there will be another control bit which indicates whether any fields have been set to a default value.
If so, then its value will be `1` and the subsequent bits should be interpreted as a `varint` encoding the length of the bitset followed by the actual bitset bits as discussed above.
If the value is `0`, then there is no bitset to decode.

Finally, this portion of the encoding will end with a final `varint` that encodes the length of the bytes that resulted from calling `Marshal()` on the message (where any custom-encoded or unchanged fields were cleared) followed by the actual marshaled bytes themselves.