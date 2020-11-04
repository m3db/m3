# Protobuf Encoding

## Overview

This package contains the encoder/decoder for compressing streams of Protobuf messages matching a provided schema.

All compression is performed in an unbuffered manner such that the encoded stream is updated with each write; there is no internal buffering or batching during which multiple writes are gathered before performing encoding.

Read [encoding.md](./encoding.md) for details on the encoding scheme.
Read [unmarshal.md](./unmarshal.md) for details on the custom dynamic Protobuf unmarshaller.
Read [marshal.md](./marshal.md) for details on the custom dynamic Protobuf marshaller.
