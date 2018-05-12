# Documents File Format

This document describes the format of a segment's documents file. The documents
file contains every document in a segment. The documents file is composed of
three sections:
  1. header
  2. documents
  3. offsets
  4. trailer

```
┌───────────────────────────┐
│          Header           │
├───────────────────────────┤
│                           │
│         Documents         │
│                           │
├───────────────────────────┤
│                           │
│          Offsets          │
│                           │
├───────────────────────────┤
│          Trailer          │
└───────────────────────────┘
```

## 1. Header

The first section of the documents file is the header. It contains the magic number for
the documents file format and the version of the format being used.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │                       │ │
│ │     Magic Number      │ │
│ │       (uint32)        │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │  File Format Version  │ │
│ │       (uint32)        │ │
│ │                       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```

The magic number is `0x6D33D0C5` and is used to uniquely identify a file as a documents
file. The file format version is the version of the documents file format that is being
used. Currently, the only valid version is `1`. Both the magic number and the version
are encoded as a little-endian `uint32`.

## 2. Documents

The documents section comes after the header and contains the actual documents.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │      Document 1       │ │
│ ├───────────────────────┤ │
│ │          ...          │ │
│ ├───────────────────────┤ │
│ │      Document n       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```

### Document

Each document is composed of an ID and fields. The ID is a sequence of valid UTF-8 bytes
and it is encoded first by encoding the length of the ID, in bytes, as a variable-sized
unsigned integer and then encoding the actual bytes which comprise the ID. Following
the ID are the fields. The number of fields in the document is encoded first as a
variable-sized unsigned integer and then the fields themselves are encoded.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │     Length of ID      │ │
│ │       (uvarint)       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │          ID           │ │
│ │        (bytes)        │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │   Number of Fields    │ │
│ │       (uvarint)       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │        Field 1        │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │          ...          │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │        Field n        │ │
│ │                       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```

#### Field

Each field is composed of a name and a value. The name and value are a sequence of valid
UTF-8 bytes and they are encoded by encoding the length of the name (value), in bytes, as
a variable-sized unsigned integer and then encoding the actual bytes which comprise the
name (value). The name is encoded first and the value second.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │ Length of Field Name  │ │
│ │       (uvarint)       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │      Field Name       │ │
│ │        (bytes)        │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │ Length of Field Value │ │
│ │       (uvarint)       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │      Field Value      │ │
│ │        (bytes)        │ │
│ │                       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```

## 3. Offsets

The offsets section follows the documents section and provides a mapping from a document's
posting ID to its offset in the documents section. The offset is calculated from the start
of the documents section and thus does not include the header. The first document will
therefore be offset at 0. Each offset is a `uint64` and since it is of a fixed size the offset
for posting ID `n` can be found at (`n` - `base`) * 8 where `base` is the first posting ID
stored.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │        Offset 1       │ │
│ │        (uint64)       │ │
│ ├───────────────────────┤ │
│ │          ...          │ │
│ ├───────────────────────┤ │
│ │        Offset 2       │ │
│ │        (uint64)       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```

## 4. Trailer

The trailer is the last section and it includes:
  1. the number of documents in the documents section
  2. the base postings ID of the offsets section
  3. the index of the offsets section in the file

All of the fields in the trailer are encoded as a `uint64` in little-endian format.

```
┌───────────────────────────┐
│ ┌───────────────────────┐ │
│ │                       │ │
│ │  Number of Documents  │ │
│ │       (uint64)        │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │    Base Postings ID   │ │
│ │        (uint64)       │ │
│ │                       │ │
│ ├───────────────────────┤ │
│ │                       │ │
│ │    Index of Offsets   │ │
│ │        (uint64)       │ │
│ │                       │ │
│ └───────────────────────┘ │
└───────────────────────────┘
```
