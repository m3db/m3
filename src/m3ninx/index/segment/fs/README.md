FS Segment
===========

```

┌───────────────────────────────┐           ┌───────────────────────────────┐
│ FST Fields File               │           │ FST Terms File                │
│-------------------------------│           │-------------------------------│
│- Vellum V1 FST                │           │`n` records, each:             │
│- []byte -> FST Terms Offset   │─────┐     │  - payload (`size` bytes)     │
└───────────────────────────────┘     │     │  - size (int64)               │
                                      └────▶│  - magic number (int64)       │
                                            │                               │
                                            │Payload:                       │
                                            │- Vellum V1 FST                │
                                            │- []byte -> Postings Offset    │
                                            └───────────────────────────────┘
        ┌───────────────────────────────┐                   │
        │ Postings Data File            │                   │
        │-------------------------------│                   │
        │`n` records, each:             │                   │
        │  - payload (`size` bytes)     │                   │
        │  - size (int64)               │                   │
        │  - magic number (int64)       │◀──────────────────┘
        │                               │
        │Payload:                       │
        │- Pilosa Bitset                │
        │- List of doc.ID               │
        └──────────┬────────────────────┘
                   │
                   │
                   │
                   │       ┌─────────────────────────┐           ┌───────────────────────────┐
                   │       │ Documents Index File    │           │ Documents Data File       │
                   │       │-------------------------│           │-------------------------  │
                   │       │- Magic Number (int64)   │           │'n' records, each:         │
                   │       │- Num docs (int64)       │    ┌─────▶│  - Magic Number (int64)   │
                   │       │- Base Doc.ID `b` (int64)│    │      │  - Valid (1 byte)         │
                   └──────▶│- Doc `b` offset (int64) ├────┘      │  - Size (int64)           │
                           │- Doc `b+1` offset       │           │  - Payload (`size` bytes) │
                           │...                      │           └───────────────────────────┘
                           │- Doc `b+n-1` offset     │
                           └─────────────────────────┘

```