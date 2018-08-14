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
                   │       ┌──────────────────────────┐           ┌───────────────────────────┐
                   │       │ Documents Index File     │           │ Documents Data File       │
                   │       │--------------------------│           │-------------------------  │
                   │       │- Base Doc.ID `b` (uint64)│           │'n' records, each:         │
                   │       │- Doc `b` offset (uint64) │    ┌─────▶│  - ID (bytes)             │
                   │       │- Doc `b+1` offset        │    │      │  - Fields (bytes)         │
                   └──────▶│...                       ├────┘      └───────────────────────────┘
                           │- Doc `b+n-1` offset      │
                           └──────────────────────────┘
```