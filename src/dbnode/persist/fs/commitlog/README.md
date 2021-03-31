# Overview

The commitlog package contains all the code for reading and writing commitlog files.

# Implementation

## Writer

The commitlog writer consists of two main components:

1. The `CommitLog` itself which contains all of the public APIs as well as the logic for managing concurrency, rotating files, and inspecting the status of the commitlog (via the `ActiveLogs` and `QueueLength` APIs).
2. The `CommitLogWriter` which contains all of the logic for opening/closing files and writing bytes to disk.

### Commitlog

At a high-level, the `CommitLog` handles writes by inserting them into a queue (buffered channel) in batches and then dequeing the batches in a single-threaded manner and writing all of the individual writes for a batch into the commitlog file one at a time.

#### Synchronization

The primary synchronization that happens in the commitlog is via the queue (buffered channel). Many goroutines will insert pending writes into the queue concurrently via the `Write()` and `WriteBatch()` APIs, however, only a single goroutine will pull items off the queue and write them to disk.

```
┌──────────────────────────┐
│Goroutine 1: WriteBatch() ├─────┐                                                         ┌───────────────────────────────────┐          ┌───────────────────────────────────┐
└──────────────────────────┘     │                                                         │                                   │          │                                   │
                                 │                                                         │                                   │          │                                   │
                                 │                                                         │                                   │          │                                   │
┌──────────────────────────┐     │       ┌─────────────────────────────────────┐           │             Commitlog             │          │              Writer               │
│Goroutine 2: WriteBatch() │─────┼──────▶│           Commitlog Queue           ├──────────▶│                                   │─────────▶│                                   │
└──────────────────────────┘     │       └─────────────────────────────────────┘           │ Single-threaded Writer Goroutine  │          │           Write to disk           │
                                 │                                                         │                                   │          │                                   │
                                 │                                                         │                                   │          │                                   │
┌──────────────────────────┐     │                                                         │                                   │          │                                   │
│Goroutine 3: WriteBatch() ├─────┘                                                         └───────────────────────────────────┘          └───────────────────────────────────┘
└──────────────────────────┘
```
*The monopic file that was used to generate the diagram above is located at `docs/code_assets/commitlog/queue.monopic`.*

Since there is only one goroutine pulling items off of the queue, any state that it alone manages can remain unsynchronized since no other goroutines will interact with it.

In addition to the queue, the commitlog has two other forms of synchronization:

1. The `closedState` lock which is an RWLock. An RLock is held for the duration of any operation during which the commitlog must remain open.
2. The `flushState` lock. The scope of this lock is very narrow as its only used to protect access to the `lastFlushAt` field.

#### Rotating Files

Rotating commitlog files is initiated by the `RotateLogs()` API so that callers can control when this occurs. The `CommitLog` itself will never rotate files on its own without the `RotateLogs()` API being called.

The commitlog files are not rotated immediately when the `RotateLogs()` method is called because that would require a lot of complicated and expensive synchronization with the `CommitLogWriter` goroutine. Instead, a `rotateLogsEventType` is pushed into the queue and when the single-threaded writer goroutine pulls this event off of the channel it will rotate the commitlog files (since it has exclusive access to them) and then invoke a callback function which notifies the `RotateLogs()` method call (which has been blocked this whole time) to complete and return success to the caller.

While the `CommitLog` only writes to a single file at once, it maintains two open writers at all times so that they can be "hot-swapped" when the commitlog files need to be rotated. This allows the single-threaded writer goroutine to continue uninterrupted by syscalls and I/O during rotation events which in turn prevents the queue from backing up. Otherwise, rotation events could block the writer for so long (while it waited for a new file to be created) that it caused the queue to back up significantly.

When a rotation event occurs, instead of waiting for a new file to be opened, the `CommitLog` writer goroutine will swap the primary and secondary `CommitLogWriter` such that the secondary `CommitLogWriter` (which has an empty file) becomes the primary and vice versa. This allows the `CommitLog` writer goroutine to continue writing uninterrupted.

In the meantime, a goroutine is started in the background that is responsible for resetting the now secondary (formerly primary) `CommitLogWriter` by closing it (which will flush any pending / buffered writes to disk) and re-opening it (which will create a new empty commitlog file in anticipation of the next rotation event).

Finally, the next time the `CommitLog` attempts to rotate its commitlogs it will need to use the associated `sync.WaitGroup` to ensure that the previously spawned background goroutine has completed resetting the secondary `CommitLogWriter` before it attempts a new hot-swap.

### Handling Errors

The current implementation will panic if any I/O errors are ever encountered while writing bytes to disk or opening/closing files. In the future a "commitlog failure policy" similar to [Cassandra's "stop"](https://github.com/apache/cassandra/blob/6dfc1e7eeba539774784dfd650d3e1de6785c938/conf/cassandra.yaml#L232) may be introduced.

# Testing

The commitlog package is tested via:

1. Standard unit tests
2. Property tests
3. Concurrency tests

# File Format

See `/docs/architecture/m3db/commitlogs.md`.