---------------------------- MODULE FlushVersion ----------------------------
EXTENDS Integers, TLC

\* Constants used to limit the potential search space when
\* running the TLC model checker.
CONSTANT maxNumWrites
CONSTANT maxNumFlushes
CONSTANT maxNumTicks

(***************************************************************************
\* This specification models the version-based flushing/ticking of series
\* buffer buckets that is required for the warm/cold write feature. The
\* specification models the system by running three different processes:
\*
\*    1. One process responsible for beginning and ending the flush process.
\*       This process will begin a flush by looping through all the BucketsInMemory
\*       and setting their LastSuccessfulFlushVersion to whatever the
\*       CurrentLastSuccessfulFlushVersion is. It also models both flush
\*       successes and flush failures.
\*          Flush Success: Copy all the BucketsInMemory whose
|*                         LastSuccessfulFlushVersion is smaller than
\*                         or equal to the CurrentLastSuccessfulFlushVersion (I.E those
\*                         we could have conceivably known about at flush start time) into
\*                         the set of PersistedBuckets, and then increment the
\*                         CurrentLastSuccessfulFlushVersion.
\*
\*          Flush Failure: Nothing happens and the CurrentLastSuccessfulFlushVersion stays the same.
\*
\*
\*    2. One process responsible for creating new buckets by issuing "writes". It does
\*       this by storing them in the BucketsInMemory set, as well as in the
\*       WrittenBuckets set (so we can keep track of them independently of the eviction
\*       from memory process).
\*
\*
\*    3. One process responsible for the "Ticking" process that will periodically evict
\*       every bucket from the BucketsInMemory set whose LastSuccessfulFlushVersion is >=
\*       CurrentLastSuccessfulFlushVersion and also > 0 (because version zero is the bucket
\*       for unflushed writes).
\*
\*
\* The specification verifies the system by allowing all of these processes to run
\* concurrently (as they do in M3DB) and asserting that the invariant that we can
\* always read every bucket that has been written from the combination of the
\* BucketsInMemory and PersistedBuckets holds true at every step (meaning we never
\* lose data due to the Tick evicting data that hasn't been flushed yet.)
--algorithm FlushVersion

variable
    \* BucketsInMemory, WrittenBuckets, and Persisted buckets are all sets that
    \* store records in the form:
    \*    1. ID (int)
    \*    2. LastSuccessfulFlushVersion (int)
    \*
    \* BucketsInMemory stores all the buckets that are currently in the M3DB
    \* process's memory, WrittenBuckets stores all the buckets that the client
    \* ever issued (to serve as a source of truth for checking invariants), and
    \* PersistedBuckets stores all the buckets that have been flushed successfully.
    BucketsInMemory = {[ID |-> 0, FlushVersion |-> 0]};
    WrittenBuckets = {[ID |-> 0, FlushVersion |-> 0]};
    PersistedBuckets = {};
    \* The last version that was successfully flushed to disk.
    LastSuccessfulFlushVersion = 0;

\* Simulate background flushes which either succeed, copy buckets to the
\* PersistedBuckets set and then increment the flush version, or fail and
\* leave everything as is.
process Flush = 0
    variable
        CurrentIndex = 0;
        \* Whether a flush is currently ongoing.
        FlushInProgress = FALSE;
begin
    flush_loop: while CurrentIndex < maxNumFlushes do
        either
            await FlushInProgress = TRUE;

            either
                \* Flush success
                \*
                \* Move all BucketsInMemory that have been flushed (according to
                \* their flush version) to the PersistedBuckets set but keep them,
                \* around in memory for now.
                PersistedBuckets := PersistedBuckets \union {
                    x \in BucketsInMemory:
                        x.FlushVersion <= LastSuccessfulFlushVersion + 1 /\
                        x.FlushVersion > 0};
                LastSuccessfulFlushVersion := LastSuccessfulFlushVersion + 1;
            or
                \* Flush failure - No-op
                LastSuccessfulFlushVersion := LastSuccessfulFlushVersion;
            end either;

            FlushInProgress := FALSE;
        or
            await FlushInProgress = FALSE;

            \* Update the flush version of all BucketsInMemory in memory to the
            \* current value + 1 since that is what the (possibly partially complete)
            \* flush could have done to them.
            BucketsInMemory := {
                [ID |-> x.ID, FlushVersion |-> LastSuccessfulFlushVersion + 1]:
                    x \in BucketsInMemory};
            FlushInProgress := TRUE;
        end either;

        CurrentIndex := CurrentIndex+1;
    end while
end process

\* Simulate a process that is writing by putting new buckets into BucketsInMemory.
process Write = 1
    variable
        CurrentIndex = 0;
        NextBucketID = 1;
begin
    write_loop: while CurrentIndex < maxNumWrites do
        \* Write a new bucket. Note that the FlushVersion is always zero for a new bucket.
        BucketsInMemory := BucketsInMemory \union {[ID |-> NextBucketID, FlushVersion |-> 0]};
        WrittenBuckets := WrittenBuckets \union {[ID |-> NextBucketID, FlushVersion |-> 0]};
        NextBucketID := NextBucketID + 1;
        CurrentIndex := CurrentIndex+1;
    end while
end process

process Tick = 2
    variable CurrentIndex = 0;

begin
    tick_loop: while CurrentIndex < maxNumTicks do
        \* Evict from memory any block whose flush version is smaller than or equal to the
        \* current flush version.
        BucketsInMemory := BucketsInMemory \ {
            x \in BucketsInMemory:
                x.FlushVersion <= LastSuccessfulFlushVersion /\
                x.FlushVersion > 0};
    end while
end process

end algorithm


 ***************************************************************************)
\* BEGIN TRANSLATION
\* Process variable CurrentIndex of process Flush at line 66 col 9 changed to CurrentIndex_
\* Process variable CurrentIndex of process Write at line 114 col 9 changed to CurrentIndex_W
VARIABLES BucketsInMemory, WrittenBuckets, PersistedBuckets,
          LastSuccessfulFlushVersion, pc, CurrentIndex_, FlushInProgress,
          CurrentIndex_W, NextBucketID, CurrentIndex

vars == << BucketsInMemory, WrittenBuckets, PersistedBuckets,
           LastSuccessfulFlushVersion, pc, CurrentIndex_, FlushInProgress,
           CurrentIndex_W, NextBucketID, CurrentIndex >>

ProcSet == {0} \cup {1} \cup {2}

Init == (* Global variables *)
        /\ BucketsInMemory = {[ID |-> 0, FlushVersion |-> 0]}
        /\ WrittenBuckets = {[ID |-> 0, FlushVersion |-> 0]}
        /\ PersistedBuckets = {}
        /\ LastSuccessfulFlushVersion = 0
        (* Process Flush *)
        /\ CurrentIndex_ = 0
        /\ FlushInProgress = FALSE
        (* Process Write *)
        /\ CurrentIndex_W = 0
        /\ NextBucketID = 1
        (* Process Tick *)
        /\ CurrentIndex = 0
        /\ pc = [self \in ProcSet |-> CASE self = 0 -> "flush_loop"
                                        [] self = 1 -> "write_loop"
                                        [] self = 2 -> "tick_loop"]

flush_loop == /\ pc[0] = "flush_loop"
              /\ IF CurrentIndex_ < maxNumFlushes
                    THEN /\ \/ /\ FlushInProgress = TRUE
                               /\ \/ /\ PersistedBuckets' = (                PersistedBuckets \union {
                                                             x \in BucketsInMemory:
                                                                 x.FlushVersion <= LastSuccessfulFlushVersion + 1 /\
                                                                 x.FlushVersion > 0})
                                     /\ LastSuccessfulFlushVersion' = LastSuccessfulFlushVersion + 1
                                  \/ /\ LastSuccessfulFlushVersion' = LastSuccessfulFlushVersion
                                     /\ UNCHANGED PersistedBuckets
                               /\ FlushInProgress' = FALSE
                               /\ UNCHANGED BucketsInMemory
                            \/ /\ FlushInProgress = FALSE
                               /\ BucketsInMemory' =                {
                                                     [ID |-> x.ID, FlushVersion |-> LastSuccessfulFlushVersion + 1]:
                                                         x \in BucketsInMemory}
                               /\ FlushInProgress' = TRUE
                               /\ UNCHANGED <<PersistedBuckets, LastSuccessfulFlushVersion>>
                         /\ CurrentIndex_' = CurrentIndex_+1
                         /\ pc' = [pc EXCEPT ![0] = "flush_loop"]
                    ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
                         /\ UNCHANGED << BucketsInMemory, PersistedBuckets,
                                         LastSuccessfulFlushVersion,
                                         CurrentIndex_, FlushInProgress >>
              /\ UNCHANGED << WrittenBuckets, CurrentIndex_W, NextBucketID,
                              CurrentIndex >>

Flush == flush_loop

write_loop == /\ pc[1] = "write_loop"
              /\ IF CurrentIndex_W < maxNumWrites
                    THEN /\ BucketsInMemory' = (BucketsInMemory \union {[ID |-> NextBucketID, FlushVersion |-> 0]})
                         /\ WrittenBuckets' = (WrittenBuckets \union {[ID |-> NextBucketID, FlushVersion |-> 0]})
                         /\ NextBucketID' = NextBucketID + 1
                         /\ CurrentIndex_W' = CurrentIndex_W+1
                         /\ pc' = [pc EXCEPT ![1] = "write_loop"]
                    ELSE /\ pc' = [pc EXCEPT ![1] = "Done"]
                         /\ UNCHANGED << BucketsInMemory, WrittenBuckets,
                                         CurrentIndex_W, NextBucketID >>
              /\ UNCHANGED << PersistedBuckets, LastSuccessfulFlushVersion,
                              CurrentIndex_, FlushInProgress, CurrentIndex >>

Write == write_loop

tick_loop == /\ pc[2] = "tick_loop"
             /\ IF CurrentIndex < maxNumTicks
                   THEN /\ BucketsInMemory' =                BucketsInMemory \ {
                                              x \in BucketsInMemory:
                                                  x.FlushVersion <= LastSuccessfulFlushVersion /\
                                                  x.FlushVersion > 0}
                        /\ pc' = [pc EXCEPT ![2] = "tick_loop"]
                   ELSE /\ pc' = [pc EXCEPT ![2] = "Done"]
                        /\ UNCHANGED BucketsInMemory
             /\ UNCHANGED << WrittenBuckets, PersistedBuckets,
                             LastSuccessfulFlushVersion, CurrentIndex_,
                             FlushInProgress, CurrentIndex_W, NextBucketID,
                             CurrentIndex >>

Tick == tick_loop

Next == Flush \/ Write \/ Tick
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION


\* Invariants, include these when running the model checker.
WrittenIDs == {x.ID: x \in WrittenBuckets}
ReadableIDs == {x.ID: x \in (BucketsInMemory \union PersistedBuckets)}
DoesNotLoseData ==  WrittenIDs \subseteq ReadableIDs

=============================================================================
\* Modification History
\* Last modified Fri Nov 30 19:08:55 EST 2018 by richardartoul
\* Created Fri Nov 30 15:08:04 EST 2018 by richardartoul
