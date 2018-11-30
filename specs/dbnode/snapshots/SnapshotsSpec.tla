------------------------------- MODULE SnapshotsSpec -------------------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS numClients
CONSTANTS numWrites

\* Set to reasonable values to limit the search space that TLC needs to check. Otherwise,
\* the search space is infinite because its always valid for the server to perform a
\* persistence step.
CONSTANTS minNumWritesForPersistence
CONSTANTS minNumWritesForCleanup

AllExceptLast(seq) == SubSeq(seq, 1, Len(seq)-1)

(***************************************************************************
--algorithm SnapshotsSpec

\* Writes issued and acked by the client.
variable
    \* Unique identifier for the next write.
    CurrentIndex = 0;
    \* Writes issued by a client, acked or not.
    IssuedWrites = {};
    \* Writes that have been acked by M3DB.
    AckedWrites = {};
    \* Sequence of files, each of which is represented by a set which stores
    \* the writes contained by the commitlog.
    CommitLogFiles = << {} >>;
    \* Sequence of snapshot checkpoint files, each of which points to the index
    \* in the CommitLogFiles sequence which it contains all the writes up to.
    SnapshotCheckpointFiles = << >>;
    \* Writes persisted outside of the commitlog.
    PersistedWrites = {};

macro write_to_commitlog_and_ack(writes)
begin
    \* Store the writes in the last (more recent / active) commitlog file.
    CommitLogFiles[Len(CommitLogFiles)] := CommitLogFiles[Len(CommitLogFiles)] \union (writes);
    \* Mark all writes as Acked.
    AckedWrites := AckedWrites \union writes;
end macro

macro handle_snapshot()
begin
    \* We haven't already started a snapshot, so rotate the commitlog and mark a snapshot as in progress.
    if snapshotInProgress = FALSE /\ CurrentIndex-lastPersistIndex >= minNumWritesForPersistence then
        \* "Rotate" the commitlog by adding a new one.
        CommitLogFiles := Append(CommitLogFiles, {});
        snapshotInProgress := TRUE;
        lastPersistIndex := CurrentIndex;
    \* We've already started a snapshot, so complete it.
    elsif snapshotInProgress = TRUE then
        \* Sanity checks.
        assert(Len(CommitLogFiles) >= 2);

        either
            \* Snapshot success
            \*
            \* Since we rotate the commitlog at the beginning of every Snapshot before doing
            \* anything else, we know that when snapshotting is complete we can add to PersistedWrites
            \* all the writes in all the commitlog files except for the (most recent) rotated one.
            with allCommitlogFilesExceptLast = AllExceptLast(CommitLogFiles);
                writesToPersist = (UNION {allCommitlogFilesExceptLast[x]: x \in DOMAIN allCommitlogFilesExceptLast});
            do
                PersistedWrites := PersistedWrites \union writesToPersist;
                \* Add a new snapshot checkpoint file which points to the commitlog file up until
                \* which it contains all the data for.
                SnapshotCheckpointFiles := Append(SnapshotCheckpointFiles, Len(CommitLogFiles)-1);
                snapshotInProgress := FALSE;
            end with
        or
            \* Snapshot failure
            snapshotInProgress := FALSE;
        end either
    end if
end macro

macro handle_cleanup()
begin
    if Len(SnapshotCheckpointFiles) >=1 /\
        CurrentIndex-lastCleanupIndex >= minNumWritesForCleanup
    then
        with lastSnapshottedCommitlogIndex = SnapshotCheckpointFiles[Len(SnapshotCheckpointFiles)];
        do
            \* Identify the most recent snapshot metadata file, and delete all commitlogs up to
            \* and including that one because all of thoes writes should have been snapshotted already.
            CommitLogFiles := SubSeq(CommitLogFiles, lastSnapshottedCommitlogIndex+1, Len(CommitLogFiles));
            SnapshotCheckpointFiles := << >>;
            lastCleanupIndex := CurrentIndex;
        end with
    end if
end macro

\* Server process.
process M3DB = 0
variable
    \* Variables used for persistence state (flushing / snapshotting)
    snapshotInProgress = FALSE;

    \* Variables used for preventing background operations from occurring
    \* infinitely.
    lastPersistIndex = 0;
    lastCleanupIndex = 0;

begin
    server_loop: while TRUE do
        either
            \* Take all the unacked writes in IssuedWrites and put them in the commitlog and ack them.
            write_to_commitlog_and_ack(IssuedWrites \ AckedWrites);
        or
            handle_snapshot();
        or
            handle_cleanup();
        end either
    end while
end process

\* Client processes.
process n \in 1..numClients
begin
    client_loop: while CurrentIndex < numWrites do
        IssuedWrites := IssuedWrites \union {CurrentIndex};
        CurrentIndex := CurrentIndex+1;
    end while
end process

end algorithm;
 ***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES CurrentIndex, IssuedWrites, AckedWrites, CommitLogFiles,
          SnapshotCheckpointFiles, PersistedWrites, pc, snapshotInProgress,
          lastPersistIndex, lastCleanupIndex

vars == << CurrentIndex, IssuedWrites, AckedWrites, CommitLogFiles,
           SnapshotCheckpointFiles, PersistedWrites, pc, snapshotInProgress,
           lastPersistIndex, lastCleanupIndex >>

ProcSet == {0} \cup (1..numClients)

Init == (* Global variables *)
        /\ CurrentIndex = 0
        /\ IssuedWrites = {}
        /\ AckedWrites = {}
        /\ CommitLogFiles = << {} >>
        /\ SnapshotCheckpointFiles = << >>
        /\ PersistedWrites = {}
        (* Process M3DB *)
        /\ snapshotInProgress = FALSE
        /\ lastPersistIndex = 0
        /\ lastCleanupIndex = 0
        /\ pc = [self \in ProcSet |-> CASE self = 0 -> "server_loop"
                                        [] self \in 1..numClients -> "client_loop"]

server_loop == /\ pc[0] = "server_loop"
               /\ \/ /\ CommitLogFiles' = [CommitLogFiles EXCEPT ![Len(CommitLogFiles)] = CommitLogFiles[Len(CommitLogFiles)] \union ((IssuedWrites \ AckedWrites))]
                     /\ AckedWrites' = (AckedWrites \union (IssuedWrites \ AckedWrites))
                     /\ UNCHANGED <<SnapshotCheckpointFiles, PersistedWrites, snapshotInProgress, lastPersistIndex, lastCleanupIndex>>
                  \/ /\ IF snapshotInProgress = FALSE /\ CurrentIndex-lastPersistIndex >= minNumWritesForPersistence
                           THEN /\ CommitLogFiles' = Append(CommitLogFiles, {})
                                /\ snapshotInProgress' = TRUE
                                /\ lastPersistIndex' = CurrentIndex
                                /\ UNCHANGED << SnapshotCheckpointFiles,
                                                PersistedWrites >>
                           ELSE /\ IF snapshotInProgress = TRUE
                                      THEN /\ Assert((Len(CommitLogFiles) >= 2),
                                                     "Failure of assertion at line 54, column 9 of macro called at line 111, column 13.")
                                           /\ \/ /\ LET allCommitlogFilesExceptLast == AllExceptLast(CommitLogFiles) IN
                                                      LET writesToPersist == (UNION {allCommitlogFilesExceptLast[x]: x \in DOMAIN allCommitlogFilesExceptLast}) IN
                                                        /\ PersistedWrites' = (PersistedWrites \union writesToPersist)
                                                        /\ SnapshotCheckpointFiles' = Append(SnapshotCheckpointFiles, Len(CommitLogFiles)-1)
                                                        /\ snapshotInProgress' = FALSE
                                              \/ /\ snapshotInProgress' = FALSE
                                                 /\ UNCHANGED <<SnapshotCheckpointFiles, PersistedWrites>>
                                      ELSE /\ TRUE
                                           /\ UNCHANGED << SnapshotCheckpointFiles,
                                                           PersistedWrites,
                                                           snapshotInProgress >>
                                /\ UNCHANGED << CommitLogFiles,
                                                lastPersistIndex >>
                     /\ UNCHANGED <<AckedWrites, lastCleanupIndex>>
                  \/ /\ IF Len(SnapshotCheckpointFiles) >=1 /\
                            CurrentIndex-lastCleanupIndex >= minNumWritesForCleanup
                           THEN /\ LET lastSnapshottedCommitlogIndex == SnapshotCheckpointFiles[Len(SnapshotCheckpointFiles)] IN
                                     /\ CommitLogFiles' = SubSeq(CommitLogFiles, lastSnapshottedCommitlogIndex+1, Len(CommitLogFiles))
                                     /\ SnapshotCheckpointFiles' = << >>
                                     /\ lastCleanupIndex' = CurrentIndex
                           ELSE /\ TRUE
                                /\ UNCHANGED << CommitLogFiles,
                                                SnapshotCheckpointFiles,
                                                lastCleanupIndex >>
                     /\ UNCHANGED <<AckedWrites, PersistedWrites, snapshotInProgress, lastPersistIndex>>
               /\ pc' = [pc EXCEPT ![0] = "server_loop"]
               /\ UNCHANGED << CurrentIndex, IssuedWrites >>

M3DB == server_loop

client_loop(self) == /\ pc[self] = "client_loop"
                     /\ IF CurrentIndex < numWrites
                           THEN /\ IssuedWrites' = (IssuedWrites \union {CurrentIndex})
                                /\ CurrentIndex' = CurrentIndex+1
                                /\ pc' = [pc EXCEPT ![self] = "client_loop"]
                           ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                /\ UNCHANGED << CurrentIndex, IssuedWrites >>
                     /\ UNCHANGED << AckedWrites, CommitLogFiles,
                                     SnapshotCheckpointFiles, PersistedWrites,
                                     snapshotInProgress, lastPersistIndex,
                                     lastCleanupIndex >>

n(self) == client_loop(self)

Next == M3DB
           \/ (\E self \in 1..numClients: n(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION

\* Invariants - Add these to the model checker when running.
AllAckedWritesAreBootstrappable == AckedWrites \subseteq ( (UNION { CommitLogFiles[x] : x \in DOMAIN CommitLogFiles }) \union PersistedWrites)
=============================================================================
\* Modification History
\* Last modified Sun Nov 25 21:19:27 EST 2018 by richardartoul
\* Created Sat Nov 24 16:19:03 EST 2018 by richardartoul
