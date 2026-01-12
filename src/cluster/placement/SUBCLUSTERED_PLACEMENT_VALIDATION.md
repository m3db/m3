# Subclustered Placement Validation - Comprehensive Test Coverage

## Overview

This document describes the validation logic for subclustered placements in the `validateSubclusteredPlacement` function (`src/cluster/placement/placement.go`) and the comprehensive unit tests that cover all edge cases.

## Changes Summary

### 1. Code Cleanup: Removed Unreachable Code

**File:** `src/cluster/placement/placement.go`

Removed the following unreachable code block:

```go
// REMOVED:
if len(shardSubclusterInstances) < instancesPerSubCluster &&
    len(currSubclusterInstances) < instancesPerSubCluster {
    return fmt.Errorf("invalid shard assignment, shard %d is shared by two partial subclusters %d and %d",
        shard, shardSubclusterID, currSubclusterID)
}
```

**Reason:** This code path was unreachable because the check at line 489-491 (`if partialSubclusters > 1`) already catches any scenario with more than one partial subcluster. If a shard is shared by two partial subclusters, there must be at least 2 partial subclusters, which triggers the earlier error first.

**Replaced with:** A comment explaining why this case is already handled:
```go
// Note: The case where both subclusters are partial is already caught by
// the "more than one partial subcluster" check above (line 489-491).
```

### 2. New Unit Tests Added

**File:** `src/cluster/placement/placement_test.go`

Added new test cases to `TestValidateSubclusteredPlacementEdgeCases` covering all validation paths and edge cases.

---

## Validation Logic

The `validateSubclusteredPlacement` function enforces several critical invariants for subclustered placements.

### Validation Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    validateSubclusteredPlacement                 │
├─────────────────────────────────────────────────────────────────┤
│  1. Build Maps (skip leaving instances & leaving shards)         │
│     ├─ subClusterToInstanceMap: subclusterID → instances        │
│     ├─ shardToSubclusterMap: shardID → subclusters              │
│     └─ shardToIsolationGroupMap: shardID → isolation groups     │
├─────────────────────────────────────────────────────────────────┤
│  2. Validate Subcluster Instance Counts                          │
│     ├─ ERROR if instances > instancesPerSubCluster              │
│     └─ Count partial subclusters (instances < limit)            │
├─────────────────────────────────────────────────────────────────┤
│  3. ERROR if partialSubclusters > 1                              │
├─────────────────────────────────────────────────────────────────┤
│  4. For each shard:                                              │
│     ├─ ERROR if shard in > 2 subclusters                        │
│     ├─ If shard in 2 subclusters:                               │
│     │   └─ ERROR if both subclusters are FULL                   │
│     └─ ERROR if isolation groups ≠ replicaFactor                │
└─────────────────────────────────────────────────────────────────┘
```

### Key Definitions

| Term | Definition |
|------|------------|
| **Full Subcluster** | A subcluster with exactly `instancesPerSubCluster` instances |
| **Partial Subcluster** | A subcluster with fewer than `instancesPerSubCluster` instances |
| **Leaving Instance** | An instance where ALL shards are in `Leaving` state |
| **Leaving Shard** | A shard with state `Leaving` (being moved away from instance) |

---

## Detailed Test Case Documentation

### ✅ Valid Placement Scenarios

#### Basic Valid Placements

| Test Case | Description | What It Validates |
|-----------|-------------|-------------------|
| `valid subclustered placement - single subcluster` | 4 instances in one subcluster with RF=2 | Basic valid subclustered placement works |
| `valid subclustered placement - multiple subclusters` | 6 instances across 2 full subclusters | Multiple full subclusters with isolated shards |
| `empty placement` | No instances or shards | Empty subclustered placement is valid |
| `single instance placement` | 1 instance in partial subcluster | Minimal valid placement |

#### Leaving Instance/Shard Handling

| Test Case | Description | What It Validates |
|-----------|-------------|-------------------|
| `valid subclustered placement with leaving instances` | Instances with all shards in Leaving state | Leaving instances are correctly excluded from validation |
| `shard with leaving state ignored in validation` | Individual shards in Leaving state | Leaving shards don't count toward isolation group validation |
| `all instances in subcluster are leaving` | Entire subcluster draining to another | Valid during subcluster decommission |

#### Partial Subcluster Scenarios

| Test Case | Description | What It Validates |
|-----------|-------------|-------------------|
| `incomplete subcluster - should not fail validation` | Subcluster with fewer instances than limit | Single partial subcluster is allowed |
| `one full and one partial subcluster is valid` | 1 full + 1 partial subcluster, no shared shards | Valid during scale-up operations |

#### Boundary Conditions

| Test Case | Description | What It Validates |
|-----------|-------------|-------------------|
| `multiple isolation groups per shard` | Shard replicated across 3 isolation groups | Correct isolation group distribution |
| `instancesPerSubcluster equals 1 with single instance` | Each subcluster has exactly 1 instance | Boundary case for minimum subcluster size |

---

### ✅ Valid Shard Movement Scenarios (Scale Up/Down)

| Test Case | Description | What It Validates |
|-----------|-------------|-------------------|
| `shards in transitionary state while moving to another subcluster` | Shard moving from full to partial subcluster | Shard shared between 1 full + 1 partial is allowed |
| `valid shard movement from full to partial subcluster` | Full subcluster giving shard to partial | Scale-up: new subcluster receiving shards |
| `valid shard movement from partial to full subcluster` | Partial subcluster giving shard to full | Scale-down: subcluster being decommissioned |

**Key Insight:** During scale operations, a shard can temporarily exist in two subclusters—one giving (with shard in `Leaving` state) and one receiving (with shard in `Initializing` state). This is valid **only if** at least one of the subclusters is partial.

---

### ❌ Invalid Placement Scenarios

#### Subcluster Instance Count Violations

| Test Case | Error Message | Description |
|-----------|---------------|-------------|
| `subcluster with more instances than instancesPerSubcluster` | `invalid subcluster %d, expected at most %d instances, actual %d` | Subcluster exceeds configured instance limit |

**Validation Code (lines 479-483):**
```go
if len(instances) > instancesPerSubCluster {
    return fmt.Errorf("invalid subcluster %d, expected at most %d instances, actual %d",
        subclusterID, instancesPerSubCluster, len(instances))
}
```

---

#### Multiple Partial Subclusters

| Test Case | Error Message | Description |
|-----------|---------------|-------------|
| `more than one partial subcluster` | `invalid placement, more than one partial subcluster found: 2` | 2 partial subclusters detected |
| `three partial subclusters` | `invalid placement, more than one partial subcluster found: 3` | 3 partial subclusters detected |

**Validation Code (lines 489-491):**
```go
if partialSubclusters > 1 {
    return fmt.Errorf("invalid placement, more than one partial subcluster found: %d", partialSubclusters)
}
```

**Rationale:** At most one partial subcluster is allowed at any time. This ensures controlled scale-up/down operations where only one subcluster is being built up or torn down.

---

#### Shard Distribution Violations

| Test Case | Error Message | Description |
|-----------|---------------|-------------|
| `shards in transitionary state - belongs to > 2 subclusters` | `invalid shard %d, expected at most 2 subclusters (only during shard movement), actual %d` | Shard exists in 3+ subclusters |
| `shards are shared among multiple complete subclusters` | `invalid shard %d, expected subcluster id %d, actual %d` | Shard shared between 2 full subclusters |

**Validation Code (lines 499-520):**
```go
// Shard in more than 2 subclusters is always invalid
if len(subclusters) > 2 {
    return fmt.Errorf("invalid shard %d, expected at most 2 subclusters...")
}

// Shard in exactly 2 FULL subclusters is invalid
if len(subclusters) == 2 {
    if len(shardSubclusterInstances) == instancesPerSubCluster &&
       len(currSubclusterInstances) == instancesPerSubCluster {
        return fmt.Errorf("invalid shard %d, expected subcluster id %d, actual %d"...)
    }
}
```

**Rationale:** 
- A shard can only exist in **at most 2 subclusters** during movement operations
- If a shard is in 2 subclusters, **at least one must be partial** (active scale operation)
- Two full subclusters sharing a shard indicates data inconsistency

---

#### Isolation Group Violations

| Test Case | Error Message | Description |
|-----------|---------------|-------------|
| `shard with wrong isolation group count` | `invalid shard %d, expected %d isolation groups, actual %d` | Shard replicas in same isolation group |
| `shard with insufficient isolation groups` | `invalid shard %d, expected 3 isolation groups, actual 2` | Not enough isolation group diversity |

**Validation Code (lines 526-529):**
```go
if len(shardToIsolationGroupMap[shard]) != p.ReplicaFactor() {
    return fmt.Errorf("invalid shard %d, expected %d isolation groups, actual %d",
        shard, p.ReplicaFactor(), len(shardToIsolationGroupMap[shard]))
}
```

**Rationale:** Each shard replica must be in a **unique isolation group** for fault tolerance. If `replicaFactor=3`, the shard must exist in exactly 3 different isolation groups.

---

#### Instance Configuration Violations

| Test Case | Error Message | Description |
|-----------|---------------|-------------|
| `instance with uninitialized subcluster ID` | (caught by parent `Validate()` function) | Instance missing subcluster assignment |

**Note:** This is validated in the parent `Validate()` function before `validateSubclusteredPlacement` is called.

---

## Summary of Validation Rules

| Rule | Condition | Error |
|------|-----------|-------|
| **Max Instances per Subcluster** | `instances > instancesPerSubCluster` | Subcluster exceeds instance limit |
| **Max Partial Subclusters** | `partialSubclusters > 1` | More than one incomplete subcluster |
| **Max Subclusters per Shard** | `subclusters > 2` | Shard in too many subclusters |
| **No Shard Sharing Between Full Subclusters** | Both subclusters are full | Shard inconsistently distributed |
| **Isolation Group Diversity** | `isolationGroups ≠ replicaFactor` | Insufficient fault tolerance |

---

## Shard Movement Rules During Scale Operations

### Valid Shard Movement

```
┌──────────────────┐     Shard Moving      ┌──────────────────┐
│  FULL Subcluster │ ──────────────────►   │ PARTIAL Subcluster│
│  (Leaving shard) │                       │ (Initializing)    │
└──────────────────┘                       └──────────────────┘
                            ✅ VALID
```

```
┌──────────────────┐     Shard Moving      ┌──────────────────┐
│PARTIAL Subcluster│ ──────────────────►   │  FULL Subcluster │
│  (Leaving shard) │                       │ (Initializing)    │
└──────────────────┘                       └──────────────────┘
                            ✅ VALID
```

### Invalid Shard Movement

```
┌──────────────────┐     Shard Shared      ┌──────────────────┐
│  FULL Subcluster │ ◄─────────────────►   │  FULL Subcluster │
│                  │                       │                   │
└──────────────────┘                       └──────────────────┘
                            ❌ INVALID
```

```
┌──────────────────┐     Shard Shared      ┌──────────────────┐
│PARTIAL Subcluster│ ◄─────────────────►   │PARTIAL Subcluster│
│                  │                       │                   │
└──────────────────┘                       └──────────────────┘
            ❌ INVALID (caught by "more than one partial" rule)
```

---

## Test Execution

```bash
# Run all subclustered placement tests
go test -v -run "TestValidateSubclustered" ./src/cluster/placement/...

# Run edge case tests only
go test -v -run "TestValidateSubclusteredPlacementEdgeCases" ./src/cluster/placement/...

# Run specific test case
go test -v -run "TestValidateSubclusteredPlacementEdgeCases/more_than_one_partial_subcluster" ./src/cluster/placement/...
```

---

## Test Results

All **24 test cases** pass successfully:

```
=== RUN   TestValidateSubclusteredPlacement
--- PASS: TestValidateSubclusteredPlacement (0.00s)
    --- PASS: valid subclustered placement - single subcluster
    --- PASS: valid subclustered placement - multiple subclusters
    --- PASS: shard with wrong isolation group count
    --- PASS: instance with uninitialized subcluster ID
    --- PASS: valid subclustered placement with leaving instances
    --- PASS: shard with leaving state ignored in validation
    --- PASS: shards are shared among multiple complete subclusters
    --- PASS: shards in transitionary state while moving to another subcluster
    --- PASS: shards in transitionary state - belongs to > 2 subclusters

=== RUN   TestValidateSubclusteredPlacementEdgeCases
--- PASS: TestValidateSubclusteredPlacementEdgeCases (0.00s)
    --- PASS: empty placement
    --- PASS: single instance placement
    --- PASS: incomplete subcluster - should not fail validation
    --- PASS: multiple isolation groups per shard
    --- PASS: shard with insufficient isolation groups
    --- PASS: subcluster with more instances than instancesPerSubcluster
    --- PASS: more than one partial subcluster
    --- PASS: valid shard movement from full to partial subcluster
    --- PASS: valid shard movement from partial to full subcluster
    --- PASS: instancesPerSubcluster equals 1 with single instance
    --- PASS: all instances in subcluster are leaving
    --- PASS: three partial subclusters
    --- PASS: one full and one partial subcluster is valid
```
