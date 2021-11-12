---
title: "Fileset Migrations"
weight: 20
---

Occasionally, changes will be made to the format of fileset files on disk. When those changes need to be applied to already existing filesets, a fileset migration is required. Migrating existing filesets is beneficial so that improvements made in newer releases can be applied to all filesets, not just newly created ones.

## Migration Process
Migrations are executed during the initial stages of the bootstrap. When enabled, the filesystem bootstrapper will scan for filesets that should be migrated and migrate any filesets found. A fileset is determined to be in need of a migration based on the `MajorVersion` and `MinorVersion` found in the info file. If `MajorVersion.MinorVersion` is less than the target migration version, then that fileset will be scheduled for migration.

If migrations are deemed necessary, the bootstrap process pauses until the migrations complete. If a failure occurs while migrating, an error is logged and the process continues. If a fileset is not successfully migrated, the non-migrated version of the fileset is used going forward. In other words, whether they succeed or fail, migrations should leave filesets in a good state.

## Enabling Migrations
Migrations are enabled by setting the following fields in the M3 configuration (`m3dbnode.yml`):

```yaml
db:
  bootstrap:
    filesystem:
      migration:
        targetMigrationVersion: "1.1"
        # Optional. Defaults to the number of available CPUs / 2.
        concurrency: <# of concurrent workers>
```

## Valid Target Migration Versions

<table>
<thead>
<tr>
<th>Version</th>
<th>Description</th>
</tr>
</thead>

<tbody>
<tr>
<td><code>&quot;none&quot;</code></td>
<td>Disables migrations. Effectively the same as not including the migration option in the M3 configuration</td>
</tr>
<tr>
<td><code>&quot;1.1&quot;</code></td>
<td>Migrates to version 1.1. Version 1.1 adds checksum values to individual entries in the index file of data filesets. This speeds up bootstrapping as validating the index file no longer requires loading and calculating the checksum of the entire file against the value in the digests file.</td>
</tr>
</tbody>
</table>