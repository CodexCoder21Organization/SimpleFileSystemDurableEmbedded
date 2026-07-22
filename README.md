# SimpleFileSystemDurableEmbedded

Durable, in-process implementation of the [`simplefilesystem`](https://github.com/CodexCoder21Organization/SimpleFileSystemApi) 0.2.0 contract. File and directory metadata is stored transactionally in CockroachDB while immutable 4 MiB content blocks are stored directly in Blobstore by uppercase SHA-256 hash.

`DurableSimpleFileSystemManager` implements the multi-filesystem lifecycle API and vends `DurableSimpleFileSystem` handles. Writes upload and pin staged blocks before one metadata transaction atomically publishes a new immutable generation, updates quota usage, and queues superseded blocks for idempotent garbage collection.

## Building

Build the Maven artifact from the repository root:

```bash
scripts/build.bash simplefilesystem.durable.buildMaven
```

The artifact coordinate is:

```kotlin
MavenPrebuilt("simplefilesystem.durable:simplefilesystem-durable-embedded:0.1.0")
```

Run the complete end-to-end suite, which starts isolated single-node CockroachDB instances and uses the real Blobstore in-memory implementation:

```bash
scripts/test.bash --test . --log test_log_file.xml
```

## Programmatic example

```kotlin
import blobstore.api.BlobstoreService
import simplefilesystem.SimpleFileSystem
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

fun useDurableFilesystem(blobstore: BlobstoreService, metadataDatabase: Database) {
    val manager = DurableSimpleFileSystemManager(blobstore, metadataDatabase)
    val descriptor = manager.createFilesystem(
        description = "build-cache",
        maxSizeBytes = 100L * 1024 * 1024,
    )

    val filesystem: SimpleFileSystem = manager.openFilesystem(descriptor.uuid)
    filesystem.createDirectories("/artifacts/jvm", mustCreate = false)
    val committed = filesystem.writeUtf8("/artifacts/jvm/result.txt", "compiled", ifMatches = null)
    println("Committed ${committed.size} bytes as ${committed.contentHash}")

    val hash = requireNotNull(filesystem.metadata("/artifacts/jvm/result.txt").contentHash)
    filesystem.writeUtf8("/artifacts/jvm/result.txt", "recompiled", ifMatches = hash)
    println(filesystem.readUtf8("/artifacts/jvm/result.txt"))

    val maintenance = manager.runMaintenance()
    println("Reaped ${maintenance.reapedSessions} sessions and purged ${maintenance.purgedFilesystems} filesystems")
}
```

Callers own the injected services and database handle; the manager does not close them. Every returned `Source`, `FileSink`, and `InputStream` must be closed by its caller. A `FileSink` publishes data only when `commit()` succeeds; `abort()` discards an open stage, and `close()` aborts an uncommitted stage. Successful and failed commits are repeatable.

Directory and manager listings are cursor-paginated and expose durable snapshot revisions. Paths, filesystem descriptions, Base64, and UTF-8 are validated strictly; inline conveniences are capped at 4 MiB, while `source` and `FileSink` provide streaming access for larger files.

## Maintenance and reconciliation

Maintenance is explicit by default. Call `reapAbandonedWriteSessions()`, `reapAbandonedReaderSessions()`, `purgeExpiredFilesystems()`, and `processBlobGcOutbox()` separately, or call `runMaintenance()` for one bounded pass. Every manager construction also performs crash reconciliation: expired or explicitly aborted sessions are reaped, unreferenced pins are inventoried, and previously committed GC-outbox intents are resumed, while expiration purge remains explicit so an expired-but-unpurged filesystem can still be revived with `setExpiration(uuid, null)`. Orphan inventory persists a lexical hash high-water mark, so each invocation resumes at the next bounded range and eventually wraps to the beginning.

The current `BlobstoreApi` returns `listBlobs(owner)` as an already-materialized list. This module scans that response once without copying or sorting it and retains only the next maintenance batch in a bounded ordered set, but it cannot prevent the Blobstore client/service from materializing the original response. A future Blobstore API should expose `listBlobs(owner, afterHash, limit)` or an equivalent streaming cursor so allocation is bounded across the entire call chain.

Background maintenance is opt-in through `maintenanceIntervalMillis`; the default is `null`, so constructing a manager does not start recurring work. The interval is converted to the absolute deadline required by `Clock.schedule`, and `close()` cancels the scheduled callback without shutting down the caller-owned clock:

```kotlin
val manager = DurableSimpleFileSystemManager(
    blobstoreService = blobstore,
    metadataDatabase = metadataDatabase,
    maintenanceIntervalMillis = 60_000L,
)
try {
    // Use the manager and its filesystems.
} finally {
    manager.close()
}
```

## Durability model

Namespace changes, append assembly, generation release, recursive move/delete, and expiration purge remain one Cockroach transaction and therefore one logical transition. Large block and subtree sets are traversed with fixed-size keyset pages inside that transaction; intermediate pages are not externally visible, and this service does not retain a collection proportional to file or subtree cardinality.

- `filesystems` stores UUID identity, free-text descriptions, quota accounting, expiration, and a durable per-filesystem namespace revision.
- `simple_filesystem_manager_state` stores the durable manager-descriptor revision used by filesystem-listing pages.
- `entries` stores the directory tree and atomically points files at immutable generation UUIDs.
- `file_blocks` maps each generation to ordered 4 MiB Blobstore blocks and tracks references from entries and open readers.
- `write_sessions` records staged uploads, terminal state, and a renewable lease. Chunk writes renew the lease; reaping deletes an expired session's staged `file_blocks`, queues their hashes for GC evaluation, and leaves the durable session row in `REAPED` state without changing quota.
- `reader_sessions` gives each open `Source` or `InputStream` a renewable generation pin. Reaching EOF or closing releases it; maintenance reaps abandoned expired readers. Therefore deletion, overwrite, purge, and Blobstore GC cannot invalidate a reader that was already opened and continues making progress.
- `blob_gc_outbox` records blocks that may be unpinned after a generation loses its final reference. Writers and collectors serialize on each hash's outbox row; writers re-pin after acquiring the row lock before publishing `file_blocks`, while collectors re-check every committed and staged SQL reference before an idempotent unpin.
- Expiration is a two-step lifecycle: operations reject an expired filesystem, but callers may revive it until `purgeExpiredFilesystems()` locks and deletes it. Purge releases generations and staged blocks through the same GC outbox, so a content hash shared by another filesystem stays pinned.

Blobstore access uses the raw `BlobstoreService` API rather than `BlobstoreClient`, so blocks are neither encrypted nor compressed by this module. Production wiring is expected to supply a dedicated metadata database and a stable service-owned Blobstore pin identity.

## CI

Kompile build and test verification is provided by the external `kotlin.build (remote)` GitHub check. The legacy Actions workflow remains checked in only as a disabled family-convention reference.
