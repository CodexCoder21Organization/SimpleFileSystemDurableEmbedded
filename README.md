# SimpleFileSystemDurableEmbedded

Durable, in-process implementation of the [`simplefilesystem`](https://github.com/CodexCoder21Organization/SimpleFileSystemApi) 0.3.0 contract. File and directory metadata is stored transactionally in CockroachDB while immutable 4 MiB content blocks are stored directly in Blobstore by uppercase SHA-256 hash.

`DurableSimpleFileSystemManager` implements the multi-filesystem lifecycle API and vends `DurableSimpleFileSystem` handles. Writes upload and pin staged blocks before one metadata transaction atomically publishes a new immutable generation, updates quota usage, and queues superseded blocks for idempotent garbage collection.

## Building

Build the Maven artifact from the repository root:

```bash
scripts/build.bash simplefilesystem.durable.buildMaven
```

The artifact coordinate is:

```kotlin
MavenPrebuilt("simplefilesystem.durable:simplefilesystem-durable-embedded:0.1.1")
```

Run the complete end-to-end suite, which starts one real in-memory single-node CockroachDB
fixture and gives every test an isolated logical database on that node. Tests also use the real
Blobstore in-memory implementation:

```bash
scripts/test.bash --test . --log test_log_file.xml
```

The test-support library also starts that shared node lazily when tests are dispatched directly
instead of through `scripts/test.bash`. A cross-process lock ensures the independently forked test
JVMs converge on one memory-bounded CockroachDB process, while per-test leases keep the process
alive until every isolated logical database is finished. Direct dispatch retains those uniquely
named databases only until the disposable in-memory node stops after its final lease; a
caller-supplied longer-lived fixture drops each logical database when its test closes.

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

    var cursor = manager.watchFilesystem(descriptor.uuid, "/artifacts/jvm/result.txt", null, 100)
        .nextSinceRevision
    val changed = manager.watchFilesystem(descriptor.uuid, "/artifacts/jvm/result.txt", cursor, 100)
    changed.events.forEach { event -> println("revision ${event.revision}: ${event.contentHash}") }
    cursor = changed.nextSinceRevision

    val maintenance = manager.runMaintenance()
    println("Reaped ${maintenance.reapedSessions} sessions and purged ${maintenance.purgedFilesystems} filesystems")
}
```

Callers own the injected services and database handle; the manager does not close them. Every returned `Source`, `FileSink`, and `InputStream` must be closed by its caller. A `FileSink` publishes data only when `commit()` succeeds; `abort()` discards an open stage, and `close()` aborts an uncommitted stage. Successful and failed commits are repeatable.

Directory and manager listings are cursor-paginated and expose durable snapshot revisions. Paths, filesystem descriptions, Base64, and UTF-8 are validated strictly; inline conveniences are capped at 4 MiB, while `source` and `FileSink` provide streaming access for larger files.

`watchFilesystem(uuid, path, sinceRevision, limit)` reads the durable, exact-path namespace event log. A null cursor returns one current snapshot; subsequent calls return committed events after the cursor. Creation and removal also emit a snapshot for the direct parent directory, and filesystem expiration, explicit deletion, and purge emit terminal root events. Events carry metadata and content hashes, never file contents, so consumers re-read when a hash changes. Advance with `nextSinceRevision`, discard duplicate revisions, and rebuild from a null cursor if `PathWatchResyncRequiredException` reports that the requested history has been retained out. A later server/Observables binding can repeatedly invoke this backend-neutral pull primitive without changing its persistence semantics.

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

Generation deletion uses pages of 16,384 blocks by default. Backends with a different transaction-size budget can set `generationDeleteBatchSize` when constructing `DurableSimpleFileSystemManager`; the value must be positive and does not change the atomic visibility of the transition.

- `filesystems` stores UUID identity, free-text descriptions, quota accounting, expiration, and a durable per-filesystem namespace revision.
- `simple_filesystem_manager_state` stores the durable manager-descriptor revision used by filesystem-listing pages.
- `namespace_event_streams` stores each filesystem's latest revision, oldest safe watch cursor, and terminal state even after the filesystem descriptor has been removed.
- `namespace_events` stores per-path snapshots at the same revision and in the same CockroachDB transaction as each namespace mutation. The default retention keeps seven days and the newest 10,000 event rows, pruning only whole revisions so a multi-path transaction is never partially visible. A single exceptionally wide mutation may therefore temporarily exceed the row target. Constructor settings `namespaceEventRetentionCount` and `namespaceEventRetentionMillis` may tighten these bounds. Once pruning advances the oldest safe cursor, older non-null cursors receive `PathWatchResyncRequiredException`; null always requests a fresh snapshot or the retained terminal event.
- `entries` stores the directory tree and atomically points files at immutable generation UUIDs.
- `file_blocks` maps each generation to ordered 4 MiB Blobstore blocks and tracks references from entries and open readers.
- `write_sessions` records staged uploads, terminal state, and a renewable lease. Chunk writes renew the lease; reaping deletes an expired session's staged `file_blocks`, queues their hashes for GC evaluation, and leaves the durable session row in `REAPED` state without changing quota.
- `reader_sessions` gives each open `Source` or `InputStream` a renewable generation pin. Reaching EOF or closing releases it; maintenance reaps abandoned expired readers. Therefore deletion, overwrite, purge, and Blobstore GC cannot invalidate a reader that was already opened and continues making progress.
- `blob_gc_outbox` records blocks that may be unpinned after a generation loses its final reference. Writers and collectors serialize on each hash's outbox row; writers re-pin after acquiring the row lock before publishing `file_blocks`, while collectors re-check every committed and staged SQL reference before an idempotent unpin.
- Expiration is a two-step lifecycle: operations reject an expired filesystem, but callers may revive it until `purgeExpiredFilesystems()` locks and deletes it. Purge releases generations and staged blocks through the same GC outbox, so a content hash shared by another filesystem stays pinned.

Blobstore access uses the raw `BlobstoreService` API rather than `BlobstoreClient`, so blocks are neither encrypted nor compressed by this module. Production wiring is expected to supply a dedicated metadata database and a stable service-owned Blobstore pin identity.

## CI

Kompile build and test verification is provided by the external `kotlin.build (remote)` GitHub check. The legacy Actions workflow remains checked in only as a disabled family-convention reference.
