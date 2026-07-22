# SimpleFileSystemDurableEmbedded

Durable, in-process implementation of the [`simplefilesystem`](https://github.com/CodexCoder21Organization/SimpleFileSystemApi) contract. File and directory metadata is stored transactionally in CockroachDB while immutable 4 MiB content blocks are stored directly in Blobstore by uppercase SHA-256 hash.

`DurableSimpleFileSystemManager` implements the multi-filesystem lifecycle API and vends `DurableSimpleFileSystem` handles. Writes upload and pin staged blocks before one metadata transaction atomically publishes a new immutable generation, updates quota usage, and queues superseded blocks for later garbage collection.

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
    filesystem.writeUtf8("/artifacts/jvm/result.txt", "compiled", ifMatches = null)

    val hash = requireNotNull(filesystem.metadata("/artifacts/jvm/result.txt").contentHash)
    filesystem.writeUtf8("/artifacts/jvm/result.txt", "recompiled", ifMatches = hash)
    println(filesystem.readUtf8("/artifacts/jvm/result.txt"))
}
```

Callers own the injected services and database handle; the manager does not close them. Every returned `Source`, `Sink`, and `InputStream` must be closed by its caller. `sink` commits only on a successful close, so an abandoned writer never exposes partial content or changes `usedBytes`.

## Durability model

- `filesystems` stores UUID identity, free-text descriptions, quota accounting, and expiration.
- `entries` stores the directory tree and atomically points files at immutable generation UUIDs.
- `file_blocks` maps each generation to ordered 4 MiB Blobstore blocks and tracks how many entries share that generation.
- `write_sessions` records staged uploads. A close transaction validates compare-and-swap and quota constraints before publishing.
- `blob_gc_outbox` records blocks that may be unpinned after a generation loses its final reference. Outbox processing, abandoned-session reaping, and expiration purge are intentionally phase-2 work.

Blobstore access uses the raw `BlobstoreService` API rather than `BlobstoreClient`, so blocks are neither encrypted nor compressed by this module. Production wiring is expected to supply a dedicated metadata database and a stable service-owned Blobstore pin identity.

## CI

Kompile build and test verification is provided by the external `kotlin.build (remote)` GitHub check. The legacy Actions workflow remains checked in only as a disabled family-convention reference.
