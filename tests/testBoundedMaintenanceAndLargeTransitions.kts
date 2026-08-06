@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import simplefilesystem.FilesystemNotFoundException

fun testBoundedMaintenanceAndLargeTransitions() {
    try {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val clock = ManualClock(50_000L)
            val blobs = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                clock = clock,
                maintenanceBatchSize = 3,
            )
            val uuid = manager.createFilesystem("bounded stress", 1_000_000L).uuid
            val filesystem = manager.openFilesystem(uuid)

            val orphanHashes = (0 until 8).map { index ->
                val bytes = "crash-leftover-$index".toByteArray()
                val hash = MessageDigest.getInstance("SHA-256").digest(bytes)
                    .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
                blobs.putBlob(
                    "simplefilesystem-durable-embedded",
                    hash,
                    bytes.size.toLong(),
                    ByteArrayInputStream(bytes),
                )
                assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", hash))
                hash
            }
            manager.runMaintenance(3)
            assertEquals(5, orphanHashes.count { blobs.isPinned("simplefilesystem-durable-embedded", it) })
            manager.close()

            val resumedManager = DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                clock = clock,
                maintenanceBatchSize = 3,
            )
            repeat(4) { resumedManager.runMaintenance(3) }
            orphanHashes.forEach { hash ->
                assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", hash))
            }

            val oneByte = byteArrayOf('x'.code.toByte())
            val oneByteHash = MessageDigest.getInstance("SHA-256").digest(oneByte)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            blobs.putBlob(
                "simplefilesystem-durable-embedded",
                oneByteHash,
                1L,
                ByteArrayInputStream(oneByte),
            )
            assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", oneByteHash))
            filesystem.writeUtf8("/large", "seed", null)
            // Set-based cardinality seed: constructing 12,000 public 4 MiB blocks would require 48 GiB.
            // This does not assert schema details; all behavior assertions below use public filesystem operations.
            val generation = database.getRows(
                "SELECT generation_uuid FROM entries WHERE filesystem_uuid = ?::UUID AND path = '/large'",
                uuid,
            ).single().results["generation_uuid"]
            val expectedPrefix = "x".repeat(12_000)
            val prefixHash = MessageDigest.getInstance("SHA-256").digest(expectedPrefix.toByteArray())
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            database.execute("DELETE FROM file_blocks WHERE generation_uuid = ?", generation)
            database.execute(
                """INSERT INTO file_blocks (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                    SELECT ?::UUID, value::INT4, ?, 1, 1
                    FROM generate_series(0, 11999) AS generated(value)""".trimIndent(),
                generation,
                oneByteHash,
            )
            database.execute(
                """UPDATE entries SET size_bytes = 12000, content_hash = ?
                    WHERE filesystem_uuid = ?::UUID AND path = '/large'""".trimIndent(),
                prefixHash,
                uuid,
            )
            database.execute("UPDATE filesystems SET used_bytes = 12000 WHERE uuid = ?::UUID", uuid)

            val appended = filesystem.appendingWriteUtf8("/large", "tail", true)
            assertEquals(12_004L, appended.size)
            assertEquals(expectedPrefix + "tail", filesystem.readUtf8("/large"))

            filesystem.createDirectory("/move", true)
            database.execute(
                """INSERT INTO entries
                    (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                     size_bytes, content_hash, created_at_millis, modified_at_millis)
                    SELECT ?::UUID, '/move/n' || lpad(value::STRING, 4, '0'), '/move',
                           'n' || lpad(value::STRING, 4, '0'), 'DIRECTORY', NULL, NULL, NULL, 50000, 50000
                    FROM generate_series(0, 2047) AS generated(value)""".trimIndent(),
                uuid,
            )
            filesystem.atomicMove("/move", "/moved")
            var after: String? = null
            var movedCount = 0
            do {
                val page = filesystem.listRecursively("/moved", after, 1_000)
                movedCount += page.entries.size
                after = page.nextAfter
            } while (after != null)
            assertEquals(2_048, movedCount)
            filesystem.deleteRecursively("/moved", true)
            assertFalse(filesystem.exists("/moved"))

            database.execute(
                """INSERT INTO entries
                    (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                     size_bytes, content_hash, created_at_millis, modified_at_millis)
                    SELECT ?::UUID, '/purge' || lpad(value::STRING, 4, '0'), '/',
                           'purge' || lpad(value::STRING, 4, '0'), 'DIRECTORY', NULL, NULL, NULL, 50000, 50000
                    FROM generate_series(0, 2047) AS generated(value)""".trimIndent(),
                uuid,
            )
            resumedManager.setExpiration(uuid, clock.currentTimeMillis())
            assertEquals(1, resumedManager.purgeExpiredFilesystems())
            val missing = runCatching { resumedManager.getFilesystemInfo(uuid) }.exceptionOrNull()
            assertTrue(missing is FilesystemNotFoundException)
            resumedManager.close()
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
    } catch (failure: Throwable) {
        throw AssertionError(failure.stackTraceToString())
    }
}
