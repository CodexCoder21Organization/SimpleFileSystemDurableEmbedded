@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("cockroachdb.testharness:cockroachdb-test-harness:0.0.4")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import community.kotlin.clocks.simple.SystemClock
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import sql.Database

fun testCrashReconciliation() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(20_000L)
            val blobs = InMemoryBlobstoreService()
            val firstManager = DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                clock = clock,
                sessionLeaseMillis = 100L,
            )
            val uuid = firstManager.createFilesystem("crash", 10_000L).uuid
            val filesystem = firstManager.openFilesystem(uuid)
            val oldHash = filesystem.writeUtf8("/committed", "old committed bytes", null).contentHash!!
            filesystem.overwriteUtf8("/committed", "new committed bytes")
            assertTrue(blobs.isPinned("simplefilesystem-durable-embedded", oldHash))
            assertEquals(1L, database.getLong("SELECT count(*) FROM blob_gc_outbox WHERE blob_hash = ?", oldHash))

            val orphanBytes = "pinned before SQL commit".toByteArray()
            val orphanHash = MessageDigest.getInstance("SHA-256").digest(orphanBytes)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            blobs.putBlob(
                "simplefilesystem-durable-embedded",
                orphanHash,
                orphanBytes.size.toLong(),
                ByteArrayInputStream(orphanBytes),
            )
            assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", orphanHash))
            val session = UUID.randomUUID()
            database.execute(
                """INSERT INTO write_sessions
                    (session_uuid, filesystem_uuid, path, expected_hash, bytes_received, created_at_millis,
                     lease_expires_at_millis, state)
                    VALUES (?, ?::UUID, '/orphan', NULL, ?, ?, ?, 'OPEN')""".trimIndent(),
                session,
                uuid,
                orphanBytes.size.toLong(),
                20_000L,
                20_100L,
            )
            database.execute(
                """INSERT INTO file_blocks
                    (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                    VALUES (?, 0, ?, ?, 0)""".trimIndent(),
                session,
                orphanHash,
                orphanBytes.size,
            )

            val pinOnlyBytes = "crashed before file_blocks insert".toByteArray()
            val pinOnlyHash = MessageDigest.getInstance("SHA-256").digest(pinOnlyBytes)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            blobs.putBlob(
                "simplefilesystem-durable-embedded",
                pinOnlyHash,
                pinOnlyBytes.size.toLong(),
                ByteArrayInputStream(pinOnlyBytes),
            )
            assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", pinOnlyHash))

            clock.advanceBy(101L)
            DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                clock = clock,
                sessionLeaseMillis = 100L,
            )

            assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", orphanHash))
            assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", pinOnlyHash))
            assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", oldHash))
            assertEquals(listOf("REAPED"), database.getStrings("SELECT state FROM write_sessions WHERE session_uuid = ?", session))
            assertEquals(0L, database.getLong("SELECT count(*) FROM file_blocks WHERE generation_uuid = ?", session))
            assertEquals(0L, database.getLong("SELECT count(*) FROM blob_gc_outbox"))
            assertEquals("new committed bytes", filesystem.readUtf8("/committed"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
