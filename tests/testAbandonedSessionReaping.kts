@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("com.squareup.okio:okio-jvm:3.4.0")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import java.security.MessageDigest
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import okio.Buffer
import sql.Database

fun testAbandonedSessionReaping() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(2_000L)
            val blobs = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                clock = clock,
                sessionLeaseMillis = 100L,
            )
            val uuid = manager.createFilesystem("session-reaper", 20L * 1024L * 1024L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val abandoned = filesystem.sink("/abandoned", null)
            val abandonedBytes = ByteArray(4 * 1024 * 1024) { 9 }
            abandoned.write(Buffer().write(abandonedBytes), abandonedBytes.size.toLong())
            val abandonedHash = MessageDigest.getInstance("SHA-256").digest(abandonedBytes)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }

            clock.advanceBy(101L)
            assertEquals(1, manager.reapAbandonedWriteSessions())
            assertEquals(0L, manager.getUsedBytes(uuid))
            assertFalse(filesystem.exists("/abandoned"))
            manager.processBlobGcOutbox()
            assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", abandonedHash))

            val active = filesystem.sink("/active", null)
            active.write(Buffer().writeByte(1), 1L)
            clock.advanceBy(75L)
            active.write(Buffer().writeByte(2), 1L)
            clock.advanceBy(75L)
            assertEquals(0, manager.reapAbandonedWriteSessions())
            active.commit()
            assertEquals("\u0001\u0002", filesystem.readUtf8("/active"))

            val scheduledClock = ManualClock(5_000L)
            val scheduledBlobs = InMemoryBlobstoreService()
            val scheduledManager = DurableSimpleFileSystemManager(
                blobstoreService = scheduledBlobs,
                metadataDatabase = database,
                clock = scheduledClock,
                sessionLeaseMillis = 5L,
                maintenanceIntervalMillis = 10L,
            )
            val scheduledUuid = scheduledManager.createFilesystem("scheduled", 10L * 1024L * 1024L).uuid
            val scheduledSink = scheduledManager.openFilesystem(scheduledUuid).sink("/scheduled-abandon", null)
            val scheduledBytes = ByteArray(4 * 1024 * 1024) { 3 }
            scheduledSink.write(Buffer().write(scheduledBytes), scheduledBytes.size.toLong())
            val scheduledHash = MessageDigest.getInstance("SHA-256").digest(scheduledBytes)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            scheduledClock.advanceBy(9L)
            assertTrue(scheduledBlobs.isPinned("simplefilesystem-durable-embedded", scheduledHash))
            scheduledClock.advanceBy(1L)
            assertFalse(scheduledBlobs.isPinned("simplefilesystem-durable-embedded", scheduledHash))
            assertFalse(scheduledManager.openFilesystem(scheduledUuid).exists("/scheduled-abandon"))
            scheduledManager.close()
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
