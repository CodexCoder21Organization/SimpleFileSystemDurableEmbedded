@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("cockroachdb.testharness:cockroachdb-test-harness:0.0.4")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("com.squareup.okio:okio-jvm:3.4.0")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import community.kotlin.clocks.simple.SystemClock
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import okio.Buffer
import sql.Database

fun testAbandonedSessionReaping() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
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
            abandoned.write(Buffer().write(ByteArray(4 * 1024 * 1024) { 9 }), 4L * 1024L * 1024L)
            val abandonedHash = database.getStrings(
                "SELECT blob_hash FROM file_blocks WHERE generation_uuid IN " +
                    "(SELECT session_uuid FROM write_sessions WHERE path = '/abandoned')",
            ).single()

            clock.advanceBy(101L)
            assertEquals(1, manager.reapAbandonedWriteSessions())
            assertEquals(listOf("REAPED"), database.getStrings("SELECT state FROM write_sessions WHERE path = '/abandoned'"))
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
            assertEquals(listOf("OPEN"), database.getStrings("SELECT state FROM write_sessions WHERE path = '/active'"))
            active.abort()

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
            scheduledSink.write(Buffer().write(ByteArray(4 * 1024 * 1024) { 3 }), 4L * 1024L * 1024L)
            val scheduledHash = database.getStrings(
                "SELECT blob_hash FROM file_blocks WHERE generation_uuid IN " +
                    "(SELECT session_uuid FROM write_sessions WHERE path = '/scheduled-abandon')",
            ).single()
            scheduledClock.advanceBy(9L)
            assertTrue(scheduledBlobs.isPinned("simplefilesystem-durable-embedded", scheduledHash))
            scheduledClock.advanceBy(1L)
            assertFalse(scheduledBlobs.isPinned("simplefilesystem-durable-embedded", scheduledHash))
            assertEquals(listOf("REAPED"), database.getStrings(
                "SELECT state FROM write_sessions WHERE path = '/scheduled-abandon'",
            ))
            scheduledManager.close()
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
