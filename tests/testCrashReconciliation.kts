@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
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
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import okio.Buffer

fun testCrashReconciliation() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
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

            val orphanBytes = ByteArray(4 * 1024 * 1024) { 7 }
            val orphanHash = MessageDigest.getInstance("SHA-256").digest(orphanBytes)
                .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
            val interruptedSink = filesystem.sink("/orphan", null)
            interruptedSink.write(Buffer().write(orphanBytes), orphanBytes.size.toLong())
            assertTrue(blobs.isPinned("simplefilesystem-durable-embedded", orphanHash))

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
            assertFalse(filesystem.exists("/orphan"))
            assertEquals("new committed bytes".toByteArray().size.toLong(), firstManager.getUsedBytes(uuid))
            assertEquals("new committed bytes", filesystem.readUtf8("/committed"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
