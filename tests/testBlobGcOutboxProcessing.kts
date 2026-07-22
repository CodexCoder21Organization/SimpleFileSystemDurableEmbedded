@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-support:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("blobstore.api:blobstore-api:0.0.2")
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
import community.kotlin.clocks.simple.ManualClock
import community.kotlin.clocks.simple.SystemClock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import sql.Database
import simplefilesystem.durable.testing.ControlledBlobstoreService

fun testBlobGcOutboxProcessing() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val blobs = ControlledBlobstoreService()
            val manager = DurableSimpleFileSystemManager(blobs, database, ManualClock(1_000L))
            val uuid = manager.createFilesystem("gc-outbox", 10_000L).uuid
            val filesystem = manager.openFilesystem(uuid)

            val sharedHash = filesystem.writeUtf8("/source", "shared", null).contentHash!!
            filesystem.copy("/source", "/copy")
            filesystem.overwriteUtf8("/source", "replacement")
            manager.processBlobGcOutbox()
            assertTrue(blobs.delegate.isPinned("simplefilesystem-durable-embedded", sharedHash))
            assertEquals(0, blobs.unpinCounts[sharedHash]?.get() ?: 0)

            filesystem.delete("/copy", true)
            manager.processBlobGcOutbox()
            assertFalse(blobs.delegate.isPinned("simplefilesystem-durable-embedded", sharedHash))
            assertEquals(1, blobs.unpinCounts[sharedHash]?.get())
            manager.processBlobGcOutbox()
            assertEquals(1, blobs.unpinCounts[sharedHash]?.get())

            val readdedHash = filesystem.writeUtf8("/old", "re-added", null).contentHash!!
            filesystem.overwriteUtf8("/old", "new value")
            filesystem.writeUtf8("/new-reference", "re-added", null)
            manager.processBlobGcOutbox()
            assertTrue(blobs.delegate.isPinned("simplefilesystem-durable-embedded", readdedHash))
            assertEquals(0, blobs.unpinCounts[readdedHash]?.get() ?: 0)

            val writerDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            try {
                val writerManager = DurableSimpleFileSystemManager(blobs, writerDatabase, ManualClock(1_000L))
                val writerFilesystem = writerManager.openFilesystem(uuid)
                val racingHash = filesystem.writeUtf8("/race-old", "racing content", null).contentHash!!
                filesystem.overwriteUtf8("/race-old", "superseded")
                val unpinEntered = CountDownLatch(1)
                val writerPinObserved = CountDownLatch(1)
                val allowUnpin = CountDownLatch(1)
                blobs.beforeUnpin = { hash ->
                    if (hash == racingHash) {
                        unpinEntered.countDown()
                        allowUnpin.await()
                    }
                }
                blobs.beforePin = { hash ->
                    if (hash == racingHash && unpinEntered.count == 0L) writerPinObserved.countDown()
                }
                val failure = AtomicReference<Throwable?>()
                val collector = Thread {
                    try {
                        manager.processBlobGcOutbox()
                    } catch (problem: Throwable) {
                        failure.compareAndSet(null, problem)
                    }
                }
                collector.start()
                unpinEntered.await()
                val writer = Thread {
                    try {
                        writerFilesystem.writeUtf8("/race-new", "racing content", null)
                    } catch (problem: Throwable) {
                        failure.compareAndSet(null, problem)
                    }
                }
                writer.start()
                writerPinObserved.await()
                allowUnpin.countDown()
                collector.join()
                writer.join()
                failure.get()?.let { throw it }
                assertTrue(blobs.delegate.isPinned("simplefilesystem-durable-embedded", racingHash))
                assertEquals("racing content", filesystem.readUtf8("/race-new"))
                assertEquals(1, blobs.unpinCounts[racingHash]?.get())
            } finally {
                blobs.beforePin = null
                blobs.beforeUnpin = null
                writerDatabase.close()
            }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
