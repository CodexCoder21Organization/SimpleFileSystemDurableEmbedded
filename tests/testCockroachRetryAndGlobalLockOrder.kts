@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
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
import community.kotlin.clocks.simple.ManualClock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import okio.Buffer
import simplefilesystem.FilesystemNotFoundException
import simplefilesystem.durable.testing.ControlledBlobstoreService
import sql.Database

fun testCockroachRetryAndGlobalLockOrder() {
    val cluster = SharedCockroachCluster().start()
    try {
        val primaryDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        val writerDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(80_000L)
            val blobs = ControlledBlobstoreService()
            val manager = DurableSimpleFileSystemManager(blobs, primaryDatabase, clock)
            val writerManager = DurableSimpleFileSystemManager(blobs, writerDatabase, clock)
            val retryUuid = manager.createFilesystem("forced 40001", 100_000L).uuid
            val filesystem = manager.openFilesystem(retryUuid)
            val writerFilesystem = writerManager.openFilesystem(retryUuid)
            val retryHash = filesystem.writeUtf8("/old", "retry-content", null).contentHash!!
            filesystem.overwriteUtf8("/old", "replacement")
            blobs.pinCounts[retryHash]?.set(0)
            blobs.unpinCounts[retryHash]?.set(0)

            val unpinEntered = CountDownLatch(1)
            val writerPinnedBeforeSql = CountDownLatch(1)
            val allowCollectorCommit = CountDownLatch(1)
            blobs.beforeUnpin = { hash ->
                if (hash == retryHash) {
                    unpinEntered.countDown()
                    assertTrue(allowCollectorCommit.await(10, TimeUnit.SECONDS))
                }
            }
            blobs.beforePin = { hash ->
                if (hash == retryHash && unpinEntered.count == 0L) writerPinnedBeforeSql.countDown()
            }
            val retryExecutor = Executors.newFixedThreadPool(2)
            try {
                val collector = retryExecutor.submit<Int> { manager.processBlobGcOutbox(1) }
                assertTrue(unpinEntered.await(10, TimeUnit.SECONDS))
                val writer = retryExecutor.submit { writerFilesystem.writeUtf8("/retried", "retry-content", null) }
                assertTrue(writerPinnedBeforeSql.await(10, TimeUnit.SECONDS))
                allowCollectorCommit.countDown()
                assertEquals(1, collector.get(10, TimeUnit.SECONDS))
                assertEquals("retry-content", writer.get(10, TimeUnit.SECONDS).let { writerFilesystem.readUtf8("/retried") })
            } finally {
                allowCollectorCommit.countDown()
                retryExecutor.shutdownNow()
                blobs.beforePin = null
                blobs.beforeUnpin = null
            }
            assertEquals(2, blobs.pinCounts[retryHash]?.get(), "One pre-SQL pin plus one retry compensation pin are required.")
            assertEquals(1, blobs.unpinCounts[retryHash]?.get(), "The retried transaction must not repeat collector unpin.")
            assertTrue(blobs.delegate.isPinned("simplefilesystem-durable-embedded", retryHash))

            val domainMessage = "configured Blobstore pin domain failure"
            blobs.beforePin = { throw IllegalStateException(domainMessage) }
            val domainFailure = runCatching { writerFilesystem.writeUtf8("/domain-failure", "unique", null) }.exceptionOrNull()
            blobs.beforePin = null
            assertEquals("java.lang.IllegalStateException", domainFailure?.javaClass?.name)
            assertEquals(domainMessage, domainFailure?.message)
            assertFalse(writerFilesystem.exists("/domain-failure"))

            repeat(4) { iteration ->
                val stressBlobs = ControlledBlobstoreService()
                val stressManager = DurableSimpleFileSystemManager(stressBlobs, primaryDatabase, clock)
                val stressWriterManager = DurableSimpleFileSystemManager(stressBlobs, writerDatabase, clock)
                val uuid = stressManager.createFilesystem("lock order $iteration", 100_000L).uuid
                val stressFilesystem = stressManager.openFilesystem(uuid)
                val stressWriter = stressWriterManager.openFilesystem(uuid)
                val garbageHash = stressFilesystem.writeUtf8("/garbage", "garbage-$iteration", null).contentHash!!
                stressFilesystem.overwriteUtf8("/garbage", "replacement-$iteration")
                stressFilesystem.writeUtf8("/log", "base", null)
                val appendSink = stressWriter.appendingSink("/log")
                appendSink.write(Buffer().writeUtf8("append-$iteration"), "append-$iteration".length.toLong())
                val stageSink = stressWriter.sink("/staged", null)
                stageSink.write(Buffer().writeUtf8("stage-$iteration"), "stage-$iteration".length.toLong())

                val externalPinsReached = CountDownLatch(2)
                val releaseExternalPins = CountDownLatch(1)
                stressBlobs.beforePin = {
                    externalPinsReached.countDown()
                    assertTrue(releaseExternalPins.await(10, TimeUnit.SECONDS))
                }
                val executor = Executors.newFixedThreadPool(4)
                try {
                    val commitsStarted = CountDownLatch(2)
                    val append = executor.submit {
                        commitsStarted.countDown()
                        runCatching { appendSink.commit() }.exceptionOrNull()
                    }
                    val staging = executor.submit {
                        commitsStarted.countDown()
                        runCatching { stageSink.commit() }.exceptionOrNull()
                    }
                    assertTrue(commitsStarted.await(10, TimeUnit.SECONDS))
                    assertTrue(externalPinsReached.await(10, TimeUnit.SECONDS))
                    stressManager.setExpiration(uuid, clock.currentTimeMillis())
                    val collector = executor.submit<Int> { stressManager.processBlobGcOutbox(10) }
                    val purge = executor.submit<Int> { stressManager.purgeExpiredFilesystems(1) }
                    releaseExternalPins.countDown()
                    val appendFailure = append.get(10, TimeUnit.SECONDS)
                    val stagingFailure = staging.get(10, TimeUnit.SECONDS)
                    assertTrue(
                        appendFailure == null || appendFailure is FilesystemNotFoundException,
                        "Append completed with unexpected failure: $appendFailure",
                    )
                    assertTrue(
                        stagingFailure == null || stagingFailure is FilesystemNotFoundException,
                        "Staging completed with unexpected failure: $stagingFailure",
                    )
                    collector.get(10, TimeUnit.SECONDS)
                    assertEquals(1, purge.get(10, TimeUnit.SECONDS))
                } finally {
                    releaseExternalPins.countDown()
                    executor.shutdownNow()
                    stressBlobs.beforePin = null
                }
                repeat(3) { stressManager.runMaintenance(100) }
                assertTrue(runCatching { stressManager.getFilesystemInfo(uuid) }.exceptionOrNull() is FilesystemNotFoundException)
                assertFalse(stressBlobs.delegate.isPinned("simplefilesystem-durable-embedded", garbageHash))
                stressManager.close()
                stressWriterManager.close()
            }
            manager.close()
            writerManager.close()
        } finally {
            writerDatabase.close()
            primaryDatabase.close()
        }
    } finally {
        cluster.close()
    }
}
