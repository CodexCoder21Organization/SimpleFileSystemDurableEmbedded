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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import okio.Buffer
import simplefilesystem.QuotaExceededException
import simplefilesystem.QuotaArithmeticOverflowException

fun testQuotaAndConcurrentWrites() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(4L))
            val uuid = manager.createFilesystem("quota", 12L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val ready = CountDownLatch(2)
            val start = CountDownLatch(1)
            val executor = Executors.newFixedThreadPool(2)
            try {
                val futures = listOf("/a", "/b").map { path ->
                    executor.submit<Throwable?> {
                        ready.countDown()
                        start.await()
                        try {
                            filesystem.writeUtf8(path, "12345678", null)
                            null
                        } catch (failure: Throwable) {
                            failure
                        }
                    }
                }
                ready.await()
                start.countDown()
                val failures = futures.map { it.get() }
                assertEquals(1, failures.count { it == null })
                val quotaFailure = failures.single { it != null }!!
                assertEquals("simplefilesystem.QuotaExceededException", quotaFailure.javaClass.name)
                assertEquals(8L, manager.getUsedBytes(uuid))
                assertEquals(1, listOf("/a", "/b").count { filesystem.exists(it) })
                assertEquals(
                    "Cannot mutate path '${listOf("/a", "/b").single { !filesystem.exists(it) }}': the " +
                        "filesystem limit is 12 bytes and current usage is 8 bytes, but the mutation would " +
                        "change usage to 16 bytes.",
                    quotaFailure.message,
                )

                val rejected = filesystem.sink("/too-large", null)
                rejected.write(Buffer().writeUtf8("12345678"), 8L)
                val commitFailure = assertFailsWith<QuotaExceededException> { rejected.commit() }
                assertSame(commitFailure, assertFailsWith<QuotaExceededException> { rejected.commit() })
                rejected.close()

                database.execute(
                    "UPDATE filesystems SET max_size_bytes = ?, used_bytes = ? WHERE uuid = ?",
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    java.util.UUID.fromString(uuid),
                )
                val overflow = filesystem.sink("/overflow", null)
                overflow.write(Buffer().writeByte(1), 1L)
                assertFailsWith<QuotaArithmeticOverflowException> { overflow.commit() }
            } finally {
                executor.shutdownNow()
            }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
