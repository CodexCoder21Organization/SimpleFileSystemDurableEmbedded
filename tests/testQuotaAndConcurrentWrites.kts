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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import simplefilesystem.QuotaExceededException
import sql.Database

fun testQuotaAndConcurrentWrites() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
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
                val quotaFailure = failures.single { it != null }
                assertIs<QuotaExceededException>(quotaFailure)
                assertEquals(12L, quotaFailure.maxSizeBytes)
                assertEquals(8L, quotaFailure.usedBytes)
                assertEquals(16L, quotaFailure.attemptedUsedBytes)
                assertEquals(8L, manager.getUsedBytes(uuid))
                assertEquals(1, listOf("/a", "/b").count { filesystem.exists(it) })
                assertTrue(quotaFailure.message!!.contains("current usage is 8 bytes"))
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
