@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
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
import kotlin.test.assertTrue
import simplefilesystem.FileContentConflictException
import sql.Database

fun testConcurrentConditionalWrite() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(11L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("concurrent CAS", 1_000L).uuid)
            filesystem.writeUtf8("/value", "before", null)
            val expected = filesystem.metadata("/value").contentHash!!
            val ready = CountDownLatch(2)
            val start = CountDownLatch(1)
            val executor = Executors.newFixedThreadPool(2)
            try {
                val futures = listOf("first", "second").map { content ->
                    executor.submit<Pair<String, Throwable?>> {
                        ready.countDown()
                        start.await()
                        content to try {
                            filesystem.writeUtf8("/value", content, expected)
                            null
                        } catch (failure: Throwable) {
                            failure
                        }
                    }
                }
                ready.await()
                start.countDown()
                val outcomes = futures.map { it.get() }
                assertEquals(1, outcomes.count { it.second == null })
                val failure = outcomes.single { it.second != null }.second!!
                assertEquals("simplefilesystem.FileContentConflictException", failure.javaClass.name)
                assertTrue(filesystem.readUtf8("/value") in setOf("first", "second"))
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
