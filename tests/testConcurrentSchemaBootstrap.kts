@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import simplefilesystem.durable.testing.SharedCockroachCluster
import sql.Database

fun testConcurrentSchemaBootstrap() {
    val cluster = SharedCockroachCluster().start()
    val databases = (0 until 8).map {
        Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
    }
    val executor = Executors.newFixedThreadPool(databases.size)
    val ready = CountDownLatch(databases.size)
    val start = CountDownLatch(1)
    try {
        val managers = databases.map { database ->
            executor.submit<DurableSimpleFileSystemManager> {
                ready.countDown()
                check(start.await(10, TimeUnit.SECONDS)) {
                    "Concurrent schema bootstrap workers were not released within 10 seconds."
                }
                DurableSimpleFileSystemManager(
                    InMemoryBlobstoreService(),
                    database,
                    ManualClock(25_000L),
                )
            }
        }
        check(ready.await(10, TimeUnit.SECONDS)) {
            "Not all concurrent schema bootstrap workers became ready within 10 seconds."
        }
        start.countDown()
        val constructed = managers.map { it.get(20, TimeUnit.SECONDS) }
        try {
            val created = constructed.first().createFilesystem("concurrent bootstrap", 1_000L)
            assertEquals(
                listOf(created.uuid),
                constructed.last().listFilesystems(null, 100).filesystems.map { it.uuid },
            )
        } finally {
            constructed.forEach { it.close() }
        }
    } finally {
        start.countDown()
        executor.shutdownNow()
        databases.forEach { it.close() }
        cluster.close()
    }
}
