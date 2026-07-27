@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
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
            constructed.first().openFilesystem(created.uuid).writeUtf8("/value", "value", null)
            assertEquals(
                listOf(created.uuid),
                constructed.last().listFilesystems(null, 100).filesystems.map { it.uuid },
            )
            databases.first().execute(
                """DROP TABLE IF EXISTS namespace_event_revisions;
                    ALTER TABLE namespace_event_streams
                        DROP COLUMN IF EXISTS latest_event_ordinal;
                    ALTER TABLE namespace_event_streams
                        DROP COLUMN IF EXISTS retained_event_count;
                    ALTER TABLE namespace_event_streams
                        DROP COLUMN IF EXISTS oldest_retained_revision_last_event_ordinal;
                    ALTER TABLE namespace_event_streams
                        DROP COLUMN IF EXISTS oldest_event_at_millis;
                    UPDATE simple_filesystem_schema_version
                        SET schema_version = 1 WHERE singleton = true""".trimIndent(),
            )
            val migrationDatabases = (0 until databases.size).map {
                Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            }
            try {
                val migrationReady = CountDownLatch(migrationDatabases.size)
                val migrationStart = CountDownLatch(1)
                val migrations = migrationDatabases.map { database ->
                    executor.submit<DurableSimpleFileSystemManager> {
                        migrationReady.countDown()
                        check(migrationStart.await(10, TimeUnit.SECONDS)) {
                            "Concurrent schema migration workers were not released within 10 seconds."
                        }
                        DurableSimpleFileSystemManager(
                            InMemoryBlobstoreService(),
                            database,
                            ManualClock(25_000L),
                        )
                    }
                }
                check(migrationReady.await(10, TimeUnit.SECONDS)) {
                    "Not all concurrent schema migration workers became ready within 10 seconds."
                }
                migrationStart.countDown()
                val migrated = migrations.map { it.get(20, TimeUnit.SECONDS) }
                try {
                    assertEquals(
                        listOf(1L),
                        migrated.first()
                            .watchFilesystem(created.uuid, "/value", 0L, 100)
                            .events
                            .map { it.revision },
                    )
                    val stream = migrationDatabases.first().getRows(
                        """SELECT latest_event_ordinal, retained_event_count,
                                  oldest_retained_revision_last_event_ordinal
                            FROM namespace_event_streams WHERE filesystem_uuid = ?""".trimIndent(),
                        java.util.UUID.fromString(created.uuid),
                    ).single()
                    assertEquals(2L, (stream.results["latest_event_ordinal"] as Number).toLong())
                    assertEquals(2L, (stream.results["retained_event_count"] as Number).toLong())
                    assertEquals(
                        2L,
                        (stream.results["oldest_retained_revision_last_event_ordinal"] as Number).toLong(),
                    )
                    assertEquals(
                        listOf(2L),
                        migrationDatabases.last().getRows(
                            """SELECT event_count FROM namespace_event_revisions
                                WHERE filesystem_uuid = ? ORDER BY revision""".trimIndent(),
                            java.util.UUID.fromString(created.uuid),
                        ).map { (it.results["event_count"] as Number).toLong() },
                    )
                } finally {
                    migrated.forEach { it.close() }
                }
            } finally {
                migrationDatabases.forEach { it.close() }
            }
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
