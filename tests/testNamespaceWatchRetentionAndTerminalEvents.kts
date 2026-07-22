@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem:simplefilesystem-api:0.3.0")
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
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import simplefilesystem.PathWatchResyncRequiredException
import simplefilesystem.PathWatchTerminalReason
import sql.Database

fun testNamespaceWatchRetentionAndTerminalEvents() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(300L)
            val manager = DurableSimpleFileSystemManager(
                blobstoreService = InMemoryBlobstoreService(),
                metadataDatabase = database,
                clock = clock,
                namespaceEventRetentionCount = 2,
                namespaceEventRetentionMillis = 5L,
            )
            val truncatedUuid = manager.createFilesystem("truncated", 10_000L).uuid
            val truncated = manager.openFilesystem(truncatedUuid)
            truncated.writeUtf8("/watched", "one", null)
            truncated.overwriteUtf8("/watched", "two")
            truncated.overwriteUtf8("/watched", "three")
            val countResync = assertFailsWith<PathWatchResyncRequiredException> {
                manager.watchFilesystem(truncatedUuid, "/watched", 0L, 100)
            }
            assertEquals(1L, countResync.oldestAvailableSinceRevision)
            assertEquals(3L, countResync.latestRevision)
            val initial = manager.watchFilesystem(truncatedUuid, "/watched", null, 100)
            assertEquals(3L, initial.events.single().revision)
            assertEquals(3L, initial.nextSinceRevision)

            clock.advanceBy(6L)
            truncated.overwriteUtf8("/watched", "four")
            val ageResync = assertFailsWith<PathWatchResyncRequiredException> {
                manager.watchFilesystem(truncatedUuid, "/watched", 2L, 100)
            }
            assertEquals(3L, ageResync.oldestAvailableSinceRevision)
            assertEquals(4L, ageResync.latestRevision)

            val deletedUuid = manager.createFilesystem("deleted", 10_000L).uuid
            manager.openFilesystem(deletedUuid).writeUtf8("/value", "value", null)
            manager.deleteFilesystem(deletedUuid)
            val deletedTerminal = manager.watchFilesystem(deletedUuid, "/value", 1L, 100).events.single()
            assertEquals(2L, deletedTerminal.revision)
            assertEquals("/", deletedTerminal.path)
            assertFalse(deletedTerminal.exists)
            assertEquals(PathWatchTerminalReason.FILESYSTEM_DELETED, deletedTerminal.terminalReason)

            val expiredUuid = manager.createFilesystem("expired", 10_000L).uuid
            manager.setExpiration(expiredUuid, clock.currentTimeMillis())
            val expiredTerminal = manager.watchFilesystem(expiredUuid, "/anything", 0L, 100).events.single()
            assertEquals(PathWatchTerminalReason.FILESYSTEM_EXPIRED, expiredTerminal.terminalReason)
            manager.setExpiration(expiredUuid, null)

            val purgedUuid = manager.createFilesystem("purged", 10_000L).uuid
            manager.openFilesystem(purgedUuid).writeUtf8("/value", "value", null)
            manager.setExpiration(purgedUuid, clock.currentTimeMillis())
            assertEquals(1, manager.purgeExpiredFilesystems())
            val purgedTerminal = manager.watchFilesystem(purgedUuid, "/value", 2L, 100).events.single()
            assertEquals(3L, purgedTerminal.revision)
            assertEquals(PathWatchTerminalReason.FILESYSTEM_PURGED, purgedTerminal.terminalReason)
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
