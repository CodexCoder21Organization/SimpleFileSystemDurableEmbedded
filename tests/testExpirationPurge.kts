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
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import simplefilesystem.FilesystemNotFoundException
import sql.Database

fun testExpirationPurge() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(10_000L)
            val blobs = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(blobs, database, clock)

            val revived = manager.createFilesystem("revived", 10_000L)
            manager.openFilesystem(revived.uuid).writeUtf8("/kept", "revive me", null)
            manager.setExpiration(revived.uuid, 10_010L)
            clock.advanceBy(10L)
            manager.setExpiration(revived.uuid, null)

            val sharedKeeper = manager.createFilesystem("shared keeper", 10_000L)
            val sharedExpired = manager.createFilesystem("shared expired", 10_000L)
            val sharedHash = manager.openFilesystem(sharedKeeper.uuid).writeUtf8("/shared", "same bytes", null).contentHash!!
            manager.openFilesystem(sharedExpired.uuid).writeUtf8("/shared", "same bytes", null)
            manager.setExpiration(sharedExpired.uuid, 10_010L)

            val uniqueExpired = manager.createFilesystem("unique expired", 10_000L)
            val uniqueHash = manager.openFilesystem(uniqueExpired.uuid).writeUtf8("/unique", "unique bytes", null).contentHash!!
            manager.setExpiration(uniqueExpired.uuid, 10_010L)

            assertEquals(2, manager.purgeExpiredFilesystems())
            manager.processBlobGcOutbox()
            assertNull(manager.getExpiration(revived.uuid))
            assertEquals("revive me", manager.openFilesystem(revived.uuid).readUtf8("/kept"))
            assertFailsWith<FilesystemNotFoundException> { manager.getFilesystemInfo(sharedExpired.uuid) }
            assertFailsWith<FilesystemNotFoundException> { manager.openFilesystem(uniqueExpired.uuid) }
            assertTrue(blobs.isPinned("simplefilesystem-durable-embedded", sharedHash))
            assertFalse(blobs.isPinned("simplefilesystem-durable-embedded", uniqueHash))
            assertEquals("same bytes", manager.openFilesystem(sharedKeeper.uuid).readUtf8("/shared"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
