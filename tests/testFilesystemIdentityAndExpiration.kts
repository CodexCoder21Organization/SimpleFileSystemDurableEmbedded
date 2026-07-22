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
import kotlin.test.assertNull
import simplefilesystem.FilesystemExpiredException
import simplefilesystem.InvalidFilesystemUuidException
import simplefilesystem.InvalidMaxSizeBytesException
import sql.Database

fun testFilesystemIdentityAndExpiration() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(5_000L)
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, clock)
            val first = manager.createFilesystem("duplicate description", 100L)
            val second = manager.createFilesystem("duplicate description", 200L)

            assertEquals(2, manager.listFilesystems().size)
            assertEquals(setOf(first.uuid, second.uuid), manager.listFilesystems().map { it.uuid }.toSet())
            assertEquals("duplicate description", first.description)
            assertNull(first.owner)
            assertFailsWith<InvalidMaxSizeBytesException> { manager.createFilesystem("bad", 0L) }
            assertFailsWith<InvalidFilesystemUuidException> { manager.openFilesystem(first.uuid.uppercase()) }

            manager.setExpiration(first.uuid, 5_001L)
            assertEquals(5_001L, manager.getExpiration(first.uuid))
            clock.advanceBy(1L)
            val expired = assertFailsWith<FilesystemExpiredException> { manager.openFilesystem(first.uuid) }
            assertEquals(first.uuid, expired.uuid)
            assertEquals(5_001L, expired.expiresAtMillis)
            assertEquals(5_001L, expired.observedAtMillis)

            manager.setExpiration(first.uuid, null)
            assertNull(manager.getExpiration(first.uuid))
            manager.openFilesystem(first.uuid)
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
