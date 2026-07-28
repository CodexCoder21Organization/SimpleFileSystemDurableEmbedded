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
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import simplefilesystem.FilesystemExpiredException
import simplefilesystem.InvalidCursorException
import simplefilesystem.InvalidFilesystemDescriptionException
import simplefilesystem.InvalidFilesystemUuidException
import simplefilesystem.InvalidMaxSizeBytesException
import simplefilesystem.InvalidPageLimitException

fun testFilesystemIdentityAndExpiration() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val clock = ManualClock(5_000L)
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, clock)
            val first = manager.createFilesystem("duplicate description", 100L)
            val second = manager.createFilesystem("duplicate description", 200L)

            assertEquals(2, manager.listFilesystems(null, 100).filesystems.size)
            assertEquals(
                setOf(first.uuid, second.uuid),
                manager.listFilesystems(null, 100).filesystems.map { it.uuid }.toSet(),
            )
            assertEquals("duplicate description", first.description)
            assertNull(first.owner)
            assertFailsWith<InvalidFilesystemDescriptionException> {
                manager.createFilesystem("\uD800", 1L)
            }
            assertFailsWith<InvalidFilesystemDescriptionException> {
                manager.createFilesystem("é".repeat(513), 1L)
            }
            val invalidSize = runCatching { manager.createFilesystem("bad", 0L) }.exceptionOrNull()
            assertEquals("simplefilesystem.InvalidMaxSizeBytesException", invalidSize?.javaClass?.name)
            val invalidUuid = runCatching { manager.openFilesystem(first.uuid.uppercase()) }.exceptionOrNull()
            assertEquals("simplefilesystem.InvalidFilesystemUuidException", invalidUuid?.javaClass?.name)

            manager.setExpiration(first.uuid, 5_001L)
            assertEquals(5_001L, manager.getExpiration(first.uuid))
            val handleThatWillExpire = manager.openFilesystem(first.uuid)
            clock.advanceBy(1L)
            val expired = runCatching { manager.openFilesystem(first.uuid) }.exceptionOrNull()
            assertEquals("simplefilesystem.FilesystemExpiredException", expired?.javaClass?.name)
            assertEquals(
                "Filesystem '${first.uuid}' expired at epoch millisecond 5001; the attempted operation was " +
                    "observed at epoch millisecond 5001 and is therefore not allowed.",
                expired?.message,
            )
            val expiredRootNoOp = runCatching {
                handleThatWillExpire.createDirectories("/", mustCreate = false)
            }.exceptionOrNull()
            assertEquals("simplefilesystem.FilesystemExpiredException", expiredRootNoOp?.javaClass?.name)
            assertEquals(expired?.message, expiredRootNoOp?.message)

            manager.setExpiration(first.uuid, null)
            assertNull(manager.getExpiration(first.uuid))
            val active = manager.openFilesystem(first.uuid)

            val beforeUsageChange = manager.listFilesystems(null, 100).snapshotRevision
            active.writeUtf8("/changes-descriptor-usage", "x", null)
            val afterUsageChange = manager.listFilesystems(null, 100)
            assertTrue(afterUsageChange.snapshotRevision > beforeUsageChange)
            assertEquals(afterUsageChange.filesystems.map { it.uuid }.sorted(), afterUsageChange.filesystems.map { it.uuid })

            val firstPage = manager.listFilesystems(null, 1)
            val secondPage = manager.listFilesystems(firstPage.nextAfter, 1)
            assertEquals(firstPage.snapshotRevision, secondPage.snapshotRevision)
            val deletedBoundary = firstPage.filesystems.single().uuid
            manager.deleteFilesystem(deletedBoundary)
            val afterDeletedBoundary = manager.listFilesystems(firstPage.nextAfter, 100)
            assertTrue(afterDeletedBoundary.filesystems.none { it.uuid == deletedBoundary })
            assertFailsWith<InvalidPageLimitException> { manager.listFilesystems(null, 0) }
            assertFailsWith<InvalidCursorException> { manager.listFilesystems("bad", 1) }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
