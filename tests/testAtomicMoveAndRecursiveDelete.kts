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
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import simplefilesystem.DirectoryNotEmptyException
import simplefilesystem.FileEntryType
import simplefilesystem.InvalidMoveException
import simplefilesystem.PathTypeMismatchException
import sql.Database

fun testAtomicMoveAndRecursiveDelete() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(8L))
            val uuid = manager.createFilesystem("trees", 1_000L).uuid
            val filesystem = manager.openFilesystem(uuid)
            filesystem.createDirectories("/from/nested", true)
            filesystem.writeUtf8("/from/nested/a", "alpha", null)
            filesystem.writeUtf8("/replace", "old", null)

            assertFailsWith<DirectoryNotEmptyException> { filesystem.delete("/from", true) }
            assertFailsWith<InvalidMoveException> { filesystem.atomicMove("/", "/root-copy") }
            assertFailsWith<InvalidMoveException> { filesystem.atomicMove("/replace", "/") }
            assertFailsWith<InvalidMoveException> {
                filesystem.atomicMove("/from", "/from/nested/descendant")
            }
            filesystem.createDirectories("/occupied/child", true)
            assertFailsWith<DirectoryNotEmptyException> { filesystem.atomicMove("/from", "/occupied") }
            filesystem.createDirectory("/target-directory", true)
            val crossKind = assertFailsWith<PathTypeMismatchException> {
                filesystem.atomicMove("/replace", "/target-directory")
            }
            assertEquals(FileEntryType.REGULAR_FILE, crossKind.expectedType)
            assertEquals(FileEntryType.DIRECTORY, crossKind.observedType)

            filesystem.atomicMove("/from", "/moved")
            assertFalse(filesystem.exists("/from"))
            assertEquals("alpha", filesystem.readUtf8("/moved/nested/a"))
            filesystem.atomicMove("/moved/nested/a", "/replace")
            assertEquals("alpha", filesystem.readUtf8("/replace"))
            assertEquals(5L, manager.getUsedBytes(uuid))
            assertTrue(database.getLong("SELECT count(*) FROM blob_gc_outbox")!! >= 1L)

            filesystem.deleteRecursively("/moved", true)
            filesystem.delete("/replace", true)
            filesystem.deleteRecursively("/", true)
            assertFalse(filesystem.exists("/moved"))
            assertEquals(0L, manager.getUsedBytes(uuid))
            assertEquals(emptyList(), filesystem.list("/", null, 100).entries.map { it.path })
            assertTrue(filesystem.exists("/"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
