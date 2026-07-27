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
import java.util.Base64
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import simplefilesystem.FileEntryType
import simplefilesystem.InvalidCursorException
import simplefilesystem.InvalidPageLimitException
import sql.Database

fun testDirectoriesWholeFilesAndMetadata() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(8_000L))
            val created = manager.createFilesystem("content", 1_000_000L)
            val filesystem = manager.openFilesystem(created.uuid)

            filesystem.createDirectories("/docs/archive", mustCreate = true)
            filesystem.createDirectory("/empty", mustCreate = true)
            filesystem.writeUtf8("/docs/hello.txt", "hello", null)
            val binary = byteArrayOf(0, 1, 2, 127, -1)
            filesystem.write("/docs/archive/data.bin", Base64.getEncoder().encodeToString(binary), null)

            assertEquals("hello", filesystem.readUtf8("/docs/hello.txt"))
            assertContentEquals(binary, Base64.getDecoder().decode(filesystem.read("/docs/archive/data.bin")))
            assertEquals(
                listOf("/docs/archive", "/docs/hello.txt"),
                filesystem.list("/docs", null, 100).entries.map { it.path },
            )
            val firstPage = filesystem.list("/docs", null, 1)
            val secondPage = filesystem.list("/docs", firstPage.nextAfter, 1)
            assertEquals(listOf("/docs/archive"), firstPage.entries.map { it.path })
            assertEquals(listOf("/docs/hello.txt"), secondPage.entries.map { it.path })
            assertEquals(firstPage.snapshotRevision, secondPage.snapshotRevision)
            assertNull(secondPage.nextAfter)
            assertFailsWith<InvalidPageLimitException> { filesystem.list("/docs", null, 0) }
            assertFailsWith<InvalidCursorException> { filesystem.list("/docs", "/outside", 1) }
            assertEquals(
                listOf("/docs/archive", "/docs/archive/data.bin", "/docs/hello.txt"),
                filesystem.listRecursively("/docs", null, 100).entries.map { it.path },
            )
            val firstRecursivePage = filesystem.listRecursively("/docs", null, 1)
            val secondRecursivePage = filesystem.listRecursively("/docs", firstRecursivePage.nextAfter, 1)
            assertEquals(listOf("/docs/archive"), firstRecursivePage.entries.map { it.path })
            assertEquals(listOf("/docs/archive/data.bin"), secondRecursivePage.entries.map { it.path })
            assertEquals(firstRecursivePage.snapshotRevision, secondRecursivePage.snapshotRevision)
            assertFailsWith<InvalidCursorException> {
                filesystem.listRecursively("/docs", "/outside", 1)
            }

            val metadata = filesystem.metadata("/docs/hello.txt")
            assertEquals(FileEntryType.REGULAR_FILE, metadata.type)
            assertTrue(metadata.isRegularFile)
            assertFalse(metadata.isDirectory)
            assertEquals(5L, metadata.size)
            assertEquals("2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824", metadata.contentHash)
            assertEquals(8_000L, metadata.createdAtMillis)
            assertEquals(8_000L, metadata.lastModifiedAtMillis)
            assertNull(metadata.lastAccessedAtMillis)
            assertNull(metadata.symlinkTarget)
            assertNull(filesystem.metadataOrNull("/missing"))
            assertEquals(10L, manager.getUsedBytes(created.uuid))

            filesystem.createDirectories("/cursor", true)
            filesystem.createDirectory("/cursor/a", true)
            filesystem.createDirectory("/cursor/b", true)
            val deletedBoundary = filesystem.list("/cursor", null, 1).nextAfter
            filesystem.delete("/cursor/a", true)
            assertEquals(
                listOf("/cursor/b"),
                filesystem.list("/cursor", deletedBoundary, Int.MAX_VALUE).entries.map { it.path },
            )

            filesystem.createDirectories("/order", true)
            filesystem.createDirectory("/order/\uE000", true)
            filesystem.createDirectory("/order/\uD800\uDC00", true)
            assertEquals(
                listOf("/order/\uE000", "/order/\uD800\uDC00"),
                filesystem.list("/order", null, 100).entries.map { it.path },
            )

            val durableRevision = filesystem.list("/docs", null, 100).snapshotRevision
            val reopened = DurableSimpleFileSystemManager(
                InMemoryBlobstoreService(),
                database,
                ManualClock(8_001L),
            ).openFilesystem(created.uuid)
            assertEquals(durableRevision, reopened.list("/docs", null, 100).snapshotRevision)
            reopened.createDirectory("/docs/revision", true)
            assertTrue(reopened.list("/docs", null, 100).snapshotRevision > durableRevision)
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
