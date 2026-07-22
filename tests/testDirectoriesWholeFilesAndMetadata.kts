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
import java.util.Base64
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import sql.Database

fun testDirectoriesWholeFilesAndMetadata() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
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
            assertEquals(listOf("/docs/archive", "/docs/hello.txt"), filesystem.list("/docs").map { it.path })
            assertEquals(
                listOf("/docs/archive", "/docs/archive/data.bin", "/docs/hello.txt"),
                filesystem.listRecursively("/docs").map { it.path },
            )

            val metadata = filesystem.metadata("/docs/hello.txt")
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
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
