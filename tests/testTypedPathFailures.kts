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
import simplefilesystem.InvalidByteRangeException
import simplefilesystem.InvalidPathException
import simplefilesystem.PathAlreadyExistsException
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import sql.Database

fun testTypedPathFailures() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(1L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("errors", 100L).uuid)
            filesystem.createDirectory("/dir", true)
            filesystem.writeUtf8("/file", "abc", null)

            assertEquals(
                "Path '/missing' does not exist in this filesystem.",
                assertFailsWith<PathNotFoundException> { filesystem.readUtf8("/missing") }.message,
            )
            assertFailsWith<PathAlreadyExistsException> { filesystem.createDirectory("/dir", true) }
            assertFailsWith<PathTypeMismatchException> { filesystem.list("/file") }
            assertEquals(
                "Path 'relative' is invalid: paths must be absolute and begin with '/'.",
                assertFailsWith<InvalidPathException> { filesystem.exists("relative") }.message,
            )
            val range = assertFailsWith<InvalidByteRangeException> { filesystem.source("/file", 2L, 2L) }
            assertEquals("/file", range.path)
            assertEquals(3L, range.fileSize)
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
