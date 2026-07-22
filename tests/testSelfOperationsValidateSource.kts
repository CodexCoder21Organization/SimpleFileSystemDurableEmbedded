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
import sql.Database

fun testSelfOperationsValidateSource() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(11L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("self operations", 1_000L).uuid)

            val missingCopy = runCatching { filesystem.copy("/missing", "/missing") }.exceptionOrNull()
            assertEquals("simplefilesystem.PathNotFoundException", missingCopy?.javaClass?.name)
            assertEquals("Path '/missing' does not exist in this filesystem.", missingCopy?.message)

            val missingMove = runCatching { filesystem.atomicMove("/missing", "/missing") }.exceptionOrNull()
            assertEquals("simplefilesystem.PathNotFoundException", missingMove?.javaClass?.name)
            assertEquals("Path '/missing' does not exist in this filesystem.", missingMove?.message)

            filesystem.createDirectory("/directory", mustCreate = true)
            val directoryCopy = runCatching { filesystem.copy("/directory", "/directory") }.exceptionOrNull()
            assertEquals("simplefilesystem.PathTypeMismatchException", directoryCopy?.javaClass?.name)
            assertEquals(
                "Path '/directory' has type 'DIRECTORY', but this operation requires type 'FILE'.",
                directoryCopy?.message,
            )

            filesystem.writeUtf8("/file", "unchanged", null)
            filesystem.copy("/file", "/file")
            filesystem.atomicMove("/file", "/file")
            assertEquals("unchanged", filesystem.readUtf8("/file"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
