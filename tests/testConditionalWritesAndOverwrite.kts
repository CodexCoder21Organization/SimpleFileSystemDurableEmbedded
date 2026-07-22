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
import simplefilesystem.FileContentConflictException
import simplefilesystem.InvalidContentHashException
import sql.Database

fun testConditionalWritesAndOverwrite() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(2L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("cas", 1_000L).uuid)
            filesystem.writeUtf8("/value", "one", null)
            val oneHash = filesystem.metadata("/value").contentHash!!

            val createConflict = runCatching {
                filesystem.writeUtf8("/value", "two", null)
            }.exceptionOrNull()
            assertEquals("simplefilesystem.FileContentConflictException", createConflict?.javaClass?.name)
            assertEquals(
                "Cannot conditionally write path '/value': expected the path to be absent, but observed " +
                    "content hash '$oneHash'.",
                createConflict?.message,
            )
            assertEquals("one", filesystem.readUtf8("/value"))

            val stale = "A".repeat(64)
            val staleConflict = runCatching {
                filesystem.writeUtf8("/value", "two", stale)
            }.exceptionOrNull()
            assertEquals("simplefilesystem.FileContentConflictException", staleConflict?.javaClass?.name)
            val invalidHash = runCatching {
                filesystem.writeUtf8("/value", "two", "lowercase")
            }.exceptionOrNull()
            assertEquals("simplefilesystem.InvalidContentHashException", invalidHash?.javaClass?.name)

            filesystem.writeUtf8("/value", "two", oneHash)
            assertEquals("two", filesystem.readUtf8("/value"))
            filesystem.overwriteUtf8("/value", "unconditional")
            assertEquals("unconditional", filesystem.readUtf8("/value"))
            assertEquals(13L, manager.getUsedBytes(manager.listFilesystems().single().uuid))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
