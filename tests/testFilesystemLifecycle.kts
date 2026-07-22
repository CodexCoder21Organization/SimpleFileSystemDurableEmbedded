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
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import okio.Buffer
import simplefilesystem.FilesystemExpiredException
import simplefilesystem.FilesystemNotFoundException
import sql.Database

fun testFilesystemLifecycle() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(1_000L)
            val manager = DurableSimpleFileSystemManager(
                InMemoryBlobstoreService(),
                database,
                clock,
            )
            val created = manager.createFilesystem("scratch", 1024L)
            UUID.fromString(created.uuid)
            assertEquals("scratch", created.description)
            assertNull(created.owner)
            assertEquals(1024L, created.maxSizeBytes)
            assertEquals(0L, created.usedBytes)
            assertEquals(1_000L, created.createdAtMillis)
            assertEquals(listOf(created.uuid), manager.listFilesystems(null, 100).filesystems.map { it.uuid })
            assertEquals(created.uuid, manager.getFilesystemInfo(created.uuid).uuid)
            manager.openFilesystem(created.uuid).writeUtf8("/hello.txt", "hello", null)
            assertEquals(5L, manager.getUsedBytes(created.uuid))
            val staleOnExpiration = manager.openFilesystem(created.uuid).sink("/staged", null)
            staleOnExpiration.write(Buffer().writeUtf8("bytes"), 5L)
            manager.setExpiration(created.uuid, 1_000L)
            assertFailsWith<FilesystemExpiredException> { staleOnExpiration.commit() }
            manager.setExpiration(created.uuid, null)

            val staleOnDeletion = manager.openFilesystem(created.uuid).sink("/deleted-stage", null)
            staleOnDeletion.write(Buffer().writeUtf8("bytes"), 5L)
            manager.setExpiration(created.uuid, 9_000L)
            assertEquals(9_000L, manager.getExpiration(created.uuid))
            manager.deleteFilesystem(created.uuid)
            assertFailsWith<FilesystemNotFoundException> { staleOnDeletion.commit() }
            val missing = runCatching { manager.getFilesystemInfo(created.uuid) }.exceptionOrNull()
            assertEquals("simplefilesystem.FilesystemNotFoundException", missing?.javaClass?.name)
            assertEquals(0L, database.getLong("SELECT count(*) FROM filesystems"))
            assertEquals(0L, database.getLong("SELECT count(*) FROM entries"))
            assertEquals(0L, database.getLong("SELECT count(*) FROM write_sessions"))
            assertEquals(0L, database.getLong("SELECT count(*) FROM file_blocks"))
            assertEquals(1L, database.getLong("SELECT count(*) FROM blob_gc_outbox"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
