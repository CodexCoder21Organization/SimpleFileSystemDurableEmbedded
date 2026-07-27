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
import okio.Buffer
import okio.buffer
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import sql.Database

fun testAppendSemantics() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(3L))
            val uuid = manager.createFilesystem("append", 1_000L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val missing = runCatching {
                filesystem.appendingWriteUtf8("/log", "x", mustExist = true)
            }.exceptionOrNull()
            assertEquals("simplefilesystem.PathNotFoundException", missing?.javaClass?.name)
            filesystem.appendingWriteUtf8("/log", "alpha", mustExist = false)
            filesystem.appendingWriteUtf8("/log", "-beta", mustExist = true)
            val appendingSink = filesystem.appendingSink("/log")
            val bufferedAppend = appendingSink.buffer()
            try {
                bufferedAppend.writeUtf8("-gamma")
                bufferedAppend.flush()
                appendingSink.commit()
            } finally {
                bufferedAppend.close()
            }
            assertEquals("alpha-beta-gamma", filesystem.readUtf8("/log"))
            assertEquals(16L, manager.getUsedBytes(uuid))

            val lateAppend = filesystem.appendingSink("/log")
            lateAppend.write(Buffer().writeUtf8("-late"), 5L)
            filesystem.overwriteUtf8("/log", "winner")
            lateAppend.commit()
            assertEquals("winner-late", filesystem.readUtf8("/log"))

            val typeWinner = filesystem.appendingSink("/directory-winner")
            typeWinner.write(Buffer().writeUtf8("bytes"), 5L)
            filesystem.createDirectory("/directory-winner", true)
            assertFailsWith<PathTypeMismatchException> { typeWinner.commit() }

            filesystem.createDirectory("/parent", true)
            val missingParent = filesystem.appendingSink("/parent/file")
            missingParent.write(Buffer().writeUtf8("bytes"), 5L)
            filesystem.delete("/parent", true)
            assertFailsWith<PathNotFoundException> { missingParent.commit() }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
