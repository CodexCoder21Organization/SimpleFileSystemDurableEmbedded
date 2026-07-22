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
import okio.Buffer
import okio.buffer
import simplefilesystem.PathNotFoundException
import sql.Database

fun testAppendSemantics() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
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
            filesystem.appendingSink("/log").buffer().use { it.writeUtf8("-gamma") }
            assertEquals("alpha-beta-gamma", filesystem.readUtf8("/log"))
            assertEquals(16L, manager.getUsedBytes(uuid))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
