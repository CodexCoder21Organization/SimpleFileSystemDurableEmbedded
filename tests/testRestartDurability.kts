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

fun testRestartDurability() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val blobs = InMemoryBlobstoreService()
            val firstManager = DurableSimpleFileSystemManager(blobs, database, ManualClock(9L))
            val uuid = firstManager.createFilesystem("restart", 10_000L).uuid
            firstManager.openFilesystem(uuid).writeUtf8("/durable", "survives manager restart", null)

            val secondManager = DurableSimpleFileSystemManager(blobs, database, ManualClock(10L))
            assertEquals("restart", secondManager.getFilesystemInfo(uuid).description)
            assertEquals("survives manager restart", secondManager.openFilesystem(uuid).readUtf8("/durable"))
            assertEquals(24L, secondManager.getUsedBytes(uuid))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
