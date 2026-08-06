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

fun testCopySharesGeneration() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val blobs = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(blobs, database, ManualClock(7L))
            val uuid = manager.createFilesystem("copy", 20L * 1024L * 1024L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val content = "same immutable generation"
            val sharedHash = filesystem.writeUtf8("/source", content, null).contentHash!!
            filesystem.copy("/source", "/target")

            assertEquals(content, filesystem.readUtf8("/target"))
            assertEquals(2L * content.toByteArray().size, manager.getUsedBytes(uuid))
            assertEquals(1, blobs.storedBlobCount())
            assertEquals(true, blobs.isPinned("simplefilesystem-durable-embedded", sharedHash))

            filesystem.overwriteUtf8("/source", "changed")
            assertEquals(content, filesystem.readUtf8("/target"))
            manager.processBlobGcOutbox()
            assertEquals(true, blobs.isPinned("simplefilesystem-durable-embedded", sharedHash))
            filesystem.delete("/target", true)
            manager.processBlobGcOutbox()
            assertEquals(false, blobs.isPinned("simplefilesystem-durable-embedded", sharedHash))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
