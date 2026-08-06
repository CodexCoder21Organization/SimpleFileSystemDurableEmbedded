@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("simplefilesystem:simplefilesystem-api:0.3.0")
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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

fun testNamespaceWatchRestartAndRecreate() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val blobs = InMemoryBlobstoreService()
            val first = DurableSimpleFileSystemManager(blobs, database, ManualClock(200L))
            val uuid = first.createFilesystem("watch restart", 10_000L).uuid
            first.openFilesystem(uuid).writeUtf8("/same", "first", null)
            assertEquals(1L, first.watchFilesystem(uuid, "/same", 0L, 100).events.single().revision)
            first.close()

            val second = DurableSimpleFileSystemManager(blobs, database, ManualClock(201L))
            val filesystem = second.openFilesystem(uuid)
            filesystem.delete("/same", true)
            filesystem.writeUtf8("/same", "second", null)
            val resumed = second.watchFilesystem(uuid, "/same", 1L, 100)
            assertEquals(listOf(2L, 3L), resumed.events.map { it.revision })
            assertFalse(resumed.events[0].exists)
            assertTrue(resumed.events[1].exists)
            assertEquals("second", filesystem.readUtf8("/same"))
            second.close()
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
