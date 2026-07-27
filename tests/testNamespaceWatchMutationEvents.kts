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
import kotlin.test.assertNull
import kotlin.test.assertTrue
import simplefilesystem.FileEntryType
import sql.Database

fun testNamespaceWatchMutationEvents() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(100L))
            val uuid = manager.createFilesystem("watch mutations", 10_000L).uuid
            val filesystem = manager.openFilesystem(uuid)

            val initial = manager.watchFilesystem(uuid, "/file", null, 100)
            assertEquals(0L, initial.latestRevision)
            assertEquals(0L, initial.nextSinceRevision)
            assertFalse(initial.hasMore)
            assertEquals(1, initial.events.size)
            assertEquals("/file", initial.events.single().path)
            assertFalse(initial.events.single().exists)
            assertNull(initial.events.single().entryType)

            val created = filesystem.writeUtf8("/file", "one", null)
            val writeEvent = manager.watchFilesystem(uuid, "/file", 0L, 100).events.single()
            assertEquals(1L, writeEvent.revision)
            assertEquals(FileEntryType.REGULAR_FILE, writeEvent.entryType)
            assertEquals(3L, writeEvent.size)
            assertEquals(created.contentHash, writeEvent.contentHash)
            val rootAfterCreate = manager.watchFilesystem(uuid, "/", 0L, 100).events.single()
            assertEquals(1L, rootAfterCreate.revision)
            assertEquals(FileEntryType.DIRECTORY, rootAfterCreate.entryType)

            filesystem.overwriteUtf8("/file", "two")
            assertEquals(listOf(2L), manager.watchFilesystem(uuid, "/file", 1L, 100).events.map { it.revision })
            val unchangedRoot = manager.watchFilesystem(uuid, "/", 1L, 100)
            assertEquals(emptyList(), unchangedRoot.events)
            assertEquals(2L, unchangedRoot.nextSinceRevision)

            filesystem.appendingWriteUtf8("/file", "+three", true)
            assertEquals(listOf(3L), manager.watchFilesystem(uuid, "/file", 2L, 100).events.map { it.revision })

            filesystem.createDirectory("/dir", true)
            filesystem.copy("/file", "/dir/copy")
            assertEquals(listOf(5L), manager.watchFilesystem(uuid, "/dir/copy", 4L, 100).events.map { it.revision })
            assertEquals(listOf(5L), manager.watchFilesystem(uuid, "/dir", 4L, 100).events.map { it.revision })

            filesystem.atomicMove("/dir/copy", "/moved")
            val movedFrom = manager.watchFilesystem(uuid, "/dir/copy", 5L, 100).events.single()
            assertEquals(6L, movedFrom.revision)
            assertFalse(movedFrom.exists)
            val movedTo = manager.watchFilesystem(uuid, "/moved", 5L, 100).events.single()
            assertEquals(6L, movedTo.revision)
            assertTrue(movedTo.exists)
            assertEquals(listOf(6L), manager.watchFilesystem(uuid, "/dir", 5L, 100).events.map { it.revision })
            assertEquals(listOf(6L), manager.watchFilesystem(uuid, "/", 5L, 100).events.map { it.revision })

            filesystem.delete("/moved", true)
            val deleted = manager.watchFilesystem(uuid, "/moved", 6L, 100).events.single()
            assertEquals(7L, deleted.revision)
            assertFalse(deleted.exists)
            assertEquals(listOf(7L), manager.watchFilesystem(uuid, "/", 6L, 100).events.map { it.revision })

            filesystem.createDirectories("/tree/child", true)
            filesystem.writeUtf8("/tree/child/leaf", "leaf", null)
            filesystem.deleteRecursively("/tree", true)
            assertEquals(listOf(10L), manager.watchFilesystem(uuid, "/tree", 9L, 100).events.map { it.revision })
            assertEquals(listOf(10L), manager.watchFilesystem(uuid, "/tree/child", 9L, 100).events.map { it.revision })
            assertEquals(listOf(10L), manager.watchFilesystem(uuid, "/tree/child/leaf", 9L, 100).events.map { it.revision })
            assertEquals(listOf(10L), manager.watchFilesystem(uuid, "/", 9L, 100).events.map { it.revision })
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
