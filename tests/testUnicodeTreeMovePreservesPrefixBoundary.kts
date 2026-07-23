@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import simplefilesystem.durable.testing.SharedCockroachCluster
import sql.Database
import kotlin.test.assertEquals
import kotlin.test.assertFalse

fun testUnicodeTreeMovePreservesPrefixBoundary() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(
                InMemoryBlobstoreService(),
                database,
                ManualClock(12_000L),
            )
            try {
                val uuid = manager.createFilesystem("unicode tree move", 1_000L).uuid
                val filesystem = manager.openFilesystem(uuid)
                filesystem.createDirectories("/😀/nested", true)
                filesystem.writeUtf8("/😀/nested/file", "moved", null)
                filesystem.createDirectory("/😀suffix", true)
                filesystem.writeUtf8("/😀suffix/file", "untouched", null)

                filesystem.atomicMove("/😀", "/renamed")

                assertFalse(filesystem.exists("/😀"))
                assertEquals("moved", filesystem.readUtf8("/renamed/nested/file"))
                assertEquals("untouched", filesystem.readUtf8("/😀suffix/file"))
            } finally {
                manager.close()
            }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
