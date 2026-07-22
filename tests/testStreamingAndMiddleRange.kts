@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("blobstore.api:blobstore-api:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("cockroachdb.testharness:cockroachdb-test-harness:0.0.4")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("com.squareup.okio:okio-jvm:3.4.0")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import blobstore.api.BlobstoreService
import build.kotlin.withartifact.WithArtifact
import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import community.kotlin.clocks.simple.SystemClock
import java.io.InputStream
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import okio.Buffer
import okio.buffer
import sql.Database

fun testStreamingAndMiddleRange() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val realBlobs = InMemoryBlobstoreService()
            val fetchedHashes = CopyOnWriteArrayList<String>()
            val blobs = object : BlobstoreService by realBlobs {
                override fun getBlob(publicKeyHash: String, sha256hex: String): InputStream {
                    fetchedHashes += sha256hex
                    return realBlobs.getBlob(publicKeyHash, sha256hex)
                }
            }
            val manager = DurableSimpleFileSystemManager(blobs, database, ManualClock(5L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("streaming", 20L * 1024L * 1024L).uuid)
            val blockSize = 4 * 1024 * 1024
            val bytes = ByteArray(10 * 1024 * 1024) { index -> ((index / blockSize * 37 + index) % 251).toByte() }

            filesystem.sink("/large.bin", null).buffer().use { it.write(bytes) }
            val whole = Buffer()
            filesystem.source("/large.bin").use { source -> while (source.read(whole, 64 * 1024L) != -1L) Unit }
            assertContentEquals(bytes, whole.readByteArray())
            assertContentEquals(bytes, filesystem.inputStream("/large.bin").use { it.readBytes() })

            val offset = blockSize.toLong() + 123L
            val byteCount = 1024L * 1024L
            fetchedHashes.clear()
            val range = Buffer()
            filesystem.source("/large.bin", offset, byteCount).use { source ->
                while (source.read(range, 31_337L) != -1L) Unit
            }
            assertContentEquals(bytes.copyOfRange(offset.toInt(), (offset + byteCount).toInt()), range.readByteArray())
            assertEquals(1, fetchedHashes.size, "A middle range wholly inside block 1 must fetch only that block blob.")
            assertEquals(3L, database.getLong("SELECT count(*) FROM file_blocks"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
