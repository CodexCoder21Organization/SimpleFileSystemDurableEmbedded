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
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import okio.Buffer
import okio.buffer
import simplefilesystem.InlinePayloadTooLargeException
import simplefilesystem.MalformedBase64Exception
import simplefilesystem.MalformedUtf8Exception
import sql.Database

fun testStreamingAndMiddleRange() {
    val cluster = LocalCockroachCluster(clock = SystemClock()).start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val realBlobs = InMemoryBlobstoreService()
            val fetchedHashes = CopyOnWriteArrayList<String>()
            val failWrites = AtomicBoolean(false)
            val blobs = object : BlobstoreService by realBlobs {
                override fun putBlob(
                    publicKeyHash: String,
                    sha256hex: String,
                    size: Long,
                    data: InputStream,
                ) {
                    if (failWrites.get()) throw IllegalStateException("injected staged block failure")
                    realBlobs.putBlob(publicKeyHash, sha256hex, size, data)
                }

                override fun getBlob(publicKeyHash: String, sha256hex: String): InputStream {
                    fetchedHashes += sha256hex
                    return realBlobs.getBlob(publicKeyHash, sha256hex)
                }
            }
            val manager = DurableSimpleFileSystemManager(blobs, database, ManualClock(5L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("streaming", 20L * 1024L * 1024L).uuid)
            val blockSize = 4 * 1024 * 1024
            val bytes = ByteArray(10 * 1024 * 1024) { index -> ((index / blockSize * 37 + index) % 251).toByte() }

            val largeSink = filesystem.sink("/large.bin", null)
            val bufferedLargeSink = largeSink.buffer()
            try {
                bufferedLargeSink.write(bytes)
                bufferedLargeSink.flush()
                largeSink.commit()
            } finally {
                bufferedLargeSink.close()
            }
            assertFailsWith<InlinePayloadTooLargeException> { filesystem.read("/large.bin") }
            assertFailsWith<InlinePayloadTooLargeException> { filesystem.readUtf8("/large.bin") }
            val sessionsBeforeMalformedPayloads = database.getLong("SELECT count(*) FROM write_sessions")
            assertFailsWith<MalformedBase64Exception> { filesystem.overwrite("/bad-padding", "Zg") }
            assertFailsWith<MalformedBase64Exception> { filesystem.overwrite("/bad-bits", "AB==") }
            assertFailsWith<MalformedUtf8Exception> { filesystem.overwriteUtf8("/bad-text", "\uD800") }
            assertEquals(sessionsBeforeMalformedPayloads, database.getLong("SELECT count(*) FROM write_sessions"))

            val invalidUtf8 = filesystem.sink("/invalid-utf8", null)
            invalidUtf8.write(Buffer().writeByte(0xC3), 1L)
            invalidUtf8.commit()
            assertFailsWith<MalformedUtf8Exception> { filesystem.readUtf8("/invalid-utf8") }

            val oversizedInline = Base64.getEncoder().encodeToString(ByteArray(4 * 1024 * 1024 + 1))
            assertFailsWith<InlinePayloadTooLargeException> {
                filesystem.overwrite("/too-large-inline", oversizedInline)
            }
            assertFailsWith<MalformedBase64Exception> {
                filesystem.overwrite("/malformed-large", oversizedInline.dropLast(1) + "$")
            }
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
            assertEquals(
                3L,
                database.getLong(
                    """SELECT count(*) FROM file_blocks WHERE generation_uuid =
                        (SELECT generation_uuid FROM entries WHERE path = '/large.bin')""".trimIndent(),
                ),
            )

            failWrites.set(true)
            val poisoned = filesystem.sink("/poisoned", null)
            val poison = assertFailsWith<IllegalStateException> {
                poisoned.write(Buffer().write(ByteArray(blockSize) { 9 }), blockSize.toLong())
            }
            assertEquals("injected staged block failure", poison.message)
            assertSame(poison, assertFailsWith<IllegalStateException> { poisoned.commit() })
            assertSame(
                poison,
                assertFailsWith<IllegalStateException> {
                    poisoned.write(Buffer().writeByte(1), 1L)
                },
            )
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
