@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("blobstore.api:blobstore-api:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("com.squareup.okio:okio-jvm:3.4.0")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachCluster
import community.kotlin.clocks.simple.ManualClock
import java.util.Base64
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
import simplefilesystem.durable.testing.ControlledBlobstoreService

fun testStreamingAndMiddleRange() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val failWrites = AtomicBoolean(false)
            val blobs = ControlledBlobstoreService().apply {
                beforePut = {
                    if (failWrites.get()) throw IllegalStateException("injected staged block failure")
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
            assertFailsWith<MalformedBase64Exception> { filesystem.overwrite("/bad-padding", "Zg") }
            assertFailsWith<MalformedBase64Exception> { filesystem.overwrite("/bad-bits", "AB==") }
            assertFailsWith<MalformedUtf8Exception> { filesystem.overwriteUtf8("/bad-text", "\uD800") }
            assertEquals(false, filesystem.exists("/bad-padding"))
            assertEquals(false, filesystem.exists("/bad-bits"))
            assertEquals(false, filesystem.exists("/bad-text"))

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
            blobs.fetchedHashes.clear()
            val range = Buffer()
            filesystem.source("/large.bin", offset, byteCount).use { source ->
                while (source.read(range, 31_337L) != -1L) Unit
            }
            assertContentEquals(bytes.copyOfRange(offset.toInt(), (offset + byteCount).toInt()), range.readByteArray())
            assertEquals(
                1,
                blobs.fetchedHashes.size,
                "A middle range wholly inside block 1 must fetch only that block blob.",
            )

            failWrites.set(true)
            val poisoned = filesystem.sink("/poisoned", null)
            val poison = assertFailsWith<IllegalStateException> {
                poisoned.write(Buffer().write(ByteArray(blockSize) { 9 }), blockSize.toLong())
            }
            assertEquals("injected staged block failure", poison.message)
            assertSame(poison, assertFailsWith<IllegalStateException> { poisoned.commit() })
            assertEquals(
                "FileSink for path '/poisoned' cannot accept data: the sink failed earlier and is poisoned.",
                assertFailsWith<IllegalStateException> {
                    poisoned.write(Buffer().writeByte(1), 1L)
                }.message,
            )
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
