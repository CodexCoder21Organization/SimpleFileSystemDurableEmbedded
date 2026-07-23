@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import simplefilesystem.durable.testing.SharedCockroachCluster
import sql.Database
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

fun testTransitionGenerationDeletePagePlusOne() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val blobs = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(blobs, database)
            try {
                val sha256 = { bytes: ByteArray ->
                    MessageDigest.getInstance("SHA-256").digest(bytes)
                        .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
                }
                val filesystemUuid = manager.createFilesystem("delete page plus one", 100_000L).uuid
                val filesystem = manager.openFilesystem(filesystemUuid)
                val path = "/page-plus-one"
                val blockCount = 16_385
                filesystem.writeUtf8(path, "seed", null)

                val oneByte = byteArrayOf('x'.code.toByte())
                val oneByteHash = sha256(oneByte)
                blobs.putBlob(
                    "simplefilesystem-durable-embedded",
                    oneByteHash,
                    oneByte.size.toLong(),
                    ByteArrayInputStream(oneByte),
                )
                assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", oneByteHash))

                val oldGeneration = database.getRows(
                    "SELECT generation_uuid FROM entries WHERE filesystem_uuid = ?::UUID AND path = ?",
                    filesystemUuid,
                    path,
                ).single().results["generation_uuid"]
                database.execute("DELETE FROM file_blocks WHERE generation_uuid = ?::UUID", oldGeneration)
                database.execute(
                    """INSERT INTO file_blocks
                        (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                        SELECT ?::UUID, value::INT4, ?, 1, 1
                        FROM generate_series(0, ?) AS generated(value)""".trimIndent(),
                    oldGeneration,
                    oneByteHash,
                    blockCount - 1,
                )
                database.execute(
                    """UPDATE entries SET size_bytes = ?, content_hash = ?
                        WHERE filesystem_uuid = ?::UUID AND path = ?""".trimIndent(),
                    blockCount.toLong(),
                    sha256(ByteArray(blockCount) { 'x'.code.toByte() }),
                    filesystemUuid,
                    path,
                )
                database.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?::UUID",
                    blockCount.toLong(),
                    filesystemUuid,
                )

                filesystem.overwriteUtf8(path, "replacement")
                assertEquals("replacement", filesystem.readUtf8(path))
                assertEquals(
                    0L,
                    database.getRows(
                        "SELECT count(*) AS block_count FROM file_blocks WHERE generation_uuid = ?::UUID",
                        oldGeneration,
                    ).single().results.getValue("block_count").toString().toLong(),
                    "Replacing a generation must delete blocks on both sides of the page boundary.",
                )

                manager.processBlobGcOutbox()
                assertFalse(
                    blobs.isPinned("simplefilesystem-durable-embedded", oneByteHash),
                    "The old blob must be unpinned after the page-plus-one generation is gone.",
                )
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
