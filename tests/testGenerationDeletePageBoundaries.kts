@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
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
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

fun testGenerationDeletePageBoundaries() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val blobs = InMemoryBlobstoreService()
            val generationDeleteBatchSize = 64
            val manager = DurableSimpleFileSystemManager(
                blobstoreService = blobs,
                metadataDatabase = database,
                generationDeleteBatchSize = generationDeleteBatchSize,
            )
            try {
                val sha256 = { bytes: ByteArray ->
                    MessageDigest.getInstance("SHA-256").digest(bytes)
                        .joinToString("") { "%02X".format(it.toInt() and 0xFF) }
                }
                val filesystemUuid = manager.createFilesystem("delete page boundaries", 100_000L).uuid
                val filesystem = manager.openFilesystem(filesystemUuid)
                val blockCounts = linkedMapOf(
                    "/exact-page" to generationDeleteBatchSize,
                    "/page-plus-one" to generationDeleteBatchSize + 1,
                )
                blockCounts.keys.forEach { path -> filesystem.writeUtf8(path, "seed", null) }

                val oneByte = byteArrayOf('x'.code.toByte())
                val oneByteHash = sha256(oneByte)
                blobs.putBlob(
                    "simplefilesystem-durable-embedded",
                    oneByteHash,
                    oneByte.size.toLong(),
                    ByteArrayInputStream(oneByte),
                )
                assertTrue(blobs.pinBlob("simplefilesystem-durable-embedded", oneByteHash))

                val oldGenerations = blockCounts.map { (path, count) ->
                    val generation = database.getRows(
                        "SELECT generation_uuid FROM entries WHERE filesystem_uuid = ?::UUID AND path = ?",
                        filesystemUuid,
                        path,
                    ).single().results["generation_uuid"]
                    database.execute("DELETE FROM file_blocks WHERE generation_uuid = ?::UUID", generation)
                    database.execute(
                        """INSERT INTO file_blocks
                            (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                            SELECT ?::UUID, value::INT4, ?, 1, 1
                            FROM generate_series(0, ?) AS generated(value)""".trimIndent(),
                        generation,
                        oneByteHash,
                        count - 1,
                    )
                    database.execute(
                        """UPDATE entries SET size_bytes = ?, content_hash = ?
                            WHERE filesystem_uuid = ?::UUID AND path = ?""".trimIndent(),
                        count.toLong(),
                        sha256(ByteArray(count) { 'x'.code.toByte() }),
                        filesystemUuid,
                        path,
                    )
                    generation
                }
                database.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?::UUID",
                    blockCounts.values.sumOf(Int::toLong),
                    filesystemUuid,
                )

                blockCounts.keys.forEach { path ->
                    filesystem.overwriteUtf8(path, "replacement")
                    assertEquals("replacement", filesystem.readUtf8(path))
                }
                oldGenerations.forEach { generation ->
                    assertEquals(
                        0L,
                        database.getRows(
                            "SELECT count(*) AS block_count FROM file_blocks WHERE generation_uuid = ?::UUID",
                            generation,
                        ).single().results.getValue("block_count").toString().toLong(),
                        "Replacing a generation must delete every old block at and across the configured batch boundary.",
                    )
                }

                manager.processBlobGcOutbox()
                assertFalse(
                    blobs.isPinned("simplefilesystem-durable-embedded", oneByteHash),
                    "The shared old blob must be unpinned after every boundary-sized generation is gone.",
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
