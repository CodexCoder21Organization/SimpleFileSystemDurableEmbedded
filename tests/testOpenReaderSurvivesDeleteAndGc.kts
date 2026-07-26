@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
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
import kotlin.test.assertContentEquals
import okio.Buffer
import sql.Database
import simplefilesystem.durable.testing.ControlledBlobstoreService

fun testOpenReaderSurvivesDeleteAndGc() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(
                ControlledBlobstoreService().apply { requirePinnedOnGet = true },
                database,
                ManualClock(1_000L),
            )
            val filesystemUuid = manager.createFilesystem("reader pin", 20_000_000L).uuid
            val filesystem = manager.openFilesystem(filesystemUuid)
            val expected = ByteArray(4 * 1024 * 1024 + 37) { index -> (index % 251).toByte() }
            val sink = filesystem.sink("/payload.bin", null)
            sink.write(Buffer().write(expected), expected.size.toLong())
            sink.commit()

            val source = filesystem.source("/payload.bin")
            filesystem.delete("/payload.bin", mustExist = true)
            manager.processBlobGcOutbox()

            val actual = Buffer()
            source.use {
                while (it.read(actual, 8192L) != -1L) Unit
            }
            assertContentEquals(expected, actual.readByteArray())

            filesystem.sink("/overwrite.bin", null).also {
                it.write(Buffer().write(expected), expected.size.toLong())
                it.commit()
            }
            val midReadSource = filesystem.source("/overwrite.bin")
            val midRead = Buffer()
            midReadSource.read(midRead, 8192L)
            filesystem.overwriteUtf8("/overwrite.bin", "replacement")
            manager.processBlobGcOutbox()
            midReadSource.use {
                while (it.read(midRead, 8192L) != -1L) Unit
            }
            assertContentEquals(expected, midRead.readByteArray())

            filesystem.sink("/purged.bin", null).also {
                it.write(Buffer().write(expected), expected.size.toLong())
                it.commit()
            }
            val purgeReader = filesystem.source("/purged.bin")
            manager.setExpiration(filesystemUuid, 1_000L)
            manager.purgeExpiredFilesystems()
            manager.processBlobGcOutbox()
            val afterPurge = Buffer()
            purgeReader.use {
                while (it.read(afterPurge, 8192L) != -1L) Unit
            }
            assertContentEquals(expected, afterPurge.readByteArray())
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
