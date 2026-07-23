@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import okio.Buffer
import simplefilesystem.durable.testing.SharedCockroachCluster
import sql.Database
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith

fun testWriteLeaseExpiresMidStream() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(10_000L)
            val manager = DurableSimpleFileSystemManager(
                blobstoreService = InMemoryBlobstoreService(),
                metadataDatabase = database,
                clock = clock,
                sessionLeaseMillis = 100L,
            )
            try {
                val filesystemUuid = manager.createFilesystem("mid-stream expiry", 1_024L).uuid
                val filesystem = manager.openFilesystem(filesystemUuid)
                val sink = filesystem.sink("/expired-write", null)
                val sessionUuid = database.getRows(
                    """SELECT session_uuid FROM write_sessions
                        WHERE filesystem_uuid = ?::UUID AND path = '/expired-write'""".trimIndent(),
                    filesystemUuid,
                ).single().results.getValue("session_uuid").toString()

                sink.write(Buffer().writeByte(1), 1L)
                clock.advanceBy(101L)
                val failure = assertFailsWith<IllegalStateException> {
                    sink.write(Buffer().writeByte(2), 1L)
                }
                assertEquals(
                    "Write session '$sessionUuid' lease expired at epoch millisecond 10100; " +
                        "the attempted chunk was observed at epoch millisecond 10101.",
                    failure.message,
                )
                assertEquals(
                    "ABORTED",
                    database.getRows(
                        "SELECT state FROM write_sessions WHERE session_uuid = ?::UUID",
                        sessionUuid,
                    ).single().results.getValue("state"),
                    "A failed renewal must poison and abort the write session.",
                )
                assertFalse(filesystem.exists("/expired-write"))
                assertEquals(0L, manager.getUsedBytes(filesystemUuid))
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
