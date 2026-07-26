@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
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
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import okio.Buffer
import sql.Database

fun testAbandonedSinkIsolation() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(6L))
            val uuid = manager.createFilesystem("abandoned", 10L * 1024L * 1024L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val sink = filesystem.sink("/incomplete", null)
            sink.write(Buffer().write(ByteArray(4 * 1024 * 1024) { 7 }), 4L * 1024L * 1024L)

            assertFalse(filesystem.exists("/incomplete"))
            assertEquals(0L, manager.getUsedBytes(uuid))
            sink.close()
            sink.close()
            assertFalse(filesystem.exists("/incomplete"))

            val aborted = filesystem.sink("/aborted", null)
            aborted.write(Buffer().writeUtf8("discarded"), 9L)
            aborted.abort()
            aborted.abort()
            aborted.close()
            assertFalse(filesystem.exists("/aborted"))
            assertEquals(
                "FileSink for path '/aborted' cannot accept data: the sink was aborted.",
                assertFailsWith<IllegalStateException> { aborted.write(Buffer(), 0L) }.message,
            )
            assertEquals(
                "FileSink for path '/aborted' cannot commit: the sink was aborted.",
                assertFailsWith<IllegalStateException> { aborted.commit() }.message,
            )

            val closed = filesystem.sink("/closed", null)
            closed.write(Buffer().writeUtf8("discarded"), 9L)
            closed.close()
            assertFalse(filesystem.exists("/closed"))
            assertEquals(
                "FileSink for path '/closed' cannot commit: the sink was closed without commit, which aborted it.",
                assertFailsWith<IllegalStateException> { closed.commit() }.message,
            )

            val committed = filesystem.sink("/committed", null)
            committed.write(Buffer().writeUtf8("visible"), 7L)
            val metadata = committed.commit()
            assertEquals(7L, metadata.size)
            assertSame(metadata, committed.commit())
            committed.abort()
            committed.close()
            assertEquals(
                "FileSink for path '/committed' cannot accept data: the sink was already committed.",
                assertFailsWith<IllegalStateException> { committed.flush() }.message,
            )
            assertEquals("visible", filesystem.readUtf8("/committed"))
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
