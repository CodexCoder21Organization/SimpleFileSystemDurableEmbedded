@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
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
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import okio.Buffer
import sql.Database

fun testReaperCannotCorruptWholeCommit() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val clock = ManualClock(1_000L)
            val blobstore = InMemoryBlobstoreService()
            val manager = DurableSimpleFileSystemManager(
                blobstore,
                database,
                clock,
                sessionLeaseMillis = 100L,
            )
            val uuid = manager.createFilesystem("whole commit/reaper race", 10_000_000L).uuid
            val filesystem = manager.openFilesystem(uuid)
            val bytes = ByteArray(4 * 1024 * 1024) { index -> (index % 251).toByte() }
            val sink = filesystem.sink("/whole.bin", null)
            sink.write(Buffer().write(bytes), bytes.size.toLong())
            clock.advanceBy(50L)
            val reaperDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            val reaperManager = DurableSimpleFileSystemManager(
                blobstore,
                reaperDatabase,
                clock,
                sessionLeaseMillis = 100L,
            )

            // Schema-forensic synchronization: public APIs cannot hold the filesystem row lock that places
            // commit after its session renewal but before publication. Assertions below remain public behavior.
            val sessionLocked = CountDownLatch(1)
            val releaseSession = CountDownLatch(1)
            val filesystemLocked = CountDownLatch(1)
            val releaseFilesystem = CountDownLatch(1)
            val publicationSessionLocked = CountDownLatch(1)
            val releasePublicationSession = CountDownLatch(1)
            val sessionLockDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            val filesystemLockDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            val publicationSessionDatabase = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
            val executor = Executors.newFixedThreadPool(5)
            try {
                val sessionHolder = executor.submit {
                    sessionLockDatabase.execute { transaction ->
                        transaction.getRows(
                            "SELECT session_uuid FROM write_sessions WHERE path = '/whole.bin' FOR UPDATE",
                        ).single()
                        sessionLocked.countDown()
                        assertTrue(releaseSession.await(5, TimeUnit.SECONDS))
                    }
                }
                assertTrue(sessionLocked.await(5, TimeUnit.SECONDS))
                val commit = executor.submit<Throwable?> {
                    try {
                        sink.commit()
                        null
                    } catch (failure: Throwable) {
                        failure
                    }
                }
                val lockHolder = executor.submit {
                    filesystemLockDatabase.execute { transaction ->
                        transaction.getRows(
                            "SELECT uuid FROM filesystems WHERE uuid = ? FOR UPDATE",
                            UUID.fromString(uuid),
                        ).single()
                        filesystemLocked.countDown()
                        assertTrue(releaseFilesystem.await(5, TimeUnit.SECONDS))
                    }
                }
                assertTrue(filesystemLocked.await(5, TimeUnit.SECONDS))
                releaseSession.countDown()
                sessionHolder.get(5, TimeUnit.SECONDS)
                // Let commit advance to publication. Publication must claim the session row before waiting
                // for the filesystem row, so the independent session-lock probe distinguishes the two orders.
                Thread.sleep(250L)
                clock.advanceBy(101L)
                val publicationSessionHolder = executor.submit {
                    publicationSessionDatabase.execute { transaction ->
                        transaction.getRows(
                            "SELECT session_uuid FROM write_sessions WHERE path = '/whole.bin' FOR UPDATE",
                        ).single()
                        publicationSessionLocked.countDown()
                        assertTrue(releasePublicationSession.await(5, TimeUnit.SECONDS))
                    }
                }
                val buggyCommitDidNotClaimSession = publicationSessionLocked.await(1, TimeUnit.SECONDS)
                val reaper = executor.submit<Int> { reaperManager.reapAbandonedWriteSessions() }
                if (buggyCommitDidNotClaimSession) {
                    releasePublicationSession.countDown()
                    publicationSessionHolder.get(5, TimeUnit.SECONDS)
                    reaper.get(5, TimeUnit.SECONDS)
                    releaseFilesystem.countDown()
                } else {
                    releaseFilesystem.countDown()
                    releasePublicationSession.countDown()
                }
                lockHolder.get(5, TimeUnit.SECONDS)
                publicationSessionHolder.get(5, TimeUnit.SECONDS)
                reaper.get(5, TimeUnit.SECONDS)
                val commitFailure = commit.get(5, TimeUnit.SECONDS)

                if (commitFailure == null) {
                    val read = Buffer()
                    filesystem.source("/whole.bin").use { source ->
                        while (source.read(read, 8192L) != -1L) Unit
                    }
                    assertContentEquals(bytes, read.readByteArray())
                    assertEquals(bytes.size.toLong(), manager.getUsedBytes(uuid))
                } else {
                    assertFalse(filesystem.exists("/whole.bin"))
                    assertEquals(0L, manager.getUsedBytes(uuid))
                }
            } finally {
                releaseSession.countDown()
                releaseFilesystem.countDown()
                releasePublicationSession.countDown()
                executor.shutdownNow()
                publicationSessionDatabase.close()
                filesystemLockDatabase.close()
                sessionLockDatabase.close()
                reaperManager.close()
                reaperDatabase.close()
            }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
