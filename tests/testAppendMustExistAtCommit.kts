@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("blobstore.api:blobstore-api:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachCluster
import community.kotlin.clocks.simple.ManualClock
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import simplefilesystem.durable.testing.ControlledBlobstoreService
import kotlin.test.assertEquals
import kotlin.test.assertFalse

fun testAppendMustExistAtCommit() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = cluster.openDatabase()
        try {
            val blockAppend = AtomicBoolean(false)
            val appendReachedBlobstore = CountDownLatch(1)
            val allowAppend = CountDownLatch(1)
            val blobs = ControlledBlobstoreService().apply {
                beforePin = {
                    if (blockAppend.get()) {
                        appendReachedBlobstore.countDown()
                        allowAppend.await()
                    }
                }
            }
            val manager = DurableSimpleFileSystemManager(blobs, database, ManualClock(12L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("append race", 1_000L).uuid)
            filesystem.writeUtf8("/log", "old", null)

            blockAppend.set(true)
            val executor = Executors.newSingleThreadExecutor()
            try {
                val append = executor.submit<Throwable?> {
                    try {
                        filesystem.appendingWriteUtf8("/log", "new", mustExist = true)
                        null
                    } catch (failure: Throwable) {
                        failure
                    }
                }
                appendReachedBlobstore.await()
                filesystem.delete("/log", mustExist = true)
                allowAppend.countDown()

                val failure = append.get()
                assertEquals("simplefilesystem.PathNotFoundException", failure?.javaClass?.name)
                assertFalse(filesystem.exists("/log"))
            } finally {
                allowAppend.countDown()
                executor.shutdownNow()
            }
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
