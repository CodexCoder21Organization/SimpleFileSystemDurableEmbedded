@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.conformance:simplefilesystem-conformance:0.1.0")
@file:WithArtifact("simplefilesystem:simplefilesystem-api:0.3.0")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("cockroachdb.testharness:cockroachdb-test-harness:0.0.4")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import community.kotlin.clocks.simple.SystemClock
import kotlin.test.assertEquals
import simplefilesystem.conformance.ConformanceClockHandle
import simplefilesystem.conformance.ConformanceArea
import simplefilesystem.conformance.SimpleFileSystemConformanceSuite
import simplefilesystem.conformance.SimpleFileSystemManagerFactory
import simplefilesystem.conformance.SimpleFileSystemManagerFixture
import sql.Database

private val durableConformanceFactory = object : SimpleFileSystemManagerFactory {
            override val backendName: String = "Durable CockroachDB + in-memory Blobstore"

            override fun create(initialTimeMillis: Long): SimpleFileSystemManagerFixture {
                val cluster = LocalCockroachCluster(clock = SystemClock()).start()
                val database = Database(
                    "org.postgresql.Driver",
                    cluster.jdbcUrl(),
                    cluster.username,
                    cluster.password,
                )
                val manualClock = ManualClock(initialTimeMillis)
                val backendManager = DurableSimpleFileSystemManager(
                    blobstoreService = InMemoryBlobstoreService(),
                    metadataDatabase = database,
                    clock = manualClock,
                    namespaceEventRetentionCount = 4,
                    namespaceEventRetentionMillis = Long.MAX_VALUE,
                )
                return object : SimpleFileSystemManagerFixture {
                    override val manager = backendManager
                    override val clock = object : ConformanceClockHandle {
                        override var currentTimeMillis: Long
                            get() = manualClock.currentTimeMillis()
                            set(value) = manualClock.advanceBy(value - manualClock.currentTimeMillis())
                    }

                    override fun close() {
                        backendManager.close()
                        manualClock.shutdown()
                        database.close()
                        cluster.close()
                    }
                }
            }
}

private fun runDurableConformanceArea(area: ConformanceArea, expectedCount: Int) {
    val result = SimpleFileSystemConformanceSuite.run(durableConformanceFactory, setOf(area))
    assertEquals(expectedCount, result.totalScenarioCount)
}

fun testConformanceSuiteCockroachBlobstorePrecedenceAndMessages() {
    runDurableConformanceArea(ConformanceArea.PRECEDENCE_AND_MESSAGES, 12)
}

fun testConformanceSuiteCockroachBlobstoreOperationMatrix() {
    runDurableConformanceArea(ConformanceArea.OPERATION_MATRIX, 12)
}

fun testConformanceSuiteCockroachBlobstoreContentCas() {
    runDurableConformanceArea(ConformanceArea.CONTENT_CAS, 7)
}

fun testConformanceSuiteCockroachBlobstoreSnapshotsAndPagination() {
    runDurableConformanceArea(ConformanceArea.SNAPSHOTS_AND_PAGINATION, 4)
}

fun testConformanceSuiteCockroachBlobstoreStreaming() {
    runDurableConformanceArea(ConformanceArea.STREAMING, 6)
}

fun testConformanceSuiteCockroachBlobstoreLifecycle() {
    runDurableConformanceArea(ConformanceArea.LIFECYCLE, 8)
}

fun testConformanceSuiteCockroachBlobstoreWatchLog() {
    runDurableConformanceArea(ConformanceArea.WATCH_LOG, 11)
}

fun testConformanceSuiteCockroachBlobstoreInlineCodecsAndArithmetic() {
    runDurableConformanceArea(ConformanceArea.INLINE_CODECS_AND_ARITHMETIC, 9)
}
