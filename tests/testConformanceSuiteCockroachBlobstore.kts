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
import simplefilesystem.conformance.SimpleFileSystemConformanceSuite
import simplefilesystem.conformance.SimpleFileSystemManagerFactory
import simplefilesystem.conformance.SimpleFileSystemManagerFixture
import sql.Database

fun testConformanceSuiteCockroachBlobstore() {
    val result = SimpleFileSystemConformanceSuite.runAll(
        object : SimpleFileSystemManagerFactory {
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
        },
    )
    assertEquals(69, result.totalScenarioCount)
}

