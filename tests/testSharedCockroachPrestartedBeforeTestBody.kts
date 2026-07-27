@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import kotlin.test.assertTrue
import simplefilesystem.durable.testing.SharedCockroachCluster

fun testSharedCockroachPrestartedBeforeTestBody() {
    SharedCockroachCluster().start().use { cluster ->
        assertTrue(
            cluster.fixtureWasReadyBeforeStart(),
            "The shared CockroachDB fixture must be ready before this test body's per-test clock " +
                "starts, but this test had to wait for managed-node startup.",
        )
    }
}
