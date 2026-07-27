@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves stale purge preserves a live lease and rejects PID matches with different start times. */
fun testSharedCockroachLeaseAndPidReuseRecovery() {
    SharedCockroachProtocolV2Scenarios.run("lease-and-pid-reuse-recovery")
}
