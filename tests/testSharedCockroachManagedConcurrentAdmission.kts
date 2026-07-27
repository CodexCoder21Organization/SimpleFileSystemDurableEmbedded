@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves four managed fixture clients can share the bounded node without a host admission queue. */
fun testSharedCockroachManagedConcurrentAdmission() {
    SharedCockroachProtocolV2Scenarios.run("managed-concurrent-admission")
}
