@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves attachment cannot extend the absolute startup deadline. */
fun testSharedCockroachExpiredAttachedStartup() {
    SharedCockroachProtocolV2Scenarios.run("expired-attached-startup")
}
