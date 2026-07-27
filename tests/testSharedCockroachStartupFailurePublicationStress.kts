@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves startup failure is durable before a dead child can trigger replacement election. */
fun testSharedCockroachStartupFailurePublicationStress() {
    SharedCockroachProtocolV2Scenarios.run("startup-failure-publication-stress")
}
