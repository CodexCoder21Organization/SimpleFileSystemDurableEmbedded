@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves global records reap a child even when both work-directory identity files are absent. */
fun testSharedCockroachMissingLocalProcessEvidence() {
    SharedCockroachProtocolV2Scenarios.run("missing-local-process-evidence")
}
