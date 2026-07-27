@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves a daemon killed after child spawn is reaped before one replacement is elected. */
fun testSharedCockroachPreReadinessDaemonCrash() {
    SharedCockroachProtocolV2Scenarios.run("pre-readiness-daemon-crash")
}
