@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves malformed node and lease records remain invalid evidence with complete diagnostics. */
fun testSharedCockroachMalformedRecordDiagnostics() {
    SharedCockroachProtocolV2Scenarios.run("malformed-record-diagnostics")
}
