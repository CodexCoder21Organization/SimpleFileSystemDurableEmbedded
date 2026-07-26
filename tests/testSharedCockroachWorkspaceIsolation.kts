@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ControlAwareScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves workspace namespaces are isolated and unknown protocol records are left unchanged. */
fun testSharedCockroachWorkspaceIsolation() {
    SharedCockroachProtocolV2Scenarios.run("workspace-isolation")
}
