@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Proves workspace namespaces are isolated and unknown protocol records are left unchanged. */
fun testSharedCockroachWorkspaceIsolation() {
    SharedCockroachProtocolScenario.run("workspace-isolation")
}
