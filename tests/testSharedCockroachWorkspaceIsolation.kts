@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

fun testSharedCockroachWorkspaceIsolation() {
    SharedCockroachProtocolScenario.run("workspace-isolation")
}
