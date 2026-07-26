@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ConcurrentScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves reconciliation reaps a child when its daemon dies before child identity publication. */
fun testSharedCockroachPostSpawnIdentityCrash() {
    SharedCockroachProtocolV2Scenarios.run("post-spawn-identity-crash")
}
