@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves readiness follows production warmup and forced warmup failure publishes no readiness. */
fun testSharedCockroachWarmupPublication() {
    SharedCockroachProtocolV2Scenarios.run("warmup-publication")
}
