@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Proves readiness follows production warmup and forced warmup failure publishes no readiness. */
fun testSharedCockroachWarmupPublication() {
    SharedCockroachProtocolScenario.run("warmup-publication")
}
