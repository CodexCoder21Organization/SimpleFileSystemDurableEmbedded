@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Releases four armed JVMs together and proves exactly one process tree was ever spawned. */
fun testSharedCockroachDeterministicContention() {
    SharedCockroachProtocolScenario.run("deterministic-contention")
}
