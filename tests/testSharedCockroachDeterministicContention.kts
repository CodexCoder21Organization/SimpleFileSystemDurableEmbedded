@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2AdmittedScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Releases four armed JVMs together and proves exactly one process tree was ever spawned. */
fun testSharedCockroachDeterministicContention() {
    SharedCockroachProtocolV2Scenarios.run("deterministic-contention")
}
