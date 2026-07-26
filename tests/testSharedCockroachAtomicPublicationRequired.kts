@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ConcurrentScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves ownership records fail closed when the real filesystem rejects ATOMIC_MOVE. */
fun testSharedCockroachAtomicPublicationRequired() {
    SharedCockroachProtocolV2Scenarios.run("atomic-publication-required")
}
