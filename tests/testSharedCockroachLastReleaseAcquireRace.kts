@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Drives both lock-ordered final-release/acquisition interleavings without timing luck. */
fun testSharedCockroachLastReleaseAcquireRace() {
    SharedCockroachProtocolScenario.run("last-release-acquire-race")
}
