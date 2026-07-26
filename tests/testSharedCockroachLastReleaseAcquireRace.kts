@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ControlAwareScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Drives both lock-ordered final-release/acquisition interleavings without timing luck. */
fun testSharedCockroachLastReleaseAcquireRace() {
    SharedCockroachProtocolV2Scenarios.run("last-release-acquire-race")
}
