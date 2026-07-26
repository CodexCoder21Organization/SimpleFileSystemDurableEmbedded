@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.runSharedCockroachProtocolScenarioTest

fun testSharedCockroachPreReadinessDaemonCrash() {
    runSharedCockroachProtocolScenarioTest("pre-readiness-daemon-crash")
}
