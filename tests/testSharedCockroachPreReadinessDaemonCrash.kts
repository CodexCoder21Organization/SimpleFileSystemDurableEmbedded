@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Proves a daemon killed after child spawn is reaped before one replacement is elected. */
fun testSharedCockroachPreReadinessDaemonCrash() {
    SharedCockroachProtocolScenario.run("pre-readiness-daemon-crash")
}
