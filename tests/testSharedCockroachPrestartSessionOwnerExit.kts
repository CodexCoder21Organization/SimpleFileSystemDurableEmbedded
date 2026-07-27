@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ConcurrentScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves a direct-dispatch prestart cannot outlive the Kompile session that owns its lease. */
fun testSharedCockroachPrestartSessionOwnerExit() {
    SharedCockroachProtocolV2Scenarios.run("prestart-session-owner-exit")
}
