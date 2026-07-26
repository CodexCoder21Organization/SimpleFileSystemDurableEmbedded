@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Proves a pre-spawn claim exists and a dead election owner cannot leave an unowned daemon. */
fun testSharedCockroachPreAttachOwnerCrash() {
    SharedCockroachProtocolScenario.run("pre-attach-owner-crash")
}
