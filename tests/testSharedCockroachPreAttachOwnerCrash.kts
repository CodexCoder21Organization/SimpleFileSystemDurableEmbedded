@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2AdmittedScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves a pre-spawn claim exists and a dead election owner cannot leave an unowned daemon. */
fun testSharedCockroachPreAttachOwnerCrash() {
    SharedCockroachProtocolV2Scenarios.run("pre-attach-owner-crash")
}
