@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

fun testSharedCockroachPreAttachOwnerCrash() {
    SharedCockroachProtocolScenario.run("pre-attach-owner-crash")
}
