@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ConcurrentScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves attachment cannot extend the absolute startup deadline. */
fun testSharedCockroachExpiredAttachedStartup() {
    SharedCockroachProtocolV2Scenarios.run("expired-attached-startup")
}
