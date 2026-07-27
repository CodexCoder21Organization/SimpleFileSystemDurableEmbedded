@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolV2ConcurrentScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves shared-cache packaging cannot archive a fixture hit without its build-only process. */
fun testSharedCockroachBuildOnlyCacheInvalidation() {
    SharedCockroachProtocolV2Scenarios.run("build-only-cache-invalidation")
}
