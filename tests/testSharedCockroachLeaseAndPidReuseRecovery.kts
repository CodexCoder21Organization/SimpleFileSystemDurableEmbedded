@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.runSharedCockroachProtocolScenarioTest

fun testSharedCockroachLeaseAndPidReuseRecovery() {
    runSharedCockroachProtocolScenarioTest("lease-and-pid-reuse-recovery")
}
