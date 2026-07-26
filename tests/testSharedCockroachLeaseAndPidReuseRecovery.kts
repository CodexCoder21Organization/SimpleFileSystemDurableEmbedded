@file:WithArtifact("simplefilesystem.durable.buildCockroachProtocolScenarioFatJar()")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolScenario

/** Proves stale purge preserves a live lease and rejects PID matches with different start times. */
fun testSharedCockroachLeaseAndPidReuseRecovery() {
    SharedCockroachProtocolScenario.run("lease-and-pid-reuse-recovery")
}
