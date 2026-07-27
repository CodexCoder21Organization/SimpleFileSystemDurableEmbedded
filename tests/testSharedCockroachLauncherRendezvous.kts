@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture-protocol-v2:")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios

/** Proves real launcher child JVMs inherit one canonical user.dir and elect one shared node. */
fun testSharedCockroachLauncherRendezvous() {
    SharedCockroachProtocolV2Scenarios.run("launcher-rendezvous")
}
