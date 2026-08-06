@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import simplefilesystem.durable.testing.SharedCockroachCluster

fun testSharedCockroachSingleOwnerLockExclusion() {
    SharedCockroachCluster().start().use { parentCluster ->
        val diagnostics = parentCluster.diagnostics()
        val directory = Files.createTempDirectory("single-owner-contenders-").toFile()
        val startGate = File(directory, "start")
        val releaseGate = File(directory, "release")
        val processes = mutableListOf<Process>()
        try {
            repeat(8) { index ->
                processes += ProcessBuilder(
                    File(System.getProperty("java.home"), "bin/java").absolutePath,
                    "-XX:+UseSerialGC",
                    "-XX:ActiveProcessorCount=1",
                    "-XX:TieredStopAtLevel=1",
                    "-cp",
                    System.getProperty("java.class.path"),
                    "simplefilesystem.durable.testing.SharedCockroachFixtureProbeMainKt",
                    File(directory, "armed-$index").absolutePath,
                    startGate.absolutePath,
                    File(directory, "ready-$index.properties").absolutePath,
                    releaseGate.absolutePath,
                    diagnostics.sessionOwnerPid.toString(),
                    diagnostics.sessionOwnerStartedAtMillis.toString(),
                )
                    .redirectErrorStream(true)
                    .redirectOutput(File(directory, "contender-$index.log"))
                    .start()
            }
            val armedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L)
            while ((0 until 8).any { !File(directory, "armed-$it").isFile }) {
                check(System.nanoTime() < armedDeadline) {
                    "Eight real contender JVMs did not become ready before the start gate; logs:\n" +
                        (0 until 8).joinToString("\n") { index ->
                            File(directory, "contender-$index.log").takeIf(File::isFile)?.readText().orEmpty()
                        }
                }
                Thread.sleep(10L)
            }
            startGate.writeText("start")
            val readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L)
            while ((0 until 8).any { !File(directory, "ready-$it.properties").isFile }) {
                check(System.nanoTime() < readyDeadline) {
                    "Eight real contender JVMs did not acquire the shared fixture; logs:\n" +
                        (0 until 8).joinToString("\n") { index ->
                            File(directory, "contender-$index.log").takeIf(File::isFile)?.readText().orEmpty()
                        }
                }
                Thread.sleep(10L)
            }
            val results = (0 until 8).map { index ->
                Properties().apply {
                    File(directory, "ready-$index.properties").inputStream().use(::load)
                }
            }
            assertEquals(
                setOf(diagnostics.fixtureOwnerPid.toString()),
                results.map { it.getProperty("fixtureOwnerPid") }.toSet(),
                "Every contender must reuse the one build-phase fixture owner.",
            )
            assertEquals(
                setOf(diagnostics.cockroachPid.toString()),
                results.map { it.getProperty("cockroachPid") }.toSet(),
                "Every contender must use the one CockroachDB process owned by the fixture lock holder.",
            )
            assertEquals(
                1,
                results.map { it.getProperty("clusterId") }.toSet().size,
                "Every contender must connect to the same real CockroachDB cluster.",
            )
            assertEquals(
                8,
                results.map { it.getProperty("databaseName") }.toSet().size,
                "Each contender must receive a distinct logical database.",
            )
            results.forEach { result ->
                assertNotEquals(
                    "true",
                    result.getProperty("contenderBecameOwner"),
                    "A contender replaced the live build-phase fixture owner instead of reusing it.",
                )
            }
            releaseGate.writeText("release")
            processes.forEachIndexed { index, process ->
                assertTrue(
                    process.waitFor(10L, TimeUnit.SECONDS),
                    "Contender JVM $index did not exit after release.",
                )
                assertEquals(
                    0,
                    process.exitValue(),
                    "Contender JVM $index failed:\n" +
                        File(directory, "contender-$index.log").readText(),
                )
            }
        } finally {
            releaseGate.writeText("release")
            processes.forEach { process ->
                if (process.isAlive) process.destroyForcibly()
                process.waitFor(5L, TimeUnit.SECONDS)
            }
            directory.deleteRecursively()
        }
    }
}
