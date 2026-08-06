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
import simplefilesystem.durable.testing.startSelfHostedSharedCockroachBuildPhaseFixture

fun testSharedCockroachSingleOwnerLockExclusion() {
    startSelfHostedSharedCockroachBuildPhaseFixture().use { fixture ->
        fixture.newClient().start().use { parentCluster ->
            val diagnostics = parentCluster.diagnostics()
            val directory = Files.createTempDirectory(
                fixture.workspace.toPath(),
                "single-owner-contenders-",
            ).toFile()
            val startGate = File(directory, "start")
            val releaseGate = File(directory, "release")
            val processes = mutableListOf<Process>()
            try {
                repeat(8) { index ->
                    processes += fixture.clientProcessBuilder(
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
                while ((0 until 8).any { !File(directory, "armed-$it").isFile }) {
                    checkContendersAreLive(processes, directory, "become ready before the start gate")
                    Thread.sleep(10L)
                }
                startGate.writeText("start")
                while ((0 until 8).any { !File(directory, "ready-$it.properties").isFile }) {
                    checkContendersAreLive(processes, directory, "acquire the shared fixture")
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
                    assertEquals(
                        "false",
                        result.getProperty("contenderAcquiredOwnerLock"),
                        "A contender acquired the owner lock while the build-phase fixture was live.",
                    )
                    assertEquals(
                        "false",
                        result.getProperty("fixtureOwnerChanged"),
                        "A contender changed the live build-phase fixture owner instead of reusing it.",
                    )
                }
                releaseGate.writeText("release")
                processes.forEachIndexed { index, process ->
                    process.waitFor()
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
}

private fun checkContendersAreLive(processes: List<Process>, directory: File, action: String) {
    val exited = processes.withIndex().filter { !it.value.isAlive }
    check(exited.isEmpty()) {
        "Contender JVM(s) ${exited.joinToString { it.index.toString() }} exited before all eight " +
            "real contenders could $action; logs:\n" +
            processes.indices.joinToString("\n") { index ->
                File(directory, "contender-$index.log").takeIf(File::isFile)?.readText().orEmpty()
            }
    }
}
