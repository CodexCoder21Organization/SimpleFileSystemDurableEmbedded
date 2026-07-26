@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.nio.file.Files
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit
import simplefilesystem.durable.testing.SharedCockroachLeaseProbe
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

fun testSharedCockroachCrossProcessElectionAndRecovery() {
    val privateTemp = Files.createTempDirectory("durable-cross-process-node-").toFile()
    val javaBinary = File(System.getProperty("java.home"), "bin/java").absolutePath
    val fixtureJar = File(
        SharedCockroachLeaseProbe::class.java.protectionDomain.codeSource.location.toURI(),
    ).absolutePath
    val processes = mutableListOf<Process>()
    val releases = mutableListOf<File>()
    var testFailure: Throwable? = null
    try {
        val readyFiles = (0 until 4).map { index -> File(privateTemp, "ready-$index") }
        repeat(4) { index ->
            val release = File(privateTemp, "release-$index")
            releases += release
            val process = ProcessBuilder(
                javaBinary,
                "-Djava.io.tmpdir=${privateTemp.absolutePath}",
                "-cp",
                fixtureJar,
                "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
                readyFiles[index].absolutePath,
                release.absolutePath,
            )
                .redirectErrorStream(true)
                .redirectOutput(File(privateTemp, "child-$index.log"))
                .also {
                    it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
                }
                .start()
            processes += process
        }

        val readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L)
        while (readyFiles.any { !it.isFile || it.length() == 0L } && System.nanoTime() < readyDeadline) {
            Thread.sleep(20L)
        }
        readyFiles.forEachIndexed { index, ready ->
            check(ready.isFile && ready.length() > 0L) {
                "Cross-process CockroachDB child $index did not become ready; output:\n" +
                    File(privateTemp, "child-$index.log").takeIf(File::isFile)?.readText().orEmpty()
            }
        }

        val jdbcUrls = readyFiles.map { it.readText().trim() }
        assertEquals(
            1,
            jdbcUrls.map { it.substringBeforeLast('/') }.distinct().size,
            "All four independently forked JVMs must elect one CockroachDB node.",
        )
        assertEquals(
            4,
            jdbcUrls.distinct().size,
            "Every independently forked JVM must receive a distinct logical database.",
        )
        jdbcUrls.forEachIndexed { index, jdbcUrl ->
            assertDurableSchemaReady(
                jdbcUrl,
                "Forked JVM $index must publish readiness only after production schema bootstrap.",
            )
        }

        val stateFile = File(
            privateTemp,
            "simplefilesystem-durable-shared-cockroach-v1/node.properties",
        )
        val originalState = Properties().apply {
            stateFile.inputStream().use(::load)
        }
        val originalPid = requireNotNull(originalState.getProperty("pid")).toLong()
        val originalWorkDirectory = File(
            requireNotNull(originalState.getProperty("workDirectory")),
        ).canonicalFile
        originalState.setProperty("jdbcUrl", "truncated-state-file-url")
        originalState.setProperty(
            "workDirectory",
            File(privateTemp, "unsafe-work-directory").absolutePath,
        )
        stateFile.outputStream().use { originalState.store(it, null) }

        val stateRecoveryReady = File(privateTemp, "ready-state-recovery")
        val stateRecoveryRelease = File(privateTemp, "release-state-recovery")
        releases += stateRecoveryRelease
        val stateRecovery = ProcessBuilder(
            javaBinary,
            "-Djava.io.tmpdir=${privateTemp.absolutePath}",
            "-cp",
            fixtureJar,
            "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
            stateRecoveryReady.absolutePath,
            stateRecoveryRelease.absolutePath,
        )
            .redirectErrorStream(true)
            .redirectOutput(File(privateTemp, "child-state-recovery.log"))
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
            }
            .start()
        processes += stateRecovery
        val stateRecoveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L)
        while ((!stateRecoveryReady.isFile || stateRecoveryReady.length() == 0L) &&
            System.nanoTime() < stateRecoveryDeadline
        ) {
            Thread.sleep(20L)
        }
        check(stateRecoveryReady.isFile && stateRecoveryReady.length() > 0L) {
            "A new JVM did not recover the live node from corrupt node state; output:\n" +
                File(privateTemp, "child-state-recovery.log").takeIf(File::isFile)?.readText().orEmpty()
        }
        val repairedState = Properties().apply {
            stateFile.inputStream().use(::load)
        }
        assertEquals(
            originalPid,
            requireNotNull(repairedState.getProperty("pid")).toLong(),
            "Recovering corrupt state must adopt the healthy live node instead of starting a second node.",
        )
        assertEquals(
            originalWorkDirectory,
            File(requireNotNull(repairedState.getProperty("workDirectory"))).canonicalFile,
            "State recovery must not retain an untrusted work-directory path.",
        )
        DriverManager.getConnection(stateRecoveryReady.readText().trim(), "root", "").use { connection ->
            assertTrue(connection.isValid(2), "The state-recovered logical database must accept JDBC connections.")
        }
        assertDurableSchemaReady(
            stateRecoveryReady.readText().trim(),
            "A second bootstrap attempt must adopt the live node and initialize its isolated schema.",
        )

        releases[0].writeText("release")
        assertTrue(processes[0].waitFor(10L, TimeUnit.SECONDS), "The first lease holder did not exit.")
        assertEquals(0, processes[0].exitValue(), "The first lease holder failed during normal cleanup.")
        DriverManager.getConnection(jdbcUrls[1], "root", "").use { connection ->
            assertTrue(connection.isValid(2), "Closing one JVM must not stop the node used by live JVM leases.")
        }

        processes[1].destroyForcibly()
        assertTrue(
            processes[1].waitFor(10L, TimeUnit.SECONDS),
            "The deliberately killed lease holder ${processes[1].pid()} did not exit.",
        )
        val leasesDirectory = File(stateFile.parentFile, "leases")
        val staleLease = File(
            leasesDirectory,
            jdbcUrls[1].substringAfterLast('/').substringBefore('?'),
        )
        assertTrue(
            staleLease.isFile,
            "Killing lease holder ${processes[1].pid()} must leave its lease at ${staleLease.absolutePath}.",
        )
        val staleLeaseRecoveryReady = File(privateTemp, "ready-stale-lease-recovery")
        val staleLeaseRecoveryRelease = File(privateTemp, "release-stale-lease-recovery")
        releases += staleLeaseRecoveryRelease
        val staleLeaseRecovery = ProcessBuilder(
            javaBinary,
            "-Djava.io.tmpdir=${privateTemp.absolutePath}",
            "-cp",
            fixtureJar,
            "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
            staleLeaseRecoveryReady.absolutePath,
            staleLeaseRecoveryRelease.absolutePath,
        )
            .redirectErrorStream(true)
            .redirectOutput(File(privateTemp, "child-stale-lease-recovery.log"))
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
            }
            .start()
        processes += staleLeaseRecovery
        val staleLeaseRecoveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L)
        while ((!staleLeaseRecoveryReady.isFile || staleLeaseRecoveryReady.length() == 0L) &&
            System.nanoTime() < staleLeaseRecoveryDeadline
        ) {
            Thread.sleep(20L)
        }
        check(staleLeaseRecoveryReady.isFile && staleLeaseRecoveryReady.length() > 0L) {
            "A new JVM did not purge the killed holder's stale lease and adopt the live node; output:\n" +
                File(privateTemp, "child-stale-lease-recovery.log")
                    .takeIf(File::isFile)?.readText().orEmpty()
        }
        val stateAfterStaleLeaseRecovery = Properties().apply {
            stateFile.inputStream().use(::load)
        }
        assertEquals(
            originalPid,
            requireNotNull(stateAfterStaleLeaseRecovery.getProperty("pid")).toLong(),
            "Purging a killed holder's stale lease must preserve and adopt the healthy node.",
        )
        assertFalse(
            staleLease.exists(),
            "A new acquisition must purge stale lease ${staleLease.absolutePath}.",
        )
        assertDurableSchemaReady(
            staleLeaseRecoveryReady.readText().trim(),
            "The acquisition that purges a stale lease must initialize its isolated schema.",
        )

        val originalNode = ProcessHandle.of(originalPid).orElseThrow {
            IllegalStateException("The elected CockroachDB process $originalPid was not live.")
        }
        assertTrue(originalNode.destroyForcibly(), "Could not kill elected CockroachDB process $originalPid.")
        originalNode.onExit().get(10L, TimeUnit.SECONDS)

        val recoveryReady = File(privateTemp, "ready-recovery")
        val recoveryRelease = File(privateTemp, "release-recovery")
        releases += recoveryRelease
        val recovery = ProcessBuilder(
            javaBinary,
            "-Djava.io.tmpdir=${privateTemp.absolutePath}",
            "-cp",
            fixtureJar,
            "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
            recoveryReady.absolutePath,
            recoveryRelease.absolutePath,
        )
            .redirectErrorStream(true)
            .redirectOutput(File(privateTemp, "child-recovery.log"))
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
            }
            .start()
        processes += recovery

        val recoveryDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L)
        while ((!recoveryReady.isFile || recoveryReady.length() == 0L) &&
            System.nanoTime() < recoveryDeadline
        ) {
            Thread.sleep(20L)
        }
        check(recoveryReady.isFile && recoveryReady.length() > 0L) {
            "A new JVM did not replace the crashed shared CockroachDB node; output:\n" +
                File(privateTemp, "child-recovery.log").takeIf(File::isFile)?.readText().orEmpty()
        }
        val recoveredJdbcUrl = recoveryReady.readText().trim()
        val recoveredPid = Properties().apply {
            stateFile.inputStream().use(::load)
        }.getProperty("pid").toLong()
        assertNotEquals(
            originalPid,
            recoveredPid,
            "A post-crash acquisition must use a replacement CockroachDB node.",
        )
        DriverManager.getConnection(recoveredJdbcUrl, "root", "").use { connection ->
            assertTrue(connection.isValid(2), "The replacement CockroachDB node must accept JDBC connections.")
        }

        releases.drop(2).forEach { it.writeText("release") }
        processes.drop(2).forEachIndexed { index, process ->
            val logSuffix = when (index) {
                0, 1 -> (index + 2).toString()
                2 -> "state-recovery"
                3 -> "stale-lease-recovery"
                else -> "recovery"
            }
            assertTrue(
                process.waitFor(10L, TimeUnit.SECONDS),
                "Cross-process lease holder ${index + 1} did not exit.",
            )
            assertEquals(
                0,
                process.exitValue(),
                "Cross-process lease holder ${index + 1} failed; output:\n" +
                    File(privateTemp, "child-$logSuffix.log").takeIf(File::isFile)?.readText().orEmpty(),
            )
        }
        assertFalse(
            ProcessHandle.of(recoveredPid).map(ProcessHandle::isAlive).orElse(false),
            "The replacement CockroachDB node must stop after its final live JVM lease exits.",
        )
        assertFalse(
            stateFile.exists(),
            "Final lease release must remove shared node state at ${stateFile.absolutePath}.",
        )
        assertFalse(
            File(stateFile.parentFile, "node-starting.properties").exists(),
            "Final lease release must remove in-progress node state.",
        )
        assertTrue(
            leasesDirectory.listFiles().orEmpty().none { it.isFile },
            "Final lease release must remove every lease from ${leasesDirectory.absolutePath}.",
        )
        assertTrue(
            stateFile.parentFile.listFiles().orEmpty().none {
                it.isDirectory && it.name.startsWith("node-")
            },
            "Final lease release must remove every node-* work directory from " +
                "${stateFile.parentFile.absolutePath}.",
        )
    } catch (failure: Throwable) {
        testFailure = failure
        throw failure
    } finally {
        val cleanupFailures = mutableListOf<Throwable>()
        releases.forEach { release ->
            try {
                release.writeText("release")
            } catch (failure: Throwable) {
                cleanupFailures += failure
            }
        }
        processes.forEach { process ->
            if (process.isAlive) process.destroyForcibly()
        }
        processes.forEach { process ->
            try {
                check(process.waitFor(5L, TimeUnit.SECONDS)) {
                    "Cross-process probe ${process.pid()} remained alive during test cleanup."
                }
            } catch (failure: Throwable) {
                cleanupFailures += failure
            }
        }
        val stateFile = File(
            privateTemp,
            "simplefilesystem-durable-shared-cockroach-v1/node.properties",
        )
        if (stateFile.isFile) {
            try {
                val nodePid = Properties().apply {
                    stateFile.inputStream().use(::load)
                }.getProperty("pid").toLong()
                ProcessHandle.of(nodePid).ifPresent { node ->
                    if (node.isAlive) {
                        node.destroyForcibly()
                        node.onExit().get(10L, TimeUnit.SECONDS)
                    }
                }
            } catch (failure: Throwable) {
                cleanupFailures += failure
            }
        }
        if (privateTemp.exists() && !privateTemp.deleteRecursively()) {
            cleanupFailures += IllegalStateException(
                "Could not delete private cross-process test directory ${privateTemp.absolutePath}.",
            )
        }
        val primaryFailure = testFailure
        if (primaryFailure != null) {
            cleanupFailures.forEach(primaryFailure::addSuppressed)
        } else if (cleanupFailures.isNotEmpty()) {
            val cleanupFailure = IllegalStateException(
                "Cross-process CockroachDB test cleanup failed ${cleanupFailures.size} time(s).",
            )
            cleanupFailures.forEach(cleanupFailure::addSuppressed)
            throw cleanupFailure
        }
    }
}

fun assertDurableSchemaReady(jdbcUrl: String, message: String) {
    DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery(
                "SELECT schema_version FROM simple_filesystem_schema_version WHERE singleton = true",
            ).use { rows ->
                assertTrue(rows.next(), "$message No singleton schema-version row existed at $jdbcUrl.")
                assertEquals(2, rows.getInt("schema_version"), "$message JDBC URL: $jdbcUrl.")
                assertFalse(rows.next(), "$message More than one singleton row existed at $jdbcUrl.")
            }
        }
    }
}
