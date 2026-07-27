@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit
import simplefilesystem.durable.testing.SharedCockroachLeaseProbe
import simplefilesystem.durable.testing.SharedCockroachProtocolV2Scenarios
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

fun testSharedCockroachCrossProcessElectionAndRecovery() =
    SharedCockroachProtocolV2Scenarios.withHostAdmission {
    var privateTemp: File? = null
    var managedStateDirectory: File? = null
    val javaBinary = File(System.getProperty("java.home"), "bin/java").absolutePath
    val fixtureJar = File(
        SharedCockroachLeaseProbe::class.java.protectionDomain.codeSource.location.toURI(),
    ).absolutePath
    val processes = mutableListOf<Process>()
    val releases = mutableListOf<File>()
    val observedIdentities = linkedSetOf<Pair<Long, Long>>()
    var testFailure: Throwable? = null
    try {
        privateTemp = Files.createTempDirectory("durable-cross-process-node-").toFile()
        val privateTemp = requireNotNull(privateTemp)
        val readyFiles = (0 until 4).map { index -> File(privateTemp, "ready-$index") }
        repeat(4) { index ->
            val release = File(privateTemp, "release-$index")
            releases += release
            val process = ProcessBuilder(
                javaBinary,
                "-XX:+UseSerialGC",
                "-XX:ActiveProcessorCount=1",
                "-XX:TieredStopAtLevel=1",
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

        waitForCrossProcessReadyFiles(readyFiles, TimeUnit.SECONDS.toNanos(20L))
        readyFiles.forEachIndexed { index, ready ->
            check(ready.isFile) {
                "Cross-process CockroachDB child $index did not become ready; output:\n" +
                    File(privateTemp, "child-$index.log").takeIf(File::isFile)?.readText().orEmpty()
            }
        }

        val readyProperties = readyFiles.map { readyFile ->
            Properties().apply {
                readyFile.inputStream().use(::load)
            }
        }
        readyProperties.forEach { properties ->
            observedIdentities +=
                requireNotNull(properties.getProperty("daemonPid")).toLong() to
                requireNotNull(properties.getProperty("daemonStartedAtMillis")).toLong()
            observedIdentities +=
                requireNotNull(properties.getProperty("cockroachPid")).toLong() to
                requireNotNull(properties.getProperty("cockroachStartedAtMillis")).toLong()
        }
        val jdbcUrls = readyProperties.map { requireNotNull(it.getProperty("jdbcUrl")) }
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
            assertProbeDatabaseSchemaInitialized(
                jdbcUrl,
                "Forked JVM $index must initialize its isolated database through the public manager.",
            )
        }

        managedStateDirectory = File(
            requireNotNull(readyProperties.first().getProperty("stateDirectory")),
        ).canonicalFile
        assertTrue(
            managedStateDirectory!!.name.startsWith(
                "simplefilesystem-durable-shared-cockroach-v2-",
            ),
            "Managed state namespace must use protocol v2 plus a stable workspace digest, but was " +
                managedStateDirectory!!.absolutePath,
        )
        val stateFile = File(managedStateDirectory, "node.properties")
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
        val stagedCorruptState = Files.createTempFile(
            stateFile.parentFile.toPath(),
            ".node-properties-corruption-",
            ".properties",
        )
        try {
            stagedCorruptState.toFile().outputStream().use { originalState.store(it, null) }
            Files.move(
                stagedCorruptState,
                stateFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } finally {
            Files.deleteIfExists(stagedCorruptState)
        }

        val stateRecoveryReady = File(privateTemp, "ready-state-recovery")
        val stateRecoveryRelease = File(privateTemp, "release-state-recovery")
        releases += stateRecoveryRelease
        val stateRecovery = ProcessBuilder(
            javaBinary,
            "-XX:+UseSerialGC",
            "-XX:ActiveProcessorCount=1",
            "-XX:TieredStopAtLevel=1",
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
        waitForCrossProcessReadyFiles(
            listOf(stateRecoveryReady),
            TimeUnit.SECONDS.toNanos(5L),
        )
        check(stateRecoveryReady.isFile) {
            "A new JVM did not recover the live node from corrupt node state; output:\n" +
                File(privateTemp, "child-state-recovery.log").takeIf(File::isFile)?.readText().orEmpty()
        }
        val preservedInvalidStates = File(
            managedStateDirectory,
            "quarantine/invalid-readiness",
        ).listFiles().orEmpty().filter {
            it.isFile && it.name.startsWith("node.properties.") && it.name.endsWith(".invalid")
        }
        assertEquals(
            1,
            preservedInvalidStates.size,
            "Recovering from the warmup proof must preserve the invalid node record exactly once.",
        )
        val preservedInvalidState = Properties().apply {
            preservedInvalidStates.single().inputStream().use(::load)
        }
        assertEquals(
            "truncated-state-file-url",
            preservedInvalidState.getProperty("jdbcUrl"),
            "Recovery must preserve the invalid JDBC URL as diagnostic evidence.",
        )
        assertEquals(
            File(privateTemp, "unsafe-work-directory").absolutePath,
            preservedInvalidState.getProperty("workDirectory"),
            "Recovery must preserve the untrusted work-directory value as diagnostic evidence.",
        )
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
        val stateRecoveryProperties = Properties().apply {
            stateRecoveryReady.inputStream().use(::load)
        }
        observedIdentities +=
            requireNotNull(stateRecoveryProperties.getProperty("daemonPid")).toLong() to
            requireNotNull(stateRecoveryProperties.getProperty("daemonStartedAtMillis")).toLong()
        observedIdentities +=
            requireNotNull(stateRecoveryProperties.getProperty("cockroachPid")).toLong() to
            requireNotNull(stateRecoveryProperties.getProperty("cockroachStartedAtMillis")).toLong()
        DriverManager.getConnection(
            requireNotNull(stateRecoveryProperties.getProperty("jdbcUrl")),
            "root",
            "",
        ).use { connection ->
            assertTrue(connection.isValid(2), "The state-recovered logical database must accept JDBC connections.")
        }
        assertProbeDatabaseSchemaInitialized(
            requireNotNull(stateRecoveryProperties.getProperty("jdbcUrl")),
            "A second acquisition must adopt the live node and initialize its isolated database.",
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
            "-XX:+UseSerialGC",
            "-XX:ActiveProcessorCount=1",
            "-XX:TieredStopAtLevel=1",
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
        waitForCrossProcessReadyFiles(
            listOf(staleLeaseRecoveryReady),
            TimeUnit.SECONDS.toNanos(5L),
        )
        check(staleLeaseRecoveryReady.isFile) {
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
        val staleLeaseRecoveryProperties = Properties().apply {
            staleLeaseRecoveryReady.inputStream().use(::load)
        }
        observedIdentities +=
            requireNotNull(staleLeaseRecoveryProperties.getProperty("daemonPid")).toLong() to
            requireNotNull(staleLeaseRecoveryProperties.getProperty("daemonStartedAtMillis")).toLong()
        observedIdentities +=
            requireNotNull(staleLeaseRecoveryProperties.getProperty("cockroachPid")).toLong() to
            requireNotNull(staleLeaseRecoveryProperties.getProperty("cockroachStartedAtMillis")).toLong()
        assertProbeDatabaseSchemaInitialized(
            requireNotNull(staleLeaseRecoveryProperties.getProperty("jdbcUrl")),
            "The acquisition that purges a stale lease must initialize its isolated database.",
        )

        val originalNode = ProcessHandle.of(originalPid).orElseThrow {
            IllegalStateException("The elected CockroachDB process $originalPid was not live.")
        }
        assertTrue(originalNode.destroyForcibly(), "Could not kill elected CockroachDB process $originalPid.")
        originalNode.onExit().get(10L, TimeUnit.SECONDS)
        assertFalse(
            originalNode.isAlive,
            "CockroachDB ProcessHandle.onExit completed for PID $originalPid, but the same handle " +
                "still reported isAlive=true with command=" +
                originalNode.info().command().orElse("<missing>") + ".",
        )

        val recoveryReady = File(privateTemp, "ready-recovery")
        val recoveryRelease = File(privateTemp, "release-recovery")
        releases += recoveryRelease
        val recovery = ProcessBuilder(
            javaBinary,
            "-XX:+UseSerialGC",
            "-XX:ActiveProcessorCount=1",
            "-XX:TieredStopAtLevel=1",
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

        waitForCrossProcessReadyFiles(
            listOf(recoveryReady),
            TimeUnit.SECONDS.toNanos(20L),
        )
        check(recoveryReady.isFile) {
            "A new JVM did not replace the crashed shared CockroachDB node; output:\n" +
                File(privateTemp, "child-recovery.log").takeIf(File::isFile)?.readText().orEmpty() +
                "\nManaged state evidence:\n" +
                managedStateDirectory!!.walkTopDown().joinToString("\n") { evidence ->
                    if (evidence.isFile && evidence.length() < 32_768L) {
                        "${evidence.absolutePath}:\n${evidence.readText()}"
                    } else {
                        evidence.absolutePath
                    }
                }
        }
        val recoveryProperties = Properties().apply {
            recoveryReady.inputStream().use(::load)
        }
        observedIdentities +=
            requireNotNull(recoveryProperties.getProperty("daemonPid")).toLong() to
            requireNotNull(recoveryProperties.getProperty("daemonStartedAtMillis")).toLong()
        observedIdentities +=
            requireNotNull(recoveryProperties.getProperty("cockroachPid")).toLong() to
            requireNotNull(recoveryProperties.getProperty("cockroachStartedAtMillis")).toLong()
        val recoveredJdbcUrl = requireNotNull(recoveryProperties.getProperty("jdbcUrl"))
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
            File(stateFile.parentFile, "node-owner.properties").exists(),
            "Final lease release must remove the durable owner claim.",
        )
        assertFalse(
            File(stateFile.parentFile, "node-heartbeat.properties").exists(),
            "Final lease release must remove the daemon ownership heartbeat.",
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
        observedIdentities.forEach { (pid, startedAtMillis) ->
            val sameProcessIsAlive = ProcessHandle.of(pid).map { handle ->
                handle.isAlive &&
                    handle.info().startInstant().orElse(null)?.toEpochMilli() == startedAtMillis
            }.orElse(false)
            assertFalse(
                sameProcessIsAlive,
                "Every observed fixture daemon and CockroachDB identity must be dead after final " +
                    "release, but PID $pid started at $startedAtMillis was still alive.",
            )
        }
    } catch (failure: Throwable) {
        testFailure = failure
        throw failure
    } finally {
        val cleanupFailures = mutableListOf<Throwable>()
        val cleanupRoot = privateTemp
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
        var fixtureProcessesConfirmedDead = processes.none(Process::isAlive)
        val stateDirectories = linkedSetOf<File>()
        managedStateDirectory?.takeIf(File::isDirectory)?.let(stateDirectories::add)
        cleanupRoot?.listFiles().orEmpty()
            .filter {
                it.isDirectory &&
                    it.name.startsWith("simplefilesystem-durable-shared-cockroach-v2-")
            }
            .forEach(stateDirectories::add)
        stateDirectories.forEach { stateDirectory ->
            try {
                cleanupCrossProcessFixtureProcesses(stateDirectory)
            } catch (failure: Throwable) {
                fixtureProcessesConfirmedDead = false
                cleanupFailures += failure
            }
        }
        if (fixtureProcessesConfirmedDead &&
            cleanupRoot != null &&
            cleanupRoot.exists() &&
            !cleanupRoot.deleteRecursively()
        ) {
            cleanupFailures += IllegalStateException(
                "Could not delete private cross-process test directory ${cleanupRoot.absolutePath}.",
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

fun waitForCrossProcessReadyFiles(readyFiles: List<File>, timeoutNanos: Long) {
    require(readyFiles.isNotEmpty()) {
        "At least one cross-process readiness file is required, but the list was empty."
    }
    val parent = readyFiles.first().parentFile.canonicalFile
    require(readyFiles.all { it.parentFile.canonicalFile == parent }) {
        "Cross-process readiness files must share parent ${parent.absolutePath}, but were " +
            readyFiles.joinToString { it.absolutePath }
    }
    FileSystems.getDefault().newWatchService().use { watcher ->
        parent.toPath().register(
            watcher,
            StandardWatchEventKinds.ENTRY_CREATE,
        )
        val deadline = System.nanoTime() + timeoutNanos
        while (readyFiles.any { !it.isFile }) {
            val remaining = deadline - System.nanoTime()
            if (remaining <= 0L) return
            val key = watcher.poll(remaining, TimeUnit.NANOSECONDS) ?: return
            key.pollEvents()
            if (!key.reset()) {
                return
            }
        }
    }
}

fun cleanupCrossProcessFixtureProcesses(stateDirectory: File) {
    val identities = linkedSetOf<Triple<Long, Long, Long?>>()
    val failures = mutableListOf<Throwable>()
    fun record(
        file: File,
        pidKey: String,
        startedAtKey: String,
        processGroupKey: String?,
    ) {
        if (!file.isFile) return
        try {
            val properties = Properties().apply {
                file.inputStream().use(::load)
            }
            val pidValue = properties.getProperty(pidKey)
                ?: throw IllegalStateException(
                    "Fixture identity file ${file.absolutePath} did not contain $pidKey.",
                )
            val startedAtValue = properties.getProperty(startedAtKey)
                ?: throw IllegalStateException(
                    "Fixture identity file ${file.absolutePath} contained $pidKey='$pidValue' " +
                        "without $startedAtKey.",
                )
            val pid = pidValue.toLongOrNull()
                ?: throw IllegalStateException(
                    "Fixture identity file ${file.absolutePath} contained non-numeric " +
                        "$pidKey='$pidValue'.",
                )
            val startedAt = startedAtValue.toLongOrNull()
                ?: throw IllegalStateException(
                    "Fixture identity file ${file.absolutePath} contained non-numeric " +
                        "$startedAtKey='$startedAtValue'.",
                )
            val processGroupId = processGroupKey?.let { key ->
                val value = properties.getProperty(key)
                    ?: throw IllegalStateException(
                        "Fixture identity file ${file.absolutePath} contained $pidKey='$pidValue' " +
                            "without $key.",
                    )
                value.toLongOrNull() ?: throw IllegalStateException(
                    "Fixture identity file ${file.absolutePath} contained non-numeric $key='$value'.",
                )
            }
            identities += Triple(
                pid,
                startedAt,
                processGroupId,
            )
        } catch (failure: Throwable) {
            failures += failure
        }
    }

    val nodeState = File(stateDirectory, "node.properties")
    record(nodeState, "daemonPid", "daemonStartedAtMillis", "processGroupId")
    record(nodeState, "pid", "processStartedAtMillis", "processGroupId")
    record(
        File(stateDirectory, "node-owner.properties"),
        "daemonPid",
        "daemonStartedAtMillis",
        "daemonProcessGroupId",
    )
    stateDirectory.listFiles().orEmpty()
        .filter { it.isDirectory && it.name.startsWith("node-") }
        .forEach { workDirectory ->
            record(
                File(workDirectory, "daemon.properties"),
                "pid",
                "startedAtMillis",
                "processGroupId",
            )
            record(
                File(workDirectory, "cockroach.properties"),
                "pid",
                "startedAtMillis",
                "processGroupId",
            )
        }
    identities.mapNotNull { it.third }.distinct().forEach { processGroupId ->
        try {
            val leader = identities.singleOrNull { it.first == processGroupId }
                ?: throw IllegalStateException(
                    "Fixture evidence for process group $processGroupId did not contain exactly " +
                        "one group-leader identity.",
                )
            val handle = ProcessHandle.of(leader.first).orElse(null)
            if (handle != null &&
                handle.isAlive &&
                handle.info().startInstant().orElse(null)?.toEpochMilli() ==
                leader.second
            ) {
                val processGroupInspection = ProcessBuilder(
                    "/bin/ps",
                    "-o",
                    "pgid=",
                    "-p",
                    leader.first.toString(),
                ).start()
                val actualProcessGroupId = processGroupInspection.inputStream
                    .bufferedReader()
                    .use { it.readText() }
                    .trim()
                    .toLongOrNull()
                val inspectionExitCode = processGroupInspection.waitFor()
                check(
                    inspectionExitCode == 0 && actualProcessGroupId == processGroupId,
                ) {
                    "Could not verify fixture process-group leader PID ${leader.first} started at " +
                        "${leader.second}: expected process group $processGroupId, but /bin/ps " +
                        "exited with code $inspectionExitCode and reported '$actualProcessGroupId'."
                }
                val kill = ProcessBuilder(
                    "/bin/kill",
                    "-KILL",
                    "--",
                    "-$processGroupId",
                ).start()
                val exitCode = kill.waitFor()
                check(exitCode == 0 || !handle.isAlive) {
                    "Could not signal CockroachDB process group $processGroupId; /bin/kill " +
                        "exited with code $exitCode and its verified leader remained alive."
                }
            }
        } catch (failure: Throwable) {
            failures += failure
        }
    }
    identities.forEach { identity ->
        val (pid, startedAtMillis) = identity
        try {
            val handle = ProcessHandle.of(pid).orElse(null)
            if (handle != null &&
                handle.isAlive &&
                handle.info().startInstant().orElse(null)?.toEpochMilli() ==
                startedAtMillis
            ) {
                handle.destroyForcibly()
                handle.onExit().get(10L, TimeUnit.SECONDS)
                check(!handle.isAlive) {
                    "Fixture process $pid started at $startedAtMillis " +
                        "remained alive after forcible cleanup."
                }
            }
        } catch (failure: Throwable) {
            failures += failure
        }
    }
    if (failures.isNotEmpty()) {
        val failure = IllegalStateException(
            "Cross-process fixture cleanup failed ${failures.size} time(s); the state directory " +
                "${stateDirectory.absolutePath} was preserved with its process identity evidence.",
        )
        failures.forEach(failure::addSuppressed)
        throw failure
    }
}

fun assertProbeDatabaseSchemaInitialized(jdbcUrl: String, message: String) {
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
