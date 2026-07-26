package simplefilesystem.durable.testing

import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Entry point and marker for the protocol-v2 scenario executable in the fixture jar. */
object SharedCockroachProtocolV2Scenarios {
    fun run(scenario: String) {
        runSharedCockroachProtocolV2ScenarioTest(scenario)
    }
}

/**
 * Runs one end-to-end shared-CockroachDB protocol scenario in a clean child process.
 *
 * The child has no suite-fixture JDBC override, so every scenario exercises the workspace-scoped
 * detached-daemon protocol rather than the suite launcher's already-running CockroachDB node.
 */
private fun runSharedCockroachProtocolV2ScenarioTest(scenario: String) {
    var root: File? = null
    var process: Process? = null
    var failure: Throwable? = null
    try {
        root = Files.createTempDirectory("shared-cockroach-$scenario-").toFile()
        val fixtureJar = File(
            SharedCockroachProtocolV2Scenarios::class.java.protectionDomain.codeSource.location.toURI(),
        )
        check(fixtureJar.isFile) {
            "The shared CockroachDB protocol scenario runtime must be a jar file, but was " +
                "${fixtureJar.absolutePath}."
        }
        val javaBinary = File(System.getProperty("java.home"), "bin/java")
        process = ProcessBuilder(
            javaBinary.absolutePath,
            "-Djava.io.tmpdir=${root.absolutePath}",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachProtocolScenarioMainKt",
            scenario,
            root.absolutePath,
        )
            .directory(File(System.getProperty("user.dir")).canonicalFile)
            .redirectErrorStream(true)
            .redirectOutput(File(root, "scenario.log"))
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
            }
            .start()
        check(process.waitFor(20L, TimeUnit.SECONDS)) {
            "Shared CockroachDB protocol scenario '$scenario' did not finish within 20 seconds; " +
                "output:\n${File(root, "scenario.log").takeIf(File::isFile)?.readText().orEmpty()}"
        }
        check(process.exitValue() == 0) {
            "Shared CockroachDB protocol scenario '$scenario' exited with code " +
                "${process.exitValue()}; output:\n" +
                File(root, "scenario.log").takeIf(File::isFile)?.readText().orEmpty()
        }
    } catch (caught: Throwable) {
        failure = caught
        throw caught
    } finally {
        val cleanupFailures = mutableListOf<Throwable>()
        process?.takeIf(Process::isAlive)?.let { liveProcess ->
            try {
                liveProcess.destroyForcibly()
                check(liveProcess.waitFor(10L, TimeUnit.SECONDS)) {
                    "Shared CockroachDB protocol scenario '$scenario' process ${liveProcess.pid()} " +
                        "remained alive after forcible cleanup."
                }
            } catch (cleanupFailure: Throwable) {
                cleanupFailures += cleanupFailure
            }
        }
        root?.let { scenarioRoot ->
            try {
                cleanupSharedCockroachProtocolV2ScenarioEvidence(
                    scenarioRoot,
                    deleteRoot = true,
                )
            } catch (cleanupFailure: Throwable) {
                cleanupFailures += cleanupFailure
            }
        }
        if (cleanupFailures.isNotEmpty()) {
            val cleanupFailure = IllegalStateException(
                "Shared CockroachDB protocol scenario '$scenario' cleanup failed " +
                    "${cleanupFailures.size} time(s).",
            )
            cleanupFailures.forEach(cleanupFailure::addSuppressed)
            failure?.addSuppressed(cleanupFailure) ?: throw cleanupFailure
        }
    }
}

/**
 * Reaps every independently recorded daemon/CockroachDB identity. Malformed records and individual
 * kill failures are accumulated so one bad record cannot prevent later identities from being
 * handled. Evidence is deleted only after every readable identity is confirmed dead and every
 * identity record was parseable.
 */
internal fun cleanupSharedCockroachProtocolV2ScenarioEvidence(root: File, deleteRoot: Boolean) {
    val failures = mutableListOf<Throwable>()
    val identities = linkedMapOf<Triple<Long, Long, Boolean>, ScenarioProcessIdentity>()
    root.walkTopDown()
        .filter(::isScenarioIdentityRecord)
        .toList()
        .forEach { identityFile ->
            try {
                val properties = Properties().apply {
                    identityFile.inputStream().use(::load)
                }
                val pidValue = properties.getProperty("pid")
                    ?: throw IllegalStateException(
                        "Cleanup identity ${identityFile.absolutePath} did not contain pid.",
                    )
                val startedAtValue = properties.getProperty("startedAtMillis")
                    ?: throw IllegalStateException(
                        "Cleanup identity ${identityFile.absolutePath} contained pid='$pidValue' " +
                            "without startedAtMillis.",
                    )
                val pid = pidValue.toLongOrNull()
                    ?: throw IllegalStateException(
                        "Cleanup identity ${identityFile.absolutePath} contained non-numeric " +
                            "pid='$pidValue'.",
                    )
                val startedAtMillis = startedAtValue.toLongOrNull()
                    ?: throw IllegalStateException(
                        "Cleanup identity ${identityFile.absolutePath} contained non-numeric " +
                            "startedAtMillis='$startedAtValue'.",
                    )
                val cockroach = identityFile.name.startsWith("cockroach")
                val groupValue = properties.getProperty("processGroupId")
                val processGroupId = if (groupValue == null) {
                    if (cockroach) pid else null
                } else {
                    groupValue.toLongOrNull()
                        ?: throw IllegalStateException(
                            "Cleanup identity ${identityFile.absolutePath} contained non-numeric " +
                                "processGroupId='$groupValue'.",
                        )
                }
                if (cockroach && processGroupId != pid) {
                    throw IllegalStateException(
                        "Cleanup identity ${identityFile.absolutePath} recorded CockroachDB PID " +
                            "$pid but processGroupId=$processGroupId; refusing to signal an " +
                            "unverified process group.",
                    )
                }
                identities.putIfAbsent(
                    Triple(pid, startedAtMillis, cockroach),
                    ScenarioProcessIdentity(
                        pid = pid,
                        startedAt = Instant.ofEpochMilli(startedAtMillis),
                        processGroupId = processGroupId,
                        source = identityFile,
                    ),
                )
            } catch (failure: Throwable) {
                failures += failure
            }
        }

    identities.values.forEach { identity ->
        try {
            val handle = identity.liveHandle() ?: return@forEach
            if (identity.processGroupId == null) {
                handle.destroyForcibly()
            } else {
                val signal = ProcessBuilder(
                    "/bin/kill",
                    "-KILL",
                    "--",
                    "-${identity.processGroupId}",
                ).start()
                val exitCode = signal.waitFor()
                check(exitCode == 0 || !handle.isAlive) {
                    "Could not signal cleanup process group ${identity.processGroupId} from " +
                        "${identity.source.absolutePath}; /bin/kill exited with code $exitCode " +
                        "and PID ${identity.pid} remained alive."
                }
            }
            if (handle.isAlive) {
                handle.onExit().get(SHARED_COCKROACH_PROCESS_STOP_SECONDS, TimeUnit.SECONDS)
            }
            check(identity.liveHandle() == null) {
                "Cleanup identity ${identity.source.absolutePath} remained alive as PID " +
                    "${identity.pid} started at ${identity.startedAt}."
            }
        } catch (failure: Throwable) {
            failures += if (failure is TimeoutException) {
                IllegalStateException(
                    "Cleanup identity ${identity.source.absolutePath} remained alive after " +
                        "$SHARED_COCKROACH_PROCESS_STOP_SECONDS seconds.",
                    failure,
                )
            } else {
                failure
            }
        }
    }

    identities.values.forEach { identity ->
        try {
            check(identity.liveHandle() == null) {
                "Cleanup identity ${identity.source.absolutePath} is still alive as PID " +
                    "${identity.pid} started at ${identity.startedAt}; preserving evidence under " +
                    "${root.absolutePath}."
            }
        } catch (failure: Throwable) {
            failures += failure
        }
    }

    if (failures.isEmpty()) {
        val targets = if (deleteRoot) {
            listOf(root)
        } else {
            root.walkTopDown()
                .filter {
                    it.isDirectory &&
                        it.name.startsWith("simplefilesystem-durable-shared-cockroach-v2-")
                }
                .toList()
        }
        targets.forEach { target ->
            if (target.exists() && !target.deleteRecursively()) {
                failures += IllegalStateException(
                    "Could not delete cleaned shared CockroachDB scenario directory " +
                        target.absolutePath,
                )
            }
        }
    }

    if (failures.isNotEmpty()) {
        val aggregate = IllegalStateException(
            "Shared CockroachDB scenario cleanup failed ${failures.size} time(s); process evidence " +
                "was preserved under ${root.absolutePath}.",
        )
        failures.forEach(aggregate::addSuppressed)
        throw aggregate
    }
}

private fun isScenarioIdentityRecord(file: File): Boolean =
    file.isFile &&
        (
            file.name == "daemon.properties" ||
                file.name == "cockroach.properties" ||
                (file.name.startsWith("daemon-") && "-arrived-" !in file.name) ||
                (file.name.startsWith("cockroach-") && "-arrived-" !in file.name)
            ) &&
        file.name.endsWith(".properties")

private data class ScenarioProcessIdentity(
    val pid: Long,
    val startedAt: Instant,
    val processGroupId: Long?,
    val source: File,
) {
    fun liveHandle(): ProcessHandle? {
        val handle = ProcessHandle.of(pid).orElse(null) ?: return null
        if (!handle.isAlive || handle.info().startInstant().orElse(null) != startedAt) return null
        val procStat = File("/proc/$pid/stat")
        if (procStat.isFile) {
            val stat = procStat.readText()
            val commandEnd = stat.lastIndexOf(") ")
            check(commandEnd >= 0 && commandEnd + 2 < stat.length) {
                "Linux process state ${procStat.absolutePath} had unrecognised value '$stat'."
            }
            if (stat[commandEnd + 2] == 'Z') return null
        }
        return handle
    }
}
