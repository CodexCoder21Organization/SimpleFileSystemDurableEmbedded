package simplefilesystem.durable.testing

import java.io.File
import java.io.Closeable
import java.io.RandomAccessFile
import java.nio.channels.OverlappingFileLockException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Entry point and marker for the protocol-v2 scenario executable in the fixture jar. */
object SharedCockroachProtocolV2Scenarios {
    const val HOST_ADMISSION_HELD_ENV =
        "SIMPLE_FILESYSTEM_DURABLE_HOST_COCKROACH_ADMISSION_HELD"

    fun run(scenario: String) {
        if (scenario == "atomic-publication-required") {
            runSharedCockroachProtocolV2ScenarioTest(scenario)
        } else {
            withHostCockroachTestAdmission {
                runSharedCockroachProtocolV2ScenarioTest(scenario)
            }
        }
    }

    fun <T> withHostAdmission(block: () -> T): T =
        withHostCockroachTestAdmission(block)

    fun acquireHostAdmission(): Closeable =
        HostCockroachTestAdmission.acquire()
}

/**
 * BuildTest starts four child JVMs on each two-CPU shard. Two host slots admit at most one private
 * fault-injection process tree per core while retaining each admitted test's internal
 * thread/process concurrency. The filesystem-only atomic-publication scenario does not acquire a
 * slot. Managed tests already share one bounded CockroachDB node and remain concurrently
 * admissible.
 */
private object HostCockroachTestAdmission {
    private const val SLOT_COUNT = 2
    private val monitor = Any()
    private var referenceCount = 0
    private var access: RandomAccessFile? = null
    private var admissionLock: java.nio.channels.FileLock? = null

    fun acquire(): Closeable {
        synchronized(monitor) {
            if (referenceCount == 0) {
                val immediatelyAvailable = (0 until SLOT_COUNT).firstNotNullOfOrNull { slot ->
                    tryAcquireSlot(slot)
                }
                val acquired = immediatelyAvailable ?: acquireNextAvailableSlot()
                access = acquired.first
                admissionLock = acquired.second
            }
            referenceCount += 1
        }
        return HostCockroachTestAdmissionLease()
    }

    private fun tryAcquireSlot(
        slot: Int,
    ): Pair<RandomAccessFile, java.nio.channels.FileLock>? {
        val openedAccess = openSlot(slot)
        return try {
            val lock = try {
                openedAccess.channel.tryLock()
            } catch (_: OverlappingFileLockException) {
                null
            }
            if (lock == null) {
                openedAccess.close()
                null
            } else {
                openedAccess to lock
            }
        } catch (failure: Throwable) {
            openedAccess.close()
            throw failure
        }
    }

    private fun acquireNextAvailableSlot(
    ): Pair<RandomAccessFile, java.nio.channels.FileLock> {
        val tempDirectory = File(System.getProperty("java.io.tmpdir"))
        val releaseSignal = File(
            tempDirectory,
            "simplefilesystem-durable-cockroach-test-admission.signal",
        )
        if (!releaseSignal.exists()) {
            releaseSignal.createNewFile()
        }
        FileSystems.getDefault().newWatchService().use { watcher ->
            tempDirectory.toPath().register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
            )
            while (true) {
                (0 until SLOT_COUNT).firstNotNullOfOrNull(::tryAcquireSlot)?.let {
                    return it
                }
                val key = watcher.take()
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${tempDirectory.absolutePath} for a shared " +
                        "CockroachDB host-admission slot."
                }
            }
        }
    }

    private fun openSlot(slot: Int): RandomAccessFile = RandomAccessFile(
        File(
            System.getProperty("java.io.tmpdir"),
            "simplefilesystem-durable-cockroach-test-admission-$slot.lock",
        ),
        "rw",
    )

    fun release() {
        synchronized(monitor) {
            check(referenceCount > 0) {
                "The host CockroachDB test admission lease was released without an acquisition."
            }
            referenceCount -= 1
            if (referenceCount != 0) return

            var failure: Throwable? = null
            try {
                admissionLock?.release()
            } catch (caught: Throwable) {
                failure = caught
            } finally {
                admissionLock = null
                try {
                    access?.close()
                } catch (caught: Throwable) {
                    failure?.addSuppressed(caught) ?: run { failure = caught }
                } finally {
                    access = null
                }
            }
            try {
                File(
                    System.getProperty("java.io.tmpdir"),
                    "simplefilesystem-durable-cockroach-test-admission.signal",
                ).setLastModified(System.currentTimeMillis())
            } catch (caught: Throwable) {
                failure?.addSuppressed(caught) ?: run { failure = caught }
            }
            failure?.let { throw it }
        }
    }
}

private class HostCockroachTestAdmissionLease : Closeable {
    private var closed = false

    override fun close() {
        synchronized(this) {
            if (closed) return
            closed = true
        }
        HostCockroachTestAdmission.release()
    }
}

private fun <T> withHostCockroachTestAdmission(block: () -> T): T =
    HostCockroachTestAdmission.acquire().use { block() }

/**
 * Runs one end-to-end shared-CockroachDB protocol scenario with a private process tree.
 *
 * Scenario contenders, the detached daemon, and CockroachDB still run as real child processes
 * without the suite-fixture JDBC override. Running the orchestration harness in the test JVM avoids
 * spending a second, narrower process deadline around Kompile's own unchanged test deadline.
 */
private fun runSharedCockroachProtocolV2ScenarioTest(scenario: String) {
    var root: File? = null
    var failure: Throwable? = null
    try {
        root = Files.createTempDirectory("shared-cockroach-$scenario-").toFile()
        runSharedCockroachProtocolScenario(scenario, root)
    } catch (caught: Throwable) {
        failure = caught
        throw caught
    } finally {
        val cleanupFailures = mutableListOf<Throwable>()
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

    identities.values.filter { it.processGroupId == null }.forEach { identity ->
        try {
            val handle = identity.liveHandle() ?: return@forEach
            handle.destroyForcibly()
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

    identities.values.filter { it.processGroupId != null }
        .groupBy { requireNotNull(it.processGroupId) }
        .forEach { (processGroupId, members) ->
            try {
                val leader = members.singleOrNull { it.pid == processGroupId }
                    ?: throw IllegalStateException(
                        "Cleanup process group $processGroupId did not have exactly one recorded " +
                            "leader identity; sources=${members.map { it.source.absolutePath }}.",
                    )
                val liveMembers = members.mapNotNull { member ->
                    member.liveHandle()?.let { member to it }
                }
                if (liveMembers.isNotEmpty()) {
                    val signal = ProcessBuilder(
                        "/bin/kill",
                        "-KILL",
                        "--",
                        "-$processGroupId",
                    ).start()
                    val exitCode = signal.waitFor()
                    check(exitCode == 0 || liveMembers.none { it.second.isAlive }) {
                        "Could not signal cleanup process group $processGroupId from " +
                            "${leader.source.absolutePath}; /bin/kill exited with code $exitCode " +
                            "and recorded members remained alive."
                    }
                    liveMembers.forEach { (member, handle) ->
                        if (handle.isAlive) {
                            handle.onExit().get(
                                SHARED_COCKROACH_PROCESS_STOP_SECONDS,
                                TimeUnit.SECONDS,
                            )
                        }
                        check(member.liveHandle() == null) {
                            "Cleanup process-group $processGroupId member PID ${member.pid} from " +
                                "${member.source.absolutePath} remained alive."
                        }
                    }
                }
            } catch (failure: Throwable) {
                failures += if (failure is TimeoutException) {
                    IllegalStateException(
                        "Cleanup process group $processGroupId retained a recorded member after " +
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
                        it.name.startsWith(
                            "simplefilesystem-durable-shared-cockroach-" +
                                "v$SHARED_COCKROACH_PROTOCOL_VERSION-",
                        )
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
                file.name.startsWith(
                    "cockroach-after-start-before-identity-arrived-",
                ) ||
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
