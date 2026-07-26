package simplefilesystem.durable.testing

import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds
import java.nio.file.attribute.BasicFileAttributes
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

private const val SCENARIO_WAIT_SECONDS = 20L
private const val PROCESS_EXIT_SECONDS = 10L

fun main(args: Array<String>) {
    require(args.size == 2) {
        "SharedCockroachProtocolScenarioMain requires <scenario> <root>, but received " +
            "${args.size} argument(s)."
    }
    val scenario = args[0]
    val root = File(args[1]).canonicalFile
    check(root.isDirectory) {
        "Shared CockroachDB protocol scenario root must be a directory, but was " +
            root.absolutePath
    }
    ScenarioHarness(root).use { harness ->
        when (scenario) {
            "pre-attach-owner-crash" -> harness.preAttachOwnerCrash()
            "pre-readiness-daemon-crash" -> harness.preReadinessDaemonCrash()
            "deterministic-contention" -> harness.deterministicContention()
            "last-release-acquire-race" -> harness.lastReleaseAcquireRace()
            "warmup-publication" -> harness.warmupPublication()
            "workspace-isolation" -> harness.workspaceIsolation()
            "lease-and-pid-reuse-recovery" -> harness.leaseAndPidReuseRecovery()
            else -> throw IllegalArgumentException(
                "Unknown shared CockroachDB protocol scenario '$scenario'.",
            )
        }
    }
    println("SCENARIO PASSED: $scenario")
}

private class ScenarioHarness(
    private val root: File,
) : AutoCloseable {
    private val fixtureJar = File(
        SharedCockroachProtocolV2Scenarios::class.java.protectionDomain.codeSource.location.toURI(),
    )
    private val javaBinary = File(System.getProperty("java.home"), "bin/java")
    private val control = File(root, "control")
    private val runFiles = File(root, "runs")
    private val defaultWorkspace = File(root, "workspace")
    private val probes = mutableListOf<Probe>()
    private val unrelatedProcesses = mutableListOf<Process>()
    private var scenarioFailure: Throwable? = null

    init {
        check(fixtureJar.isFile) {
            "Shared CockroachDB protocol scenario runtime must be a jar, but was " +
                fixtureJar.absolutePath
        }
        listOf(control, runFiles, defaultWorkspace).forEach { directory ->
            check(directory.mkdirs() || directory.isDirectory) {
                "Could not create shared CockroachDB scenario directory ${directory.absolutePath}."
            }
        }
    }

    fun preAttachOwnerCrash() = protect {
        marker("pause-daemon-before-attach")
        val winner = startProbe("winner")
        waitForPrefix(control, "daemon-before-attach-arrived-")
        val stateDirectory = onlyStateDirectory()
        val claim = properties(File(stateDirectory, "node-owner.properties"))
        val token = required(claim, "token", File(stateDirectory, "node-owner.properties"))
        val daemon = observation("daemon", token)
        check(requiredLong(claim, "ownerPid", File(stateDirectory, "node-owner.properties")) == winner.pid) {
            "Pre-spawn owner claim token '$token' did not belong to winner PID ${winner.pid}."
        }
        check(claim.getProperty("daemonPid") == null) {
            "Pre-attach owner claim token '$token' unexpectedly attached daemonPid=" +
                "${claim.getProperty("daemonPid")}."
        }
        check(File(required(claim, "workDirectory", File(stateDirectory, "node-owner.properties"))).isDirectory) {
            "Pre-spawn owner claim token '$token' did not publish its managed work directory."
        }

        winner.kill()
        removeMarker("pause-daemon-before-attach")
        val replacement = startProbe("replacement")
        val ready = replacement.awaitReady()
        assertUsable(required(ready, "jdbcUrl", replacement.readyFile))
        check(required(ready, "token", replacement.readyFile) != token) {
            "Replacement adopted dead election token '$token' instead of electing a new owner."
        }
        assertDead(daemon, "pre-attach daemon")
        check(observations("daemon").size == 2) {
            "Expected exactly the abandoned and replacement daemons, but observed " +
                observations("daemon").map { it.pid }
        }
        check(observations("cockroach").size == 1) {
            "A pre-attach daemon must not spawn CockroachDB; observed CockroachDB identities " +
                observations("cockroach").map { it.pid }
        }
        replacement.releaseAndAwait()
        assertAllObservedDead()
    }

    fun preReadinessDaemonCrash() = protect {
        marker("pause-cockroach-before-readiness")
        marker("pause-waiter-after-readiness-check")
        val winner = startProbe("winner")
        val arrival = waitForPrefix(control, "cockroach-before-readiness-arrived-")
        val token = required(properties(arrival), "token", arrival)
        val firstDaemon = observation("daemon", token)
        val firstCockroach = observation("cockroach", token)
        val waiterArrival = waitForPrefix(
            control,
            "waiter-after-readiness-check-arrived-",
        )
        assertNoReadyState(onlyStateDirectory(), "CockroachDB-before-readiness barrier")

        killIdentity(firstDaemon)
        releaseStateLockBarrier(waiterArrival)
        removeMarker("pause-waiter-after-readiness-check")
        removeMarker("pause-cockroach-before-readiness")
        val replacement = startProbe("replacement")
        val replacementReady = replacement.awaitReady()
        val recoveredWinnerReady = winner.awaitReady()
        val replacementToken = required(replacementReady, "token", replacement.readyFile)
        check(replacementToken != token) {
            "A contender retained dead daemon token '$token' instead of electing a replacement."
        }
        check(required(recoveredWinnerReady, "token", winner.readyFile) == replacementToken) {
            "The original waiter and new contender did not rendezvous on one replacement token."
        }
        assertUsable(required(replacementReady, "jdbcUrl", replacement.readyFile))
        assertUsable(required(recoveredWinnerReady, "jdbcUrl", winner.readyFile))
        assertDead(firstDaemon, "killed pre-readiness daemon")
        assertDead(firstCockroach, "CockroachDB child of killed pre-readiness daemon")
        check(observations("daemon").size == 2 && observations("cockroach").size == 2) {
            "Expected one abandoned and one replacement process tree, but observed daemons=" +
                "${observations("daemon").map { it.pid }}, CockroachDB=" +
                observations("cockroach").map { it.pid }
        }
        winner.releaseAndAwait()
        replacement.releaseAndAwait()
        assertAllObservedDead()
    }

    fun deterministicContention() = protect {
        val gate = File(runFiles, "contention-start-gate")
        val contenders = (0 until 4).map { index ->
            startProbe("contender-$index", startGate = gate)
        }
        contenders.forEach { contender -> waitForFile(contender.armedFile) }
        marker(gate)
        val ready = contenders.map(Probe::awaitReady)
        val tokens = ready.mapIndexed { index, value ->
            required(value, "token", contenders[index].readyFile)
        }.toSet()
        check(tokens.size == 1) {
            "Simultaneously released contenders elected ${tokens.size} tokens: $tokens."
        }
        check(observations("daemon").size == 1) {
            "Simultaneous contention spawned ${observations("daemon").size} daemons: " +
                observations("daemon").map { it.pid }
        }
        check(observations("cockroach").size == 1) {
            "Simultaneous contention spawned ${observations("cockroach").size} CockroachDB " +
                "processes: ${observations("cockroach").map { it.pid }}."
        }
        val urls = ready.mapIndexed { index, value ->
            required(value, "jdbcUrl", contenders[index].readyFile)
        }
        check(urls.distinct().size == contenders.size) {
            "Four contenders received non-distinct logical database URLs: $urls."
        }
        urls.forEach(::assertUsable)
        contenders.forEach(Probe::releaseAndAwait)
        assertAllObservedDead()
    }

    fun lastReleaseAcquireRace() = protect {
        val first = startProbe("acquire-wins-a")
        val firstReady = first.awaitReady()
        val firstToken = required(firstReady, "token", first.readyFile)
        val lockFile = File(required(firstReady, "lockFile", first.readyFile))
        val lockIdentity = fileIdentity(lockFile)

        marker("pause-acquire-after-lease")
        val acquiring = startProbe("acquire-wins-b")
        val acquisitionBarrier = File(control, "acquire-after-lease-arrived-${acquiring.pid}.properties")
        waitForFile(acquisitionBarrier)
        first.signalRelease()
        marker(File(control, "acquire-after-lease-release-${acquiring.pid}"))
        val acquiringReady = acquiring.awaitReady()
        check(required(acquiringReady, "token", acquiring.readyFile) == firstToken) {
            "An acquisition whose lease committed first did not preserve token '$firstToken'."
        }
        assertUsable(required(acquiringReady, "jdbcUrl", acquiring.readyFile))
        first.awaitExit()
        assertFileIdentity(lockFile, lockIdentity)
        removeMarker("pause-acquire-after-lease")
        acquiring.releaseAndAwait()
        assertFileIdentity(lockFile, lockIdentity)

        val releasing = startProbe("release-wins-a")
        val releasingReady = releasing.awaitReady()
        val releasingToken = required(releasingReady, "token", releasing.readyFile)
        val secondLockFile = File(required(releasingReady, "lockFile", releasing.readyFile))
        val secondLockIdentity = fileIdentity(secondLockFile)
        marker("pause-release-before-count")
        releasing.signalRelease()
        val releaseBarrier = File(control, "release-before-count-arrived-${releasing.pid}.properties")
        waitForFile(releaseBarrier)
        val afterRelease = startProbe("release-wins-b")
        marker(File(control, "release-before-count-release-${releasing.pid}"))
        releasing.awaitExit()
        val afterReleaseReady = afterRelease.awaitReady()
        check(required(afterReleaseReady, "token", afterRelease.readyFile) != releasingToken) {
            "An acquirer that lost to final teardown incorrectly retained token '$releasingToken'."
        }
        assertUsable(required(afterReleaseReady, "jdbcUrl", afterRelease.readyFile))
        assertFileIdentity(secondLockFile, secondLockIdentity)
        removeMarker("pause-release-before-count")
        afterRelease.releaseAndAwait()
        assertFileIdentity(secondLockFile, secondLockIdentity)
        assertAllObservedDead()
    }

    fun warmupPublication() = protect {
        marker("pause-warmup")
        marker("pause-waiter-after-readiness-check")
        val blocked = startProbe("warmup-blocked")
        waitForPrefix(control, "warmup-arrived-")
        val stateDirectory = onlyStateDirectory()
        val waiterArrival = waitForPrefix(
            control,
            "waiter-after-readiness-check-arrived-",
        )
        assertNoReadyState(stateDirectory, "production warmup barrier")
        check(!blocked.readyFile.exists()) {
            "Probe readiness ${blocked.readyFile.absolutePath} appeared before production warmup."
        }
        releaseStateLockBarrier(waiterArrival)
        removeMarker("pause-waiter-after-readiness-check")
        val warmupArrival = waitForPrefix(control, "warmup-arrived-")
        val token = required(properties(warmupArrival), "token", warmupArrival)
        marker(File(control, "warmup-release-$token"))
        val ready = blocked.awaitReady()
        check(File(stateDirectory, "node.properties").isFile) {
            "Node readiness was not published after production warmup completed."
        }
        assertUsable(required(ready, "jdbcUrl", blocked.readyFile))
        blocked.releaseAndAwait()
        removeMarker("pause-warmup")

        marker("fail-warmup")
        val failing = startProbe("warmup-failure")
        val exitCode = failing.awaitExit()
        check(exitCode != 0) {
            "Forced production warmup failure unexpectedly exited successfully."
        }
        check(!failing.readyFile.exists()) {
            "Probe readiness ${failing.readyFile.absolutePath} appeared after forced warmup failure."
        }
        check("warmup was forced to fail" in failing.logFile.readText()) {
            "Forced warmup failure output did not name the failure control:\n" +
                failing.logFile.readText()
        }
        val failureState = onlyStateDirectory()
        check(!File(failureState, "node.properties").exists()) {
            "Node readiness remained after forced production warmup failure."
        }
        check(!File(failureState, "node-warmup.properties").exists()) {
            "Warmup proof remained after forced production warmup failure."
        }
        check(!File(failureState, "leases").exists()) {
            "Lease directory remained after forced production warmup failure."
        }
        check(failureState.listFiles().orEmpty().none {
            it.isDirectory && it.name.startsWith("node-")
        }) {
            "Managed node work directory remained after forced production warmup failure."
        }
        assertAllObservedDead()
    }

    fun workspaceIsolation() = protect {
        val workspaceA = File(root, "workspace-a").apply(::requireDirectory)
        val workspaceB = File(root, "workspace-b").apply(::requireDirectory)
        val first = startProbe("workspace-a", workspace = workspaceA)
        val second = startProbe("workspace-b", workspace = workspaceB)
        val firstReady = first.awaitReady()
        val secondReady = second.awaitReady()
        val firstState = File(required(firstReady, "stateDirectory", first.readyFile)).canonicalFile
        val secondState = File(required(secondReady, "stateDirectory", second.readyFile)).canonicalFile
        check(firstState != secondState) {
            "Independent workspaces shared state namespace ${firstState.absolutePath}."
        }
        check(required(firstReady, "token", first.readyFile) !=
            required(secondReady, "token", second.readyFile)
        ) {
            "Independent workspaces shared one election token."
        }
        assertUsable(required(firstReady, "jdbcUrl", first.readyFile))
        assertUsable(required(secondReady, "jdbcUrl", second.readyFile))

        val foreign = File(secondState, "leases/foreign-protocol")
        Properties().apply {
            setProperty("protocolVersion", "999")
            setProperty("sentinel", "must-remain")
        }.also { values ->
            foreign.outputStream().use { values.store(it, null) }
        }
        val contender = startProbe("foreign-version", workspace = workspaceB)
        val contenderExit = contender.awaitExit()
        check(contenderExit != 0 && !contender.readyFile.exists()) {
            "A claimant adopted or overwrote an unknown protocol-version record."
        }
        val foreignOutput = contender.logFile.readText()
        check("version '999'" in foreignOutput && "requires version '2'" in foreignOutput) {
            "Unknown-version failure did not name found version 999 and expected version 2:\n" +
                foreignOutput
        }
        check(properties(foreign).getProperty("sentinel") == "must-remain") {
            "Unknown protocol-version record ${foreign.absolutePath} was modified."
        }
        assertUsable(required(secondReady, "jdbcUrl", second.readyFile))
        check(foreign.delete()) {
            "Could not remove test-owned unknown-version record ${foreign.absolutePath}."
        }

        val firstDaemon = readyIdentity(firstReady, first.readyFile, "daemon")
        val secondDaemon = readyIdentity(secondReady, second.readyFile, "daemon")
        first.releaseAndAwait()
        assertDead(firstDaemon, "workspace A daemon")
        check(secondDaemon.liveHandle() != null) {
            "Tearing down workspace A stopped workspace B daemon ${secondDaemon.pid}."
        }
        assertUsable(required(secondReady, "jdbcUrl", second.readyFile))
        second.releaseAndAwait()
        assertDead(secondDaemon, "workspace B daemon")
        assertAllObservedDead()
    }

    fun leaseAndPidReuseRecovery() = protect {
        val first = startProbe("live-a")
        val second = startProbe("killed-b")
        val firstReady = first.awaitReady()
        second.awaitReady()
        val originalToken = required(firstReady, "token", first.readyFile)
        val originalCockroach = readyIdentity(firstReady, first.readyFile, "cockroach")
        val stateDirectory = File(required(firstReady, "stateDirectory", first.readyFile))
        second.kill()

        val unrelated = ProcessBuilder("/usr/bin/tail", "-f", "/dev/null")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
        unrelatedProcesses += unrelated
        val unrelatedIdentity = processIdentity(
            unrelated.toHandle(),
            "unrelated PID-reuse sentinel",
        )
        val deliberatelyWrongStart = unrelatedIdentity.startedAt.minusSeconds(60L)
        val fakeLease = File(stateDirectory, "leases/pid-reuse")
        writePropertiesAtomically(
            fakeLease,
            versionedProperties().apply {
                setProperty("pid", unrelatedIdentity.pid.toString())
                setProperty("startedAtMillis", deliberatelyWrongStart.toEpochMilli().toString())
            },
        )
        val fakeNodeDirectory = File(stateDirectory, "node-pid-reuse")
        requireDirectory(fakeNodeDirectory)
        writeIdentity(
            File(fakeNodeDirectory, "daemon.properties"),
            SharedProcessIdentity(unrelatedIdentity.pid, deliberatelyWrongStart),
            "pid-reuse-token",
        )
        writeIdentity(
            File(fakeNodeDirectory, "cockroach.properties"),
            SharedProcessIdentity(unrelatedIdentity.pid, deliberatelyWrongStart),
            "pid-reuse-token",
        )

        val third = startProbe("purger-c")
        val thirdReady = third.awaitReady()
        check(required(thirdReady, "token", third.readyFile) == originalToken) {
            "Stale-lease purge replaced live lease A's node token '$originalToken'."
        }
        check(readyIdentity(thirdReady, third.readyFile, "cockroach") == originalCockroach) {
            "Stale-lease purge replaced live lease A's CockroachDB identity."
        }
        check(!fakeLease.exists()) {
            "PID-reuse lease ${fakeLease.absolutePath} was adopted instead of purged."
        }
        check(unrelated.isAlive) {
            "PID-reuse purge killed unrelated PID ${unrelated.pid()} despite mismatched start time."
        }
        third.releaseAndAwait()
        assertUsable(required(firstReady, "jdbcUrl", first.readyFile))
        check(originalCockroach.liveHandle() != null) {
            "Releasing purger C stopped the node still leased by live process A."
        }
        first.releaseAndAwait()
        check(!fakeNodeDirectory.exists()) {
            "PID-reuse node identity directory ${fakeNodeDirectory.absolutePath} was not purged."
        }
        check(unrelated.isAlive) {
            "Final teardown killed unrelated PID ${unrelated.pid()} despite mismatched start time."
        }
        assertAllObservedDead()
    }

    private inline fun protect(block: () -> Unit) {
        try {
            block()
        } catch (failure: Throwable) {
            scenarioFailure = failure
            throw failure
        }
    }

    private fun startProbe(
        name: String,
        workspace: File = defaultWorkspace,
        startGate: File = File(runFiles, "$name-start-gate").also(::marker),
    ): Probe {
        requireDirectory(workspace)
        val ready = File(runFiles, "$name-ready.properties")
        val release = File(runFiles, "$name-release")
        val armed = File(runFiles, "$name-armed.properties")
        val log = File(runFiles, "$name.log")
        val process = ProcessBuilder(
            javaBinary.absolutePath,
            "-Djava.io.tmpdir=${root.absolutePath}",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
            ready.absolutePath,
            release.absolutePath,
            armed.absolutePath,
            startGate.absolutePath,
            control.absolutePath,
            "lease-only",
        )
            .directory(workspace)
            .redirectErrorStream(true)
            .redirectOutput(log)
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
            }
            .start()
        return Probe(name, process, ready, release, armed, log).also(probes::add)
    }

    private fun observations(type: String): List<ScenarioIdentity> =
        control.listFiles().orEmpty()
            .filter {
                it.isFile &&
                    it.name.startsWith("$type-") &&
                    "-arrived-" !in it.name &&
                    it.name.endsWith(".properties")
            }
            .map(::identity)
            .distinct()

    private fun observation(type: String, token: String): ScenarioIdentity =
        identity(File(control, "$type-$token.properties").also(::waitForFile))

    private fun identity(file: File): ScenarioIdentity {
        val values = properties(file)
        return ScenarioIdentity(
            requiredLong(values, "pid", file),
            Instant.ofEpochMilli(requiredLong(values, "startedAtMillis", file)),
            values.getProperty("processGroupId")?.let { value ->
                value.toLongOrNull() ?: throw IllegalStateException(
                    "Identity ${file.absolutePath} contained non-numeric processGroupId='$value'.",
                )
            },
        )
    }

    private fun readyIdentity(
        values: Properties,
        file: File,
        type: String,
    ): ScenarioIdentity = ScenarioIdentity(
        requiredLong(values, "${type}Pid", file),
        Instant.ofEpochMilli(requiredLong(values, "${type}StartedAtMillis", file)),
        if (type == "cockroach") {
            requiredLong(values, "cockroachProcessGroupId", file)
        } else {
            null
        },
    )

    private fun onlyStateDirectory(): File {
        val stateDirectories = root.listFiles().orEmpty()
            .filter {
                it.isDirectory &&
                    it.name.startsWith("simplefilesystem-durable-shared-cockroach-v2-")
            }
        check(stateDirectories.size == 1) {
            "Expected exactly one shared CockroachDB state directory under ${root.absolutePath}, " +
                "but found ${stateDirectories.map(File::getName)}."
        }
        return stateDirectories.single()
    }

    private fun assertNoReadyState(stateDirectory: File, boundary: String) {
        check(!File(stateDirectory, "node.properties").exists()) {
            "Node readiness ${File(stateDirectory, "node.properties").absolutePath} was published " +
                "while execution was held at $boundary."
        }
    }

    private fun assertUsable(jdbcUrl: String) {
        Class.forName("org.postgresql.Driver")
        DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT 1").use { rows ->
                    check(rows.next() && rows.getInt(1) == 1) {
                        "JDBC endpoint $jdbcUrl did not return SELECT 1."
                    }
                }
            }
        }
    }

    private fun assertAllObservedDead() {
        observations("daemon").forEach { assertDead(it, "observed fixture daemon") }
        observations("cockroach").forEach { assertDead(it, "observed CockroachDB process") }
    }

    private fun killIdentity(identity: ScenarioIdentity) {
        val handle = identity.liveHandle() ?: return
        if (identity.processGroupId == null) {
            handle.destroyForcibly()
        } else {
            check(identity.processGroupId == identity.pid) {
                "Refusing to kill process group ${identity.processGroupId} whose verified leader " +
                    "was PID ${identity.pid}."
            }
            val kill = ProcessBuilder("/bin/kill", "-KILL", "--", "-${identity.processGroupId}")
                .start()
            val exitCode = kill.waitFor()
            check(exitCode == 0 || !handle.isAlive) {
                "Could not kill process group ${identity.processGroupId}; /bin/kill exited " +
                    "$exitCode and PID ${identity.pid} remained alive."
            }
        }
        if (handle.isAlive) handle.onExit().get(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
        assertDead(identity, "forcibly stopped process")
    }

    private fun assertDead(identity: ScenarioIdentity, description: String) {
        check(identity.liveHandle() == null) {
            "$description ${identity.pid} started at ${identity.startedAt} remained alive."
        }
    }

    private fun marker(name: String) = marker(File(control, name))

    private fun removeMarker(name: String) {
        val file = File(control, name)
        if (file.exists()) {
            check(file.delete()) {
                "Could not remove scenario control marker ${file.absolutePath}."
            }
        }
    }

    private fun releaseStateLockBarrier(arrival: File) {
        val releaseName = arrival.name
            .removeSuffix(".properties")
            .replace("-arrived-", "-release-")
        marker(File(control, releaseName))
    }

    private fun marker(file: File): File {
        file.parentFile?.let(::requireDirectory)
        check(file.isFile || file.createNewFile()) {
            "Could not create scenario control marker ${file.absolutePath}."
        }
        return file
    }

    private fun waitForPrefix(directory: File, prefix: String): File {
        requireDirectory(directory)
        FileSystems.getDefault().newWatchService().use { watcher ->
            directory.toPath().register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
            )
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SCENARIO_WAIT_SECONDS)
            while (true) {
                directory.listFiles().orEmpty()
                    .firstOrNull {
                        it.isFile && it.name.startsWith(prefix) && it.name.endsWith(".properties")
                    }
                    ?.let { return it }
                val remaining = deadline - System.nanoTime()
                check(remaining > 0L) {
                    "No file beginning '$prefix' appeared in ${directory.absolutePath} within " +
                        "$SCENARIO_WAIT_SECONDS seconds."
                }
                val key = watcher.poll(remaining, TimeUnit.NANOSECONDS)
                    ?: throw IllegalStateException(
                        "No file beginning '$prefix' appeared in ${directory.absolutePath} within " +
                            "$SCENARIO_WAIT_SECONDS seconds.",
                    )
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${directory.absolutePath} for prefix '$prefix'."
                }
            }
        }
    }

    private fun waitForFile(file: File): File {
        file.parentFile?.let(::requireDirectory)
        FileSystems.getDefault().newWatchService().use { watcher ->
            file.parentFile.toPath().register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
            )
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SCENARIO_WAIT_SECONDS)
            while (!file.isFile) {
                val remaining = deadline - System.nanoTime()
                check(remaining > 0L) {
                    "File ${file.absolutePath} did not appear within $SCENARIO_WAIT_SECONDS seconds."
                }
                val key = watcher.poll(remaining, TimeUnit.NANOSECONDS)
                    ?: throw IllegalStateException(
                        "File ${file.absolutePath} did not appear within " +
                            "$SCENARIO_WAIT_SECONDS seconds.",
                    )
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${file.parentFile.absolutePath} for ${file.name}."
                }
            }
        }
        return file
    }

    private fun properties(file: File): Properties {
        waitForFile(file)
        return Properties().apply {
            file.inputStream().use(::load)
        }
    }

    private fun fileIdentity(file: File): Any {
        check(file.isFile) {
            "Shared CockroachDB lock file ${file.absolutePath} did not exist."
        }
        return requireNotNull(
            Files.readAttributes(file.toPath(), BasicFileAttributes::class.java).fileKey(),
        ) {
            "Filesystem did not expose an identity key for lock file ${file.absolutePath}."
        }
    }

    private fun assertFileIdentity(file: File, expected: Any) {
        val actual = fileIdentity(file)
        check(actual == expected) {
            "Shared CockroachDB lock file ${file.absolutePath} was replaced: expected filesystem " +
                "identity '$expected', but found '$actual'."
        }
    }

    override fun close() {
        val failures = mutableListOf<Throwable>()
        control.listFiles().orEmpty()
            .filter { it.isFile && it.name.startsWith("pause-") }
            .forEach { pause ->
                try {
                    if (!pause.delete()) {
                        failures += IllegalStateException(
                            "Could not remove cleanup barrier ${pause.absolutePath}.",
                        )
                    }
                } catch (failure: Throwable) {
                    failures += failure
                }
            }
        control.listFiles().orEmpty()
            .filter { it.isFile && "-arrived-" in it.name }
            .forEach { arrival ->
                try {
                    val releaseName = arrival.name
                        .removeSuffix(".properties")
                        .replace("-arrived-", "-release-")
                    marker(File(control, releaseName))
                } catch (failure: Throwable) {
                    failures += failure
                }
            }
        probes.forEach { probe ->
            try {
                probe.signalRelease()
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        probes.forEach { probe ->
            try {
                if (probe.process.isAlive &&
                    !probe.process.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
                ) {
                    probe.process.destroyForcibly()
                    check(probe.process.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
                        "Probe '${probe.name}' PID ${probe.pid} survived forcible cleanup."
                    }
                }
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        unrelatedProcesses.forEach { process ->
            try {
                if (process.isAlive) {
                    process.destroyForcibly()
                    check(process.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
                        "Unrelated PID-reuse sentinel ${process.pid()} survived cleanup."
                    }
                }
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        try {
            cleanupSharedCockroachProtocolV2ScenarioEvidence(root, deleteRoot = false)
        } catch (failure: Throwable) {
            failures += failure
        }
        if (failures.isNotEmpty()) {
            val cleanupFailure = IllegalStateException(
                "Shared CockroachDB scenario cleanup failed ${failures.size} time(s).",
            )
            failures.forEach(cleanupFailure::addSuppressed)
            scenarioFailure?.addSuppressed(cleanupFailure) ?: throw cleanupFailure
        }
    }

    private inner class Probe(
        val name: String,
        val process: Process,
        val readyFile: File,
        val releaseFile: File,
        val armedFile: File,
        val logFile: File,
    ) {
        val pid: Long get() = process.pid()

        fun awaitReady(): Properties {
            waitForFile(readyFile)
            check(process.isAlive) {
                "Probe '$name' PID $pid exited before its ready file was consumed; output:\n" +
                    logFile.takeIf(File::isFile)?.readText().orEmpty()
            }
            return properties(readyFile)
        }

        fun signalRelease() {
            marker(releaseFile)
        }

        fun releaseAndAwait() {
            signalRelease()
            check(awaitExit() == 0) {
                "Probe '$name' PID $pid failed during release; output:\n" +
                    logFile.takeIf(File::isFile)?.readText().orEmpty()
            }
        }

        fun awaitExit(): Int {
            check(process.waitFor(SCENARIO_WAIT_SECONDS, TimeUnit.SECONDS)) {
                "Probe '$name' PID $pid did not exit within $SCENARIO_WAIT_SECONDS seconds; " +
                    "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}"
            }
            return process.exitValue()
        }

        fun kill() {
            if (process.isAlive) process.destroyForcibly()
            check(process.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
                "Probe '$name' PID $pid survived forcible termination."
            }
        }
    }
}

private data class ScenarioIdentity(
    val pid: Long,
    val startedAt: Instant,
    val processGroupId: Long?,
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

private fun requireDirectory(directory: File) {
    check(directory.isDirectory || directory.mkdirs()) {
        "Could not create scenario directory ${directory.absolutePath}."
    }
}

private fun required(properties: Properties, key: String, file: File): String =
    properties.getProperty(key) ?: throw IllegalStateException(
        "Shared CockroachDB scenario record ${file.absolutePath} did not contain $key.",
    )

private fun requiredLong(properties: Properties, key: String, file: File): Long {
    val value = required(properties, key, file)
    return value.toLongOrNull() ?: throw IllegalStateException(
        "Shared CockroachDB scenario record ${file.absolutePath} contained non-numeric " +
            "$key='$value'.",
    )
}
