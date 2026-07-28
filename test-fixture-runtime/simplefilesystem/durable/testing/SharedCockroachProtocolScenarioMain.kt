package simplefilesystem.durable.testing

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit

private const val SCENARIO_WAIT_SECONDS = 20L
private const val PROCESS_EXIT_SECONDS = 10L
private val scenarioProcessLocalStateLock = Any()

fun main(args: Array<String>) {
    require(args.size == 2) {
        "SharedCockroachProtocolScenarioMain requires <scenario> <root>, but received " +
            "${args.size} argument(s)."
    }
    val scenario = args[0]
    val root = File(args[1]).canonicalFile
    runSharedCockroachProtocolScenario(scenario, root)
    println("SCENARIO PASSED: $scenario")
}

internal fun runSharedCockroachProtocolScenario(scenario: String, root: File) {
    check(root.isDirectory) {
        "Shared CockroachDB protocol scenario root must be a directory, but was " +
            root.absolutePath
    }
    ScenarioHarness(root).use { harness ->
        when (scenario) {
            "pre-attach-owner-crash" -> harness.preAttachOwnerCrash()
            "post-spawn-identity-crash" -> harness.postSpawnIdentityCrash()
            "pre-readiness-daemon-crash" -> harness.preReadinessDaemonCrash()
            "expired-attached-startup" -> harness.expiredAttachedStartup()
            "missing-local-process-evidence" -> harness.missingLocalProcessEvidence()
            "atomic-publication-required" -> harness.atomicPublicationRequired()
            "launcher-rendezvous" -> harness.launcherRendezvous()
            "deterministic-contention" -> harness.deterministicContention()
            "managed-concurrent-admission" -> harness.managedConcurrentAdmission()
            "last-release-acquire-race" -> harness.lastReleaseAcquireRace()
            "warmup-publication" -> harness.warmupPublication()
            "startup-failure-publication-stress" -> harness.startupFailurePublicationStress()
            "workspace-isolation" -> harness.workspaceIsolation()
            "prestart-session-owner-exit" -> harness.prestartSessionOwnerExit()
            "build-only-cache-invalidation" -> harness.buildOnlyCacheInvalidation()
            "lease-and-pid-reuse-recovery" -> harness.leaseAndPidReuseRecovery()
            "malformed-record-diagnostics" -> harness.malformedRecordDiagnostics()
            else -> throw IllegalArgumentException(
                "Unknown shared CockroachDB protocol scenario '$scenario'.",
            )
        }
    }
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

        killProcessOnly(firstDaemon)
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

    fun postSpawnIdentityCrash() = protect {
        marker("pause-cockroach-after-start-before-identity")
        val winner = startProbe("winner")
        val arrival = waitForPrefix(
            control,
            "cockroach-after-start-before-identity-arrived-",
        )
        val firstCockroach = identity(arrival)
        val token = required(properties(arrival), "token", arrival)
        val firstDaemon = observation("daemon", token)
        val originalStateDirectory = onlyStateDirectory()
        val originalOwnerFile = File(originalStateDirectory, "node-owner.properties")
        val originalWorkDirectory = File(
            required(properties(originalOwnerFile), "workDirectory", originalOwnerFile),
        )
        check(firstCockroach.processGroupId == firstDaemon.pid) {
            "CockroachDB child ${firstCockroach.pid} used process group " +
                "${firstCockroach.processGroupId}, but durable daemon ${firstDaemon.pid} must " +
                "lead every subsequently spawned child."
        }
        assertNoReadyState(originalStateDirectory, "post-spawn/pre-identity barrier")

        killProcessOnly(firstDaemon)
        removeMarker("pause-cockroach-after-start-before-identity")
        val replacement = startProbe("replacement")
        val replacementReady = replacement.awaitReady()
        val recoveredWinnerReady = winner.awaitReady()
        check(required(replacementReady, "token", replacement.readyFile) != token) {
            "Replacement retained dead daemon token '$token'."
        }
        check(
            required(recoveredWinnerReady, "token", winner.readyFile) ==
                required(replacementReady, "token", replacement.readyFile),
        ) {
            "Original waiter and replacement did not rendezvous on the replacement owner."
        }
        firstCockroach.liveHandle()?.let {
            val actualGroup = processGroupId(
                SharedProcessIdentity(firstCockroach.pid, firstCockroach.startedAt),
                "unpublished CockroachDB child",
            )
            throw IllegalStateException(
                "Unpublished CockroachDB child ${firstCockroach.pid} remained alive in process " +
                    "group $actualGroup after replacement; originally recorded group was " +
                    "${firstCockroach.processGroupId}, durable daemon was ${firstDaemon.pid}, and " +
                    "reconciliation deletedWorkDirectory=${!originalWorkDirectory.exists()}.",
            )
        }
        assertUsable(required(replacementReady, "jdbcUrl", replacement.readyFile))
        winner.releaseAndAwait()
        replacement.releaseAndAwait()
        assertAllObservedDead()
    }

    fun expiredAttachedStartup() = protect {
        marker("pause-cockroach-before-readiness")
        val probe = startProbe("expired-startup")
        val arrival = waitForPrefix(control, "cockroach-before-readiness-arrived-")
        val token = required(properties(arrival), "token", arrival)
        val expiredCockroach = observation("cockroach", token)
        val stateDirectory = onlyStateDirectory()
        val ownerFile = File(stateDirectory, "node-owner.properties")
        val owner = properties(ownerFile)
        val expiredDeadline = System.currentTimeMillis() - 1L
        owner.setProperty("attachDeadlineMillis", expiredDeadline.toString())
        if (owner.containsKey("startupDeadlineMillis")) {
            owner.setProperty("startupDeadlineMillis", expiredDeadline.toString())
        }
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(ownerFile, owner)
        }
        marker(File(control, "cockroach-before-readiness-release-$token"))
        removeMarker("pause-cockroach-before-readiness")

        val ready = probe.awaitReady()
        val replacementToken = required(ready, "token", probe.readyFile)
        check(replacementToken != token) {
            "Shared CockroachDB token '$token' published readiness after its absolute startup " +
                "deadline $expiredDeadline had elapsed instead of yielding to a replacement."
        }
        assertDead(expiredCockroach, "CockroachDB child of expired startup token")
        assertUsable(required(ready, "jdbcUrl", probe.readyFile))
        probe.releaseAndAwait()
        assertAllObservedDead()
    }

    fun missingLocalProcessEvidence() = protect {
        val original = startProbe("original")
        val originalReady = original.awaitReady()
        val token = required(originalReady, "token", original.readyFile)
        val originalDaemon = readyIdentity(originalReady, original.readyFile, "daemon")
        val originalCockroach = readyIdentity(originalReady, original.readyFile, "cockroach")
        val stateDirectory = File(
            required(originalReady, "stateDirectory", original.readyFile),
        )
        val nodeFile = File(stateDirectory, "node.properties")
        check(properties(nodeFile).getProperty("token") == token) {
            "Ready node evidence ${nodeFile.absolutePath} did not retain token '$token'."
        }
        val workDirectory = File(
            required(properties(nodeFile), "workDirectory", nodeFile),
        )

        killProcessOnly(originalDaemon)
        val originalCockroachEvidence = File(workDirectory, "cockroach.properties")
        check(originalCockroachEvidence.delete()) {
            "Could not remove test-owned local process evidence " +
                "${originalCockroachEvidence.absolutePath}."
        }
        check(nodeFile.isFile) {
            "Global node evidence ${nodeFile.absolutePath} disappeared before reconciliation."
        }

        val replacement = startProbe("replacement")
        val replacementReady = replacement.awaitReady()
        val replacementToken = required(replacementReady, "token", replacement.readyFile)
        check(replacementToken != token) {
            "Replacement retained dead daemon token '$token'."
        }
        assertDead(originalDaemon, "killed daemon with missing local process evidence")
        assertDead(
            originalCockroach,
            "CockroachDB process with missing cockroach.properties",
        )
        assertUsable(required(replacementReady, "jdbcUrl", replacement.readyFile))

        val replacementDaemon = readyIdentity(replacementReady, replacement.readyFile, "daemon")
        val replacementCockroach = readyIdentity(
            replacementReady,
            replacement.readyFile,
            "cockroach",
        )
        val replacementNodeFile = File(
            required(replacementReady, "stateDirectory", replacement.readyFile),
            "node.properties",
        )
        val replacementWorkDirectory = File(
            required(
                properties(replacementNodeFile),
                "workDirectory",
                replacementNodeFile,
            ),
        )
        killProcessOnly(replacementDaemon)
        listOf("cockroach.properties", "daemon.properties").forEach { name ->
            val evidence = File(replacementWorkDirectory, name)
            check(evidence.delete()) {
                "Could not remove test-owned local process evidence ${evidence.absolutePath}."
            }
        }
        val third = startProbe("global-evidence-only")
        val thirdReady = third.awaitReady()
        check(required(thirdReady, "token", third.readyFile) != replacementToken) {
            "Global-evidence-only recovery retained dead token '$replacementToken'."
        }
        assertDead(
            replacementCockroach,
            "CockroachDB process recoverable only from global node evidence",
        )
        assertUsable(required(thirdReady, "jdbcUrl", third.readyFile))

        original.releaseAndAwait()
        replacement.releaseAndAwait()
        third.releaseAndAwait()
        assertAllObservedDead()
    }

    fun atomicPublicationRequired() = protect {
        val target = File(root, "atomic-publication.properties").toPath()
        val sharedMemoryRoot = Path.of("/dev/shm")
        check(Files.isDirectory(sharedMemoryRoot)) {
            "Atomic-publication scenario requires the real tmpfs directory " +
                "${sharedMemoryRoot.toAbsolutePath()}, but it was unavailable."
        }
        val stagingDirectory = Files.createTempDirectory(
            sharedMemoryRoot,
            "shared-cockroach-atomic-staging-",
        )
        var scenarioFailure: Throwable? = null
        try {
            check(Files.getFileStore(target.parent) != Files.getFileStore(stagingDirectory)) {
                "Atomic-publication scenario requires distinct real filesystems, but target " +
                    "${target.parent} and staging $stagingDirectory used the same file store."
            }
            val failure = try {
                writePropertiesAtomically(
                    target,
                    versionedProperties().apply {
                        setProperty("sentinel", "must-not-publish")
                    },
                    stagingDirectory,
                )
                null
            } catch (caught: IllegalStateException) {
                caught
            }
            check(failure != null) {
                "Atomic publication unexpectedly degraded to a non-atomic move from " +
                    "$stagingDirectory to ${target.toAbsolutePath()}."
            }
            val message = failure.message.orEmpty()
            check(target.toAbsolutePath().toString() in message && "ATOMIC_MOVE" in message) {
                "Atomic-publication failure did not name target ${target.toAbsolutePath()} and " +
                    "the ATOMIC_MOVE filesystem limitation: '$message'."
            }
            check(!Files.exists(target)) {
                "Atomic-publication failure left a visible record at ${target.toAbsolutePath()}."
            }
            check(Files.list(stagingDirectory).use { entries -> entries.findAny().isEmpty }) {
                "Atomic-publication failure left staging evidence under $stagingDirectory."
            }
        } catch (failure: Throwable) {
            scenarioFailure = failure
            throw failure
        } finally {
            val cleanupFailures = mutableListOf<Throwable>()
            try {
                if (Files.exists(target) && !Files.deleteIfExists(target)) {
                    cleanupFailures += IllegalStateException(
                        "Could not delete test-owned atomic publication target " +
                            target.toAbsolutePath(),
                    )
                }
            } catch (failure: Throwable) {
                cleanupFailures += failure
            }
            try {
                stagingDirectory.toFile().listFiles().orEmpty().forEach { staging ->
                    if (!staging.delete()) {
                        cleanupFailures += IllegalStateException(
                            "Could not delete test-owned atomic staging file " +
                                staging.absolutePath,
                        )
                    }
                }
                if (Files.exists(stagingDirectory) &&
                    !Files.deleteIfExists(stagingDirectory)
                ) {
                    cleanupFailures += IllegalStateException(
                        "Could not delete test-owned atomic staging directory $stagingDirectory.",
                    )
                }
            } catch (failure: Throwable) {
                cleanupFailures += failure
            }
            if (cleanupFailures.isNotEmpty()) {
                val cleanupFailure = IllegalStateException(
                    "Atomic-publication scenario cleanup failed ${cleanupFailures.size} time(s).",
                )
                cleanupFailures.forEach(cleanupFailure::addSuppressed)
                scenarioFailure?.addSuppressed(cleanupFailure) ?: throw cleanupFailure
            }
        }
    }

    fun launcherRendezvous() = protect {
        val first = startProbe("launcher-a", workspace = null)
        val second = startProbe("launcher-b", workspace = null)
        val firstReady = first.awaitReady()
        val secondReady = second.awaitReady()
        val scenarioUserDirectory = File(System.getProperty("user.dir")).canonicalPath
        val observedUserDirectories = setOf(
            required(firstReady, "userDirectory", first.readyFile),
            required(secondReady, "userDirectory", second.readyFile),
        )
        check(observedUserDirectories == setOf(scenarioUserDirectory)) {
            "Child JVMs launched without an explicit working directory observed " +
                "$observedUserDirectories, but their real launcher parent used " +
                "'$scenarioUserDirectory'."
        }
        check(
            required(firstReady, "stateDirectory", first.readyFile) ==
                required(secondReady, "stateDirectory", second.readyFile),
        ) {
            "Child JVMs inheriting one launcher working directory fragmented shared state into " +
                "${required(firstReady, "stateDirectory", first.readyFile)} and " +
                "${required(secondReady, "stateDirectory", second.readyFile)}."
        }
        check(
            required(firstReady, "token", first.readyFile) ==
                required(secondReady, "token", second.readyFile),
        ) {
            "Child JVMs inheriting one launcher working directory elected distinct tokens."
        }
        check(observations("daemon").size == 1 && observations("cockroach").size == 1) {
            "Launcher rendezvous observed daemons=${observations("daemon").map { it.pid }} and " +
                "CockroachDB processes=${observations("cockroach").map { it.pid }}."
        }
        assertObservationFilenamesAreIdentityUnique()
        assertUsable(required(firstReady, "jdbcUrl", first.readyFile))
        first.releaseAndAwait()
        second.releaseAndAwait()
        assertAllObservedDead()
    }

    fun deterministicContention() = protect {
        val gate = File(runFiles, "contention-start-gate")
        val contenders = (0 until 4).map { index ->
            startProbe(
                "contender-$index",
                startGate = gate,
                databaseMode = "lease-only",
            ).also { contender ->
                waitForFile(contender.armedFile)
            }
        }
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
        assertCockroachCpuBudget(observations("cockroach").single())
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

    fun managedConcurrentAdmission() = protect {
        val gate = File(runFiles, "managed-admission-start-gate")
        val contenders = (0 until 4).map { index ->
            startProbe(
                name = "managed-admission-$index",
                startGate = gate,
                inheritHostAdmission = false,
                databaseMode = "lease-only",
            ).also { contender ->
                waitForFile(contender.armedFile)
            }
        }
        marker(gate)
        val ready = contenders.map(Probe::awaitReady)
        check(observations("daemon").size == 1 && observations("cockroach").size == 1) {
            "Concurrent managed fixture admission observed daemons=" +
                "${observations("daemon").map { it.pid }} and CockroachDB processes=" +
                "${observations("cockroach").map { it.pid }}."
        }
        val urls = ready.mapIndexed { index, value ->
            required(value, "jdbcUrl", contenders[index].readyFile)
        }
        check(urls.distinct().size == contenders.size) {
            "Four concurrently admitted managed fixtures received non-distinct logical " +
                "database URLs: $urls."
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
        val blocked = startProbe("warmup-blocked", databaseMode = "schema-ready")
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

        val failureWorkspace = File(root, "warmup-failure-workspace").apply(::requireDirectory)
        val failureControl = File(root, "warmup-failure-control").apply(::requireDirectory)
        marker(File(failureControl, "fail-warmup"))
        val failing = startProbe(
            name = "warmup-failure",
            workspace = failureWorkspace,
            controlDirectory = failureControl,
            databaseMode = "schema-ready",
        )

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
        val failureState = stateDirectories().singleOrNull { it != stateDirectory }
            ?: throw IllegalStateException(
                "Expected exactly one isolated failed-warmup state directory in addition to " +
                    "${stateDirectory.absolutePath}, but found " +
                    stateDirectories().map(File::getName),
            )
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

    fun startupFailurePublicationStress() = protect {
        val failureWorkspace = File(root, "failure-workspace").apply(::requireDirectory)
        val failureControl = File(root, "failure-control").apply(::requireDirectory)
        val publicationPause = File(
            failureControl,
            "pause-daemon-before-startup-failure-publication",
        )
        marker(File(failureControl, "fail-warmup"))
        marker(publicationPause)
        val failing = startProbe(
            name = "startup-failure",
            workspace = failureWorkspace,
            controlDirectory = failureControl,
            databaseMode = "schema-ready",
        )

        val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(SCENARIO_WAIT_SECONDS)
        var daemonObservations = emptyList<File>()
        while (failing.process.isAlive && System.nanoTime() < deadlineNanos) {
            daemonObservations = failureControl.listFiles().orEmpty()
                .filter {
                    it.isFile &&
                        it.name.startsWith("daemon-") &&
                        "-arrived-" !in it.name &&
                        it.name.endsWith(".properties")
                }
            if (daemonObservations.size > 1) break
            Thread.sleep(25L)
        }
        if (publicationPause.isFile && !publicationPause.delete()) {
            throw IllegalStateException(
                "Could not remove startup-failure publication barrier " +
                    "${publicationPause.absolutePath}.",
            )
        }
        check(daemonObservations.size <= 1) {
            failing.kill()
            "One forced warmup failure elected ${daemonObservations.size} daemon generations " +
                "${daemonObservations.map(File::getName)} before the failure became observable."
        }
        check(!failing.process.isAlive) {
            failing.kill()
            "Forced warmup failure probe ${failing.pid} did not publish its failure and exit " +
                "within $SCENARIO_WAIT_SECONDS seconds."
        }
        check(failing.process.exitValue() != 0) {
            "Forced production warmup failure unexpectedly exited successfully."
        }
        check(!failing.readyFile.exists()) {
            "Probe readiness ${failing.readyFile.absolutePath} appeared after forced warmup failure."
        }
        check("warmup was forced to fail" in failing.logFile.readText()) {
            "Forced warmup failure output did not name the failure control:\n" +
                failing.logFile.readText()
        }
        assertAllObservedDead()
    }

    fun workspaceIsolation() = protect {
        val legacyStateDirectory = File(
            root,
            "simplefilesystem-durable-shared-cockroach-v1",
        ).apply(::requireDirectory)
        val legacySentinel = File(legacyStateDirectory, "main-checkout-sentinel")
        val legacyContents = "owned by a concurrently running main checkout".toByteArray()
        legacySentinel.writeBytes(legacyContents)
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
        listOf(firstReady to firstState, secondReady to secondState).forEach { (ready, state) ->
            val protocolVersion = required(ready, "protocolVersion", state)
            check("-v$protocolVersion-" in state.name) {
                "Shared state directory ${state.absolutePath} did not derive its namespace suffix " +
                    "from protocolVersion='$protocolVersion'."
            }
            check(state.parentFile == root && state != legacyStateDirectory) {
                "Protocol-v2 state ${state.absolutePath} overlapped the legacy main-checkout " +
                    "namespace ${legacyStateDirectory.absolutePath}."
            }
        }
        check(legacySentinel.readBytes().contentEquals(legacyContents)) {
            "Protocol-v2 fixture activity modified legacy main-checkout evidence " +
                "${legacySentinel.absolutePath}."
        }
        check(required(firstReady, "token", first.readyFile) !=
            required(secondReady, "token", second.readyFile)
        ) {
            "Independent workspaces shared one election token."
        }
        assertUsable(required(firstReady, "jdbcUrl", first.readyFile))
        assertUsable(required(secondReady, "jdbcUrl", second.readyFile))

        val foreign = File(secondState, "leases/foreign-protocol")
        withScenarioStateLock(secondState) {
            Properties().apply {
                setProperty("protocolVersion", "999")
                setProperty("sentinel", "must-remain")
            }.also { values ->
                foreign.outputStream().use { values.store(it, null) }
            }
        }
        val contender = startProbe("foreign-version", workspace = workspaceB)
        val contenderReady = contender.awaitReady()
        check(
            required(contenderReady, "token", contender.readyFile) ==
                required(secondReady, "token", second.readyFile),
        ) {
            "An unknown-version lease stranded the known protocol owner instead of being " +
                "quarantined."
        }
        check(!foreign.exists()) {
            "Unknown protocol-version record ${foreign.absolutePath} remained a namespace poison " +
                "pill instead of moving to quarantine."
        }
        val quarantined = File(secondState, "quarantine")
            .walkTopDown()
            .filter { it.isFile && it.name.endsWith("-leases-foreign-protocol") }
            .toList()
        check(quarantined.size == 1) {
            "Expected one preserved unknown-version lease in ${File(secondState, "quarantine")}, " +
                "but found ${quarantined.map(File::getAbsolutePath)}."
        }
        val quarantinedProperties = Properties().apply {
            quarantined.single().inputStream().use(::load)
        }
        check(
            quarantinedProperties.getProperty("protocolVersion") == "999" &&
                quarantinedProperties.getProperty("sentinel") == "must-remain"
        ) {
            "Quarantined unknown-version record ${quarantined.single().absolutePath} was modified."
        }
        assertUsable(required(secondReady, "jdbcUrl", second.readyFile))
        contender.releaseAndAwait()

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
        check(legacySentinel.readBytes().contentEquals(legacyContents)) {
            "Protocol-v2 teardown modified legacy main-checkout evidence " +
                "${legacySentinel.absolutePath}."
        }
        assertAllObservedDead()
    }

    fun prestartSessionOwnerExit() = protect {
        val sessionOwner = ProcessBuilder("/bin/sleep", "120")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
            .also(unrelatedProcesses::add)
        val sessionIdentity = processIdentity(
            sessionOwner.toHandle(),
            "synthetic Kompile session owner",
        )
        val cacheIndex = File(runFiles, "synthetic-cache/buildRuleResultIndex")
        check(cacheIndex.mkdirs()) {
            "Could not create synthetic Kompile build-rule cache index ${cacheIndex.absolutePath}."
        }
        val fixtureCacheEntries = SHARED_COCKROACH_FIXTURE_BUILD_RULE_CACHE_KEYS.map { cacheKey ->
            File(cacheIndex, "$cacheKey.json").apply {
                writeText("stale fixture rule")
            }
        }
        val unrelatedCacheEntry = File(cacheIndex, "unrelated-rule.json").apply {
            writeText("must remain")
        }
        val prestartLog = File(runFiles, "prestart-session.log")
        val prestart = ProcessBuilder(
            javaBinary.absolutePath,
            *SHARED_COCKROACH_CHILD_JVM_ARGUMENTS.toTypedArray(),
            "-Djava.io.tmpdir=${root.absolutePath}",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachPrestartMainKt",
            sessionIdentity.pid.toString(),
            sessionIdentity.startedAt.toEpochMilli().toString(),
            "false",
            "0",
            *fixtureCacheEntries.map(File::getAbsolutePath).toTypedArray(),
        )
            .directory(defaultWorkspace)
            .redirectErrorStream(true)
            .redirectOutput(prestartLog)
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
                it.environment()[SharedCockroachProtocolV2Scenarios.HOST_ADMISSION_HELD_ENV] = "true"
            }
            .start()
        check(prestart.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
            prestart.destroyForcibly()
            "Session prestart helper PID ${prestart.pid()} did not finish within " +
                "$PROCESS_EXIT_SECONDS seconds; output:\n" +
                prestartLog.takeIf(File::isFile)?.readText().orEmpty()
        }
        check(prestart.exitValue() == 0) {
            "Session prestart helper PID ${prestart.pid()} exited with code " +
                "${prestart.exitValue()}; output:\n" +
                prestartLog.takeIf(File::isFile)?.readText().orEmpty()
        }

        val stateDirectory = onlyStateDirectory()
        val nodeFile = File(stateDirectory, "node.properties")
        val node = properties(nodeFile)
        val daemon = ScenarioIdentity(
            requiredLong(node, "daemonPid", nodeFile),
            Instant.ofEpochMilli(requiredLong(node, "daemonStartedAtMillis", nodeFile)),
            null,
        )
        val cockroach = ScenarioIdentity(
            requiredLong(node, "pid", nodeFile),
            Instant.ofEpochMilli(requiredLong(node, "processStartedAtMillis", nodeFile)),
            requiredLong(node, "processGroupId", nodeFile),
        )
        assertUsable(required(node, "jdbcUrl", nodeFile))
        val lease = File(
            stateDirectory,
            "leases/kompile-session-${sessionIdentity.pid}-" +
                sessionIdentity.startedAt.toEpochMilli(),
        )
        check(lease.isFile) {
            "Prestarted node did not publish its session-owned lease ${lease.absolutePath}."
        }
        val malformedLease = File(stateDirectory, "leases/malformed-session-sentinel")
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(
                malformedLease,
                versionedProperties().apply {
                    setProperty("pid", "not-a-session-pid")
                    setProperty("startedAtMillis", "0")
                },
            )
        }

        sessionOwner.destroyForcibly()
        check(sessionOwner.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
            "Synthetic Kompile session owner PID ${sessionOwner.pid()} survived forcible shutdown."
        }
        daemon.liveHandle()?.onExit()?.get(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
        cockroach.liveHandle()?.onExit()?.get(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
        assertDead(daemon, "prestarted daemon after its Kompile session exited")
        assertDead(cockroach, "prestarted CockroachDB after its Kompile session exited")
        check(!lease.exists()) {
            "Expired Kompile session lease ${lease.absolutePath} remained after fixture shutdown."
        }
        val remainingFixtureCacheEntries = fixtureCacheEntries.filter(File::exists)
        check(remainingFixtureCacheEntries.isEmpty()) {
            "Expired Kompile session left fixture build-rule cache entries " +
                "${remainingFixtureCacheEntries.map(File::getAbsolutePath)}, so a later direct " +
                "test dispatch could skip prestart."
        }
        check(unrelatedCacheEntry.readText() == "must remain") {
            "Fixture cleanup modified unrelated Kompile cache entry " +
                "${unrelatedCacheEntry.absolutePath}."
        }
        check(malformedLease.isFile) {
            "Fixture shutdown discarded malformed lease evidence " +
                "${malformedLease.absolutePath} instead of preserving it."
        }
    }

    fun buildOnlyCacheInvalidation() = protect {
        val sessionOwner = ProcessBuilder("/bin/sleep", "120")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.DISCARD)
            .start()
            .also(unrelatedProcesses::add)
        val sessionIdentity = processIdentity(
            sessionOwner.toHandle(),
            "synthetic BuildTestRunner build-only session owner",
        )
        val cacheIndex = File(runFiles, "synthetic-cache/buildRuleResultIndex")
        check(cacheIndex.mkdirs()) {
            "Could not create synthetic BuildTestRunner build-rule cache index " +
                "${cacheIndex.absolutePath}."
        }
        val fixtureCacheEntries = SHARED_COCKROACH_FIXTURE_BUILD_RULE_CACHE_KEYS.map { cacheKey ->
            File(cacheIndex, "$cacheKey.json")
        }
        val unrelatedCacheEntry = File(cacheIndex, "unrelated-rule.json").apply {
            writeText("must remain")
        }
        val prestartLog = File(runFiles, "build-only-prestart.log")
        val prestart = ProcessBuilder(
            javaBinary.absolutePath,
            *SHARED_COCKROACH_CHILD_JVM_ARGUMENTS.toTypedArray(),
            "-Djava.io.tmpdir=${root.absolutePath}",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachPrestartMainKt",
            sessionIdentity.pid.toString(),
            sessionIdentity.startedAt.toEpochMilli().toString(),
            "true",
            "0",
            *fixtureCacheEntries.map(File::getAbsolutePath).toTypedArray(),
        )
            .directory(defaultWorkspace)
            .redirectErrorStream(true)
            .redirectOutput(prestartLog)
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
                it.environment()[SharedCockroachProtocolV2Scenarios.HOST_ADMISSION_HELD_ENV] = "true"
            }
            .start()
        check(prestart.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
            prestart.destroyForcibly()
            "Build-only prestart helper PID ${prestart.pid()} did not finish within " +
                "$PROCESS_EXIT_SECONDS seconds; output:\n" +
                prestartLog.takeIf(File::isFile)?.readText().orEmpty()
        }
        check(prestart.exitValue() == 0) {
            "Build-only prestart helper PID ${prestart.pid()} exited with code " +
                "${prestart.exitValue()}; output:\n" +
                prestartLog.takeIf(File::isFile)?.readText().orEmpty()
        }

        val stateDirectory = onlyStateDirectory()
        val nodeFile = File(stateDirectory, "node.properties")
        val node = properties(nodeFile)
        val daemon = ScenarioIdentity(
            requiredLong(node, "daemonPid", nodeFile),
            Instant.ofEpochMilli(requiredLong(node, "daemonStartedAtMillis", nodeFile)),
            null,
        )
        val cockroach = ScenarioIdentity(
            requiredLong(node, "pid", nodeFile),
            Instant.ofEpochMilli(requiredLong(node, "processStartedAtMillis", nodeFile)),
            requiredLong(node, "processGroupId", nodeFile),
        )
        assertUsable(required(node, "jdbcUrl", nodeFile))
        fixtureCacheEntries.forEach { cacheEntry ->
            cacheEntry.writeText("would be archived")
        }

        val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L)
        while (fixtureCacheEntries.any(File::exists) && System.nanoTime() < deadlineNanos) {
            check(sessionIdentity.liveHandle() != null) {
                "Synthetic BuildTestRunner owner ${sessionIdentity.pid} exited before live-owner " +
                    "cache invalidation could be verified."
            }
            Thread.sleep(50L)
        }
        check(fixtureCacheEntries.none(File::exists)) {
            "A live BuildTestRunner --build-only session left fixture result indexes " +
                "${fixtureCacheEntries.filter(File::exists).map(File::getAbsolutePath)}, so " +
                "shared-cache packaging could archive a prestart cache hit without its process."
        }
        check(sessionIdentity.liveHandle() != null) {
            "Fixture cache invalidation waited for BuildTestRunner owner ${sessionIdentity.pid} " +
                "to exit instead of completing before shared-cache packaging."
        }
        check(unrelatedCacheEntry.readText() == "must remain") {
            "Live-owner fixture cache invalidation modified unrelated cache entry " +
                "${unrelatedCacheEntry.absolutePath}."
        }

        sessionOwner.destroyForcibly()
        check(sessionOwner.waitFor(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)) {
            "Synthetic BuildTestRunner owner PID ${sessionOwner.pid()} survived forcible shutdown."
        }
        daemon.liveHandle()?.onExit()?.get(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
        cockroach.liveHandle()?.onExit()?.get(PROCESS_EXIT_SECONDS, TimeUnit.SECONDS)
        assertDead(daemon, "build-only daemon after its BuildTestRunner session exited")
        assertDead(cockroach, "build-only CockroachDB after its BuildTestRunner session exited")
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
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(
                fakeLease,
                versionedProperties().apply {
                    setProperty("pid", unrelatedIdentity.pid.toString())
                    setProperty("startedAtMillis", deliberatelyWrongStart.toEpochMilli().toString())
                },
            )
        }
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

    fun malformedRecordDiagnostics() = protect {
        val owner = startProbe("valid-owner")
        val ready = owner.awaitReady()
        val stateDirectory = File(required(ready, "stateDirectory", owner.readyFile))
        val malformedLease = File(stateDirectory, "leases/malformed-lease")
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(
                malformedLease,
                versionedProperties().apply {
                    setProperty("pid", "not-a-pid")
                    setProperty("startedAtMillis", "0")
                },
            )
        }
        val leaseReader = startProbe("invalid-lease-reader")
        check(leaseReader.awaitExit() != 0) {
            "A contender accepted malformed lease ${malformedLease.absolutePath}."
        }
        val leaseFailure = leaseReader.logFile.readText()
        check(
            malformedLease.absolutePath in leaseFailure &&
                "pid='not-a-pid'" in leaseFailure &&
                "preserved" in leaseFailure
        ) {
            "Malformed lease diagnostics did not name the path, offending pid and preservation " +
                "decision:\n$leaseFailure"
        }
        check(malformedLease.isFile) {
            "Malformed lease ${malformedLease.absolutePath} was deleted as stale."
        }
        withScenarioStateLock(stateDirectory) {
            check(malformedLease.delete()) {
                "Could not remove test-owned malformed lease ${malformedLease.absolutePath}."
            }
        }

        val nodeFile = File(stateDirectory, "node.properties")
        val validNode = properties(nodeFile)
        val malformedNode = Properties().apply {
            putAll(validNode)
            setProperty("pid", "not-a-node-pid")
        }
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(nodeFile, malformedNode)
        }
        val nodeReader = startProbe("invalid-node-reader")
        check(nodeReader.awaitExit() != 0) {
            "A contender treated malformed node state ${nodeFile.absolutePath} as missing."
        }
        val nodeFailure = nodeReader.logFile.readText()
        check(
            nodeFile.absolutePath in nodeFailure &&
                "pid='not-a-node-pid'" in nodeFailure &&
                "preserved" in nodeFailure
        ) {
            "Malformed node diagnostics did not name the path, offending pid and preservation " +
                "decision:\n$nodeFailure"
        }
        check(properties(nodeFile).getProperty("pid") == "not-a-node-pid") {
            "Malformed node state ${nodeFile.absolutePath} was overwritten or deleted."
        }
        withScenarioStateLock(stateDirectory) {
            writePropertiesAtomically(nodeFile, validNode)
        }
        assertUsable(required(ready, "jdbcUrl", owner.readyFile))
        owner.releaseAndAwait()
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
        workspace: File? = defaultWorkspace,
        startGate: File = File(runFiles, "$name-start-gate").also(::marker),
        controlDirectory: File = control,
        inheritHostAdmission: Boolean = true,
        databaseMode: String = "node-only",
    ): Probe {
        workspace?.let(::requireDirectory)
        requireDirectory(controlDirectory)
        val ready = File(runFiles, "$name-ready.properties")
        val release = File(runFiles, "$name-release")
        val armed = File(runFiles, "$name-armed.properties")
        val log = File(runFiles, "$name.log")
        val processBuilder = ProcessBuilder(
            javaBinary.absolutePath,
            *SHARED_COCKROACH_CHILD_JVM_ARGUMENTS.toTypedArray(),
            "-Djava.io.tmpdir=${root.absolutePath}",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachLeaseProbeMainKt",
            ready.absolutePath,
            release.absolutePath,
            armed.absolutePath,
            startGate.absolutePath,
            controlDirectory.absolutePath,
            databaseMode,
        )
        workspace?.let(processBuilder::directory)
        val process = processBuilder
            .redirectErrorStream(true)
            .redirectOutput(log)
            .also {
                it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
                if (inheritHostAdmission) {
                    it.environment()[
                        SharedCockroachProtocolV2Scenarios.HOST_ADMISSION_HELD_ENV
                    ] = "true"
                } else {
                    it.environment().remove(
                        SharedCockroachProtocolV2Scenarios.HOST_ADMISSION_HELD_ENV,
                    )
                }
            }
            .start()
        return Probe(name, process, ready, release, armed, log).also(probes::add)
    }

    private fun observations(type: String): List<ScenarioIdentity> =
        root.walkTopDown()
            .filter {
                it.isFile &&
                    it.name.startsWith("$type-") &&
                    "-arrived-" !in it.name &&
                    it.name.endsWith(".properties")
            }
            .map(::identity)
            .distinct()
            .toList()

    private fun observation(type: String, token: String): ScenarioIdentity {
        waitForPrefix(control, "$type-$token-")
        val matches = control.listFiles().orEmpty()
            .filter {
                it.isFile &&
                    it.name.startsWith("$type-$token-") &&
                    it.name.endsWith(".properties")
            }
        check(matches.size == 1) {
            "Expected exactly one $type observation for token '$token', but found " +
                matches.map(File::getName)
        }
        return identity(matches.single())
    }

    private fun assertObservationFilenamesAreIdentityUnique() {
        listOf("daemon", "cockroach").forEach { type ->
            control.listFiles().orEmpty()
                .filter {
                    it.isFile &&
                        it.name.startsWith("$type-") &&
                        "-arrived-" !in it.name &&
                        it.name.endsWith(".properties")
                }
                .forEach { file ->
                    val values = properties(file)
                    val token = required(values, "token", file)
                    val pid = requiredLong(values, "pid", file)
                    val startedAtMillis = requiredLong(values, "startedAtMillis", file)
                    check(
                        file.name ==
                            "$type-$token-$pid-$startedAtMillis.properties",
                    ) {
                        "Observation ${file.absolutePath} was not keyed by its complete process " +
                            "identity; expected $type-$token-$pid-$startedAtMillis.properties."
                    }
                }
        }
    }

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

    private fun stateDirectories(): List<File> =
        root.listFiles().orEmpty()
            .filter {
                it.isDirectory &&
                    it.name.startsWith(
                        "simplefilesystem-durable-shared-cockroach-" +
                            "v$SHARED_COCKROACH_PROTOCOL_VERSION-",
                    )
            }

    private fun onlyStateDirectory(): File {
        val stateDirectories = stateDirectories()
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

    private fun assertCockroachCpuBudget(identity: ScenarioIdentity) {
        val environmentFile = File("/proc/${identity.pid}/environ")
        check(environmentFile.isFile) {
            "Could not inspect the CPU budget of live CockroachDB PID ${identity.pid}: " +
                "${environmentFile.absolutePath} was missing."
        }
        val environment = environmentFile.readBytes()
            .toString(Charsets.UTF_8)
            .split('\u0000')
        check("GOMAXPROCS=2" in environment) {
            "CockroachDB PID ${identity.pid} started without the required GOMAXPROCS=2 CPU " +
                "budget; environment keys were " +
                environment.filter(String::isNotEmpty).map { it.substringBefore('=') } +
                "."
        }
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

    private fun killProcessOnly(identity: ScenarioIdentity) {
        val handle = identity.liveHandle() ?: return
        handle.destroyForcibly()
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

private fun <T> withScenarioStateLock(stateDirectory: File, block: () -> T): T {
    return synchronized(scenarioProcessLocalStateLock) {
        RandomAccessFile(File(stateDirectory, "state.lock"), "rw").use { lockAccess ->
            lockAccess.channel.lock().use { block() }
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
