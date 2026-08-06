package simplefilesystem.durable.testing

import java.io.Closeable
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit

private const val SELF_HOSTED_FIXTURE_STOP_SECONDS = 5L

/**
 * Starts the build-phase CockroachDB owner through the same process entry point used by the build
 * rule, but in a unique workspace owned by one scenario.
 */
fun startSelfHostedSharedCockroachBuildPhaseFixture(): SelfHostedSharedCockroachBuildPhaseFixture {
    val workspace = Files.createTempDirectory("shared-cockroach-fast-path-").toFile().canonicalFile
    val prestartLog = File(workspace, "prestart.log")
    val sessionOwner = processIdentity(ProcessHandle.current(), "self-hosted fixture session owner")
    var prestart: Process? = null
    var fixtureRecord: SharedCockroachFixtureRecord? = null
    try {
        prestart = ProcessBuilder(
            File(System.getProperty("java.home"), "bin/java").absolutePath,
            "-XX:+UseSerialGC",
            "-XX:ActiveProcessorCount=1",
            "-XX:TieredStopAtLevel=1",
            "-cp",
            System.getProperty("java.class.path"),
            "simplefilesystem.durable.testing.SharedCockroachPrestartMainKt",
            sessionOwner.pid.toString(),
            sessionOwner.startedAtMillis.toString(),
        )
            .directory(workspace)
            .redirectErrorStream(true)
            .redirectOutput(prestartLog)
            .start()
        prestart.waitFor()
        check(prestart.exitValue() == 0) {
            "The self-hosted build-phase prestart process ${prestart.pid()} exited with code " +
                "${prestart.exitValue()} before its fixture became ready in " +
                "${workspace.absolutePath}; output:\n${prestartLog.readText()}"
        }
        val record = checkNotNull(readLiveSharedCockroachFixtureOrNull(workspace)) {
            "The self-hosted build-phase prestart process ${prestart.pid()} exited successfully, " +
                "but ${sharedCockroachReadyFile(workspace).absolutePath} does not contain a live " +
                "fixture record. Its output was:\n${prestartLog.readText()}"
        }
        check(record.sessionOwner == sessionOwner) {
            "The self-hosted build-phase fixture in ${workspace.absolutePath} belongs to session " +
                "owner ${record.sessionOwner.pid} started at ${record.sessionOwner.startedAtMillis}, " +
                "not test process ${sessionOwner.pid} started at ${sessionOwner.startedAtMillis}."
        }
        fixtureRecord = record
        return SelfHostedSharedCockroachBuildPhaseFixture(
            workspace = workspace,
            fixtureOwner = record.fixtureOwner,
            cockroach = record.cockroach,
        )
    } catch (failure: Throwable) {
        val descendants = prestart?.takeIf(Process::isAlive)?.descendants()?.use { stream ->
            stream.toList().asReversed()
        }.orEmpty()
        descendants.forEach { handle ->
            handle.destroyForcibly()
            runCatching {
                handle.onExit().get(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS)
            }.exceptionOrNull()?.let(failure::addSuppressed)
        }
        prestart?.takeIf(Process::isAlive)?.let { process ->
            process.destroyForcibly()
            runCatching { process.waitFor(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS) }
                .exceptionOrNull()
                ?.let(failure::addSuppressed)
        }
        val recorded = fixtureRecord ?: runCatching {
            readSharedCockroachFixture(workspace)
        }.getOrNull()
        stopRecordedProcessDuringFailedStart(recorded?.fixtureOwner, failure)
        stopRecordedProcessDuringFailedStart(recorded?.cockroach, failure)
        deleteDirectoryDuringFailedStart(
            sharedCockroachStateDirectory(workspace),
            "self-hosted CockroachDB state directory",
            failure,
        )
        deleteDirectoryDuringFailedStart(workspace, "self-hosted CockroachDB workspace", failure)
        throw failure
    }
}

private fun stopRecordedProcessDuringFailedStart(
    identity: SharedProcessIdentity?,
    failure: Throwable,
) {
    identity?.liveHandle()?.let { handle ->
        handle.destroyForcibly()
        runCatching {
            handle.onExit().get(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS)
        }.exceptionOrNull()?.let(failure::addSuppressed)
    }
}

private fun deleteDirectoryDuringFailedStart(
    directory: File,
    description: String,
    failure: Throwable,
) {
    if (directory.exists() && !directory.deleteRecursively()) {
        failure.addSuppressed(
            IllegalStateException("Could not delete $description ${directory.absolutePath}."),
        )
    }
}

class SelfHostedSharedCockroachBuildPhaseFixture internal constructor(
    val workspace: File,
    private val fixtureOwner: SharedProcessIdentity,
    private val cockroach: SharedProcessIdentity,
) : Closeable {
    private var closed = false

    fun newClient(): SharedCockroachCluster = SharedCockroachCluster(
        workspace = workspace,
        configuredSharedJdbcUrl = null,
    )

    fun clientProcessBuilder(vararg mainAndArguments: String): ProcessBuilder = ProcessBuilder(
        File(System.getProperty("java.home"), "bin/java").absolutePath,
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=1",
        "-XX:TieredStopAtLevel=1",
        "-cp",
        System.getProperty("java.class.path"),
        *mainAndArguments,
    )
        .directory(workspace)
        .also {
            it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
        }

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        var failure: Throwable? = null
        val owner = fixtureOwner.liveHandle()
        if (owner != null) {
            owner.destroy()
            if (owner.isAlive) {
                try {
                    owner.onExit().get(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS)
                } catch (caught: Throwable) {
                    failure = caught
                    owner.destroyForcibly()
                    runCatching {
                        owner.onExit().get(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS)
                    }.exceptionOrNull()?.let(caught::addSuppressed)
                }
            }
            if (owner.isAlive) {
                val stillAlive = IllegalStateException(
                    "Self-hosted build-phase fixture owner ${fixtureOwner.pid} remained alive " +
                        "after shutdown for workspace ${workspace.absolutePath}; output:\n" +
                        File(sharedCockroachStateDirectory(workspace), "fixture-owner.log")
                            .takeIf(File::isFile)
                            ?.readText()
                            .orEmpty(),
                )
                failure?.addSuppressed(stillAlive) ?: run { failure = stillAlive }
            }
        }
        cockroach.liveHandle()?.let { handle ->
            handle.destroyForcibly()
            try {
                handle.onExit().get(SELF_HOSTED_FIXTURE_STOP_SECONDS, TimeUnit.SECONDS)
            } catch (caught: Throwable) {
                failure?.addSuppressed(caught) ?: run { failure = caught }
            }
            if (handle.isAlive) {
                val stillAlive = IllegalStateException(
                    "Self-hosted CockroachDB process ${cockroach.pid} remained alive after " +
                        "shutdown for workspace ${workspace.absolutePath}.",
                )
                failure?.addSuppressed(stillAlive) ?: run { failure = stillAlive }
            }
        }
        val stateDirectory = sharedCockroachStateDirectory(workspace)
        if (stateDirectory.exists() && !stateDirectory.deleteRecursively()) {
            val cleanupFailure = IllegalStateException(
                "Could not delete self-hosted CockroachDB state directory " +
                    stateDirectory.absolutePath,
            )
            failure?.addSuppressed(cleanupFailure) ?: run { failure = cleanupFailure }
        }
        if (workspace.exists() && !workspace.deleteRecursively()) {
            val cleanupFailure = IllegalStateException(
                "Could not delete self-hosted CockroachDB workspace ${workspace.absolutePath}.",
            )
            failure?.addSuppressed(cleanupFailure) ?: run { failure = cleanupFailure }
        }
        failure?.let { throw it }
    }
}
