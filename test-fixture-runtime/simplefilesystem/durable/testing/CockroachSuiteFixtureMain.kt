package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val COCKROACH_STARTUP_TIMEOUT_MILLIS = 120_000L
private const val COCKROACH_STOP_SECONDS = 5L

/**
 * Owns the real CockroachDB fixture and the workspace lock for one complete test-runner session.
 * Other processes only consume the atomic readiness record published by this process.
 */
fun main(args: Array<String>) {
    require(args.size >= 3) {
        "CockroachSuiteFixtureMain requires <ready-file-or-dash> <session-owner-pid> " +
            "<session-owner-started-at-millis-or-dash> [build-rule-cache-entry...], but received " +
            "${args.size} argument(s)."
    }
    val explicitReadyFile = args[0].takeUnless { it == "-" }?.let(::File)
    val sessionOwnerPid = args[1].toLongOrNull() ?: throw IllegalArgumentException(
        "The fixture session-owner PID must be numeric, but was '${args[1]}'.",
    )
    val sessionOwnerHandle = ProcessHandle.of(sessionOwnerPid).orElseThrow {
        IllegalStateException(
            "Cannot start the shared CockroachDB fixture because session-owner process " +
                "$sessionOwnerPid is not live.",
        )
    }
    val observedSessionOwner = processIdentity(sessionOwnerHandle, "fixture session owner")
    val sessionOwner = if (args[2] == "-") {
        observedSessionOwner
    } else {
        SharedProcessIdentity(
            pid = sessionOwnerPid,
            startedAtMillis = args[2].toLongOrNull() ?: throw IllegalArgumentException(
                "The fixture session-owner start time must be numeric or '-', but was '${args[2]}'.",
            ),
        ).also { expected ->
            check(expected == observedSessionOwner) {
                "Fixture session-owner process $sessionOwnerPid has start time " +
                    "${observedSessionOwner.startedAtMillis}, not the requested " +
                    "${expected.startedAtMillis}."
            }
        }
    }
    val cacheEntries = args.drop(3).map(::File)
    val stateDirectory = sharedCockroachStateDirectory()
    check(stateDirectory.isDirectory || stateDirectory.mkdirs()) {
        "Could not create shared CockroachDB state directory ${stateDirectory.absolutePath}."
    }
    RandomAccessFile(sharedCockroachLockFile(), "rw").use { lockAccess ->
        lockAccess.channel.lock().use {
            if (sessionOwner.liveHandle() == null) return
            runFixtureOwner(sessionOwner, explicitReadyFile, cacheEntries)
        }
    }
}

private fun runFixtureOwner(
    sessionOwner: SharedProcessIdentity,
    explicitReadyFile: File?,
    cacheEntries: List<File>,
) {
    cleanupStaleFixture()
    val fixtureOwner = processIdentity(ProcessHandle.current(), "fixture owner")
    val stateDirectory = sharedCockroachStateDirectory()
    val workDirectory = java.nio.file.Files.createTempDirectory(
        stateDirectory.toPath(),
        "fixture-",
    ).toFile()
    var cockroach: ManagedCockroachProcess? = null
    val shutdownHook = Thread(
        {
            deleteSharedCockroachFixtureIfOwnedBy(fixtureOwner)
            explicitReadyFile?.delete()
            runCatching { cockroach?.stop() }
            workDirectory.deleteRecursively()
        },
        "shared-cockroach-single-owner-shutdown",
    )
    Runtime.getRuntime().addShutdownHook(shutdownHook)
    try {
        cockroach = startCockroach(workDirectory)
        configureSingleNodeTestCluster(cockroach.jdbcUrl)
        val record = SharedCockroachFixtureRecord(
            workspacePath = File(".").canonicalFile.absolutePath,
            sessionOwner = sessionOwner,
            fixtureOwner = fixtureOwner,
            cockroach = cockroach.identity,
            jdbcUrl = cockroach.jdbcUrl,
            workDirectory = workDirectory,
        )
        writeSharedCockroachFixture(record)
        explicitReadyFile?.let { writeRawTextAtomically(it, cockroach.jdbcUrl) }
        while (sessionOwner.liveHandle() != null && cockroach.isAlive()) {
            cacheEntries.forEach { entry -> java.nio.file.Files.deleteIfExists(entry.toPath()) }
            Thread.sleep(250L)
        }
        check(sessionOwner.liveHandle() == null || cockroach.isAlive()) {
            "Shared CockroachDB process ${cockroach.identity.pid} exited while fixture session " +
                "owner ${sessionOwner.pid} was still live; output:\n${cockroach.logText()}"
        }
    } finally {
        deleteSharedCockroachFixtureIfOwnedBy(fixtureOwner)
        explicitReadyFile?.delete()
        cockroach?.stop()
        if (workDirectory.exists() && !workDirectory.deleteRecursively()) {
            throw IllegalStateException(
                "Could not delete shared CockroachDB work directory ${workDirectory.absolutePath}.",
            )
        }
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
        } catch (_: IllegalStateException) {
            // JVM shutdown has begun, so the registered hook owns cleanup.
        }
    }
}

private fun cleanupStaleFixture() {
    val stale = runCatching(::readSharedCockroachFixture).getOrNull()
    java.nio.file.Files.deleteIfExists(sharedCockroachReadyFile().toPath())
    stale ?: return
    stale.cockroach.liveHandle()?.let { handle ->
        handle.destroyForcibly()
        if (handle.isAlive) handle.onExit().get(COCKROACH_STOP_SECONDS, TimeUnit.SECONDS)
    }
    if (stale.workDirectory.exists() && !stale.workDirectory.deleteRecursively()) {
        throw IllegalStateException(
            "Could not delete stale shared CockroachDB work directory " +
                "${stale.workDirectory.absolutePath}.",
        )
    }
}

private fun startCockroach(workDirectory: File): ManagedCockroachProcess {
    val provider = LocalCockroachCluster()
    val binary = try {
        provider.binary()
    } finally {
        provider.close()
    }
    val listeningUrlFile = File(workDirectory, "listening-url")
    val pidFile = File(workDirectory, "cockroach.pid")
    val logFile = File(workDirectory, "cockroach.out")
    val watcher = FileSystems.getDefault().newWatchService()
    workDirectory.toPath().register(
        watcher,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_MODIFY,
    )
    val process = ProcessBuilder(
        binary.absolutePath,
        "start-single-node",
        "--insecure",
        "--store=type=mem,size=640MiB",
        "--cache=64MiB",
        "--max-sql-memory=128MiB",
        "--max-tsdb-memory=32MiB",
        "--max-disk-temp-storage=128MiB",
        "--max-go-memory=512MiB",
        "--listen-addr=localhost:0",
        "--http-addr=localhost:0",
        "--listening-url-file=${listeningUrlFile.absolutePath}",
        "--pid-file=${pidFile.absolutePath}",
    )
        .also { it.environment()["GOMAXPROCS"] = "2" }
        .directory(workDirectory)
        .redirectOutput(logFile)
        .redirectErrorStream(true)
        .start()
    val managed = ManagedCockroachProcess(
        handle = process.toHandle(),
        identity = processIdentity(process.toHandle(), "CockroachDB fixture"),
        jdbcUrl = "",
        logFile = logFile,
    )
    process.onExit().thenRun { watcher.close() }
    try {
        val deadlineNanos = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(COCKROACH_STARTUP_TIMEOUT_MILLIS)
        while (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) {
            val remainingNanos = deadlineNanos - System.nanoTime()
            if (remainingNanos <= 0L) {
                throw IllegalStateException(
                    "The shared CockroachDB fixture did not become ready within " +
                        "$COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            }
            val key = watcher.poll(remainingNanos, TimeUnit.NANOSECONDS)
                ?: throw IllegalStateException(
                    "The shared CockroachDB fixture did not become ready within " +
                        "$COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            key.pollEvents()
            check(key.reset()) {
                "Could not continue watching ${workDirectory.absolutePath} for CockroachDB readiness."
            }
        }
        val jdbcUrl = listeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
        check(canConnect(jdbcUrl)) {
            "CockroachDB wrote ${listeningUrlFile.absolutePath}, but a JDBC connection to " +
                "$jdbcUrl could not be established."
        }
        return managed.withJdbcUrl(jdbcUrl)
    } catch (failure: Throwable) {
        runCatching { managed.stop() }.exceptionOrNull()?.let(failure::addSuppressed)
        throw failure
    } finally {
        watcher.close()
    }
}

internal fun configureSingleNodeTestCluster(jdbcUrl: String) {
    DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            listOf(
                "kv.range_split.by_load_enabled",
                "sql.stats.automatic_collection.enabled",
                "sql.metrics.statement_details.enabled",
                "sql.metrics.transaction_details.enabled",
                "sql.metrics.index_usage_stats.enabled",
                "sql.stats.flush.enabled",
                "admission.kv.enabled",
                "admission.sql_kv_response.enabled",
                "admission.sql_sql_response.enabled",
                "admission.elastic_cpu.enabled",
                "admission.disk_bandwidth_tokens.elastic.enabled",
            ).forEach { setting ->
                statement.execute("SET CLUSTER SETTING $setting = false")
            }
        }
    }
}

private fun canConnect(jdbcUrl: String): Boolean = try {
    Class.forName("org.postgresql.Driver")
    val boundedUrl = jdbcUrl + if (jdbcUrl.contains('?')) "&connectTimeout=2" else "?connectTimeout=2"
    DriverManager.getConnection(boundedUrl, "root", "").use { it.isValid(2) }
} catch (_: Exception) {
    false
}

private fun listeningUrlToJdbcUrl(listeningUrl: String): String {
    val match = Regex("""^postgresql://[^@]+@([^/]+)/([^?]+)(\?.*)?$""").matchEntire(listeningUrl)
        ?: throw IllegalArgumentException(
            "The CockroachDB listening URL must have the form " +
                "postgresql://user@host:port/database?parameters, but was '$listeningUrl'.",
        )
    return "jdbc:postgresql://${match.groupValues[1]}/${match.groupValues[2]}" + match.groupValues[3]
}

private data class ManagedCockroachProcess(
    private val handle: ProcessHandle,
    val identity: SharedProcessIdentity,
    val jdbcUrl: String,
    private val logFile: File,
) {
    fun withJdbcUrl(jdbcUrl: String): ManagedCockroachProcess = copy(jdbcUrl = jdbcUrl)

    fun isAlive(): Boolean = identity.liveHandle() != null

    fun logText(): String = logFile.takeIf(File::isFile)?.readText().orEmpty()

    @Synchronized
    fun stop() {
        val liveHandle = identity.liveHandle() ?: return
        liveHandle.destroyForcibly()
        if (liveHandle.isAlive) {
            try {
                liveHandle.onExit().get(COCKROACH_STOP_SECONDS, TimeUnit.SECONDS)
            } catch (failure: TimeoutException) {
                throw IllegalStateException(
                    "Shared CockroachDB process ${identity.pid} remained alive after forcible " +
                        "shutdown; output:\n${logText()}",
                    failure,
                )
            }
        }
        check(!liveHandle.isAlive) {
            "Shared CockroachDB process ${identity.pid} remained alive after forcible shutdown; " +
                "output:\n${logText()}"
        }
    }
}
