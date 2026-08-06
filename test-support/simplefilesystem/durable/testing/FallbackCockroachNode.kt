package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.clocks.simple.Clock
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val FALLBACK_COCKROACH_STARTUP_TIMEOUT_MILLIS = 120_000L
private const val FALLBACK_PROCESS_STOP_FORCE_SECONDS = 5L

/** Main's proven per-test-JVM fixture path, used only when no build-phase record exists. */
internal object FallbackCockroachNode {
    private val processLocalLock = Any()
    private val stateDirectory = File(
        System.getProperty("java.io.tmpdir"),
        "simplefilesystem-durable-shared-cockroach-v1",
    )
    private val lockFile get() = File(stateDirectory, "state.lock")
    private val stateFile get() = File(stateDirectory, "node.properties")
    private val leasesDirectory get() = File(stateDirectory, "leases")

    fun acquire(leaseName: String, clock: Clock): String = withStateLock {
        purgeStaleLeases()
        val existing = readState()?.takeIf(::isLiveProcess) ?: discoverUsableNode()
        val state = existing ?: startNode(clock)
        if (existing != null) writeState(existing)
        writeLease(leaseName)
        state.jdbcUrl
    }

    fun release(leaseName: String) = withStateLock {
        deleteIfPresent(File(leasesDirectory, leaseName), "CockroachDB lease")
        purgeStaleLeases()
        val remainingLeases = leasesDirectory.listFiles().orEmpty().filter { it.isFile }
        if (remainingLeases.isEmpty()) {
            (readState() ?: discoverUsableNode())?.let(::stopNode)
            deleteIfPresent(stateFile, "CockroachDB node state")
        }
    }

    private fun startNode(clock: Clock): FallbackNodeState {
        readState()?.let(::stopNode)
        deleteIfPresent(stateFile, "stale CockroachDB node state")

        val binaryProvider = LocalCockroachCluster()
        val binary = try {
            binaryProvider.binary()
        } finally {
            binaryProvider.close()
        }
        val workDirectory = Files.createTempDirectory(
            stateDirectory.toPath(),
            "node-",
        ).toFile()
        val listeningUrlFile = File(workDirectory, "listening-url")
        val pidFile = File(workDirectory, "cockroach.pid")
        val logFile = File(workDirectory, "cockroach.out")
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

        try {
            val deadline = clock.currentTimeMillis() + FALLBACK_COCKROACH_STARTUP_TIMEOUT_MILLIS
            while (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) {
                if (!process.isAlive) {
                    throw IllegalStateException(
                        "The shared CockroachDB node exited with code ${process.exitValue()} " +
                            "before becoming ready; output:\n${logFile.readText()}",
                    )
                }
                if (clock.currentTimeMillis() > deadline) {
                    throw IllegalStateException(
                        "The shared CockroachDB node did not become ready within " +
                            "$FALLBACK_COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            logFile.readText(),
                    )
                }
                fallbackWaitUntil(clock, clock.currentTimeMillis() + 100L)
            }

            val jdbcUrl = fallbackListeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
            val state = FallbackNodeState(
                pid = process.pid(),
                processStartedAt = requireNotNull(process.info().startInstant().orElse(null)) {
                    "The shared CockroachDB process ${process.pid()} did not expose its start time"
                },
                jdbcUrl = jdbcUrl,
                workDirectory = workDirectory,
            )
            check(canConnect(state.jdbcUrl)) {
                "The shared CockroachDB node wrote ${listeningUrlFile.absolutePath}, but a JDBC " +
                    "connection to ${state.jdbcUrl} could not be established"
            }
            configureFallbackSingleNodeTestCluster(state.jdbcUrl)
            writeState(state)
            return state
        } catch (failure: Throwable) {
            try {
                stopProcess(process.toHandle(), process.pid(), logFile)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            if (workDirectory.exists() && !workDirectory.deleteRecursively()) {
                failure.addSuppressed(
                    IllegalStateException(
                        "Could not delete failed shared CockroachDB work directory " +
                            workDirectory.absolutePath,
                    ),
                )
            }
            throw failure
        }
    }

    private fun stopNode(state: FallbackNodeState) {
        val managedWorkDirectory = requireManagedWorkDirectory(state.workDirectory)
        val handle = ProcessHandle.of(state.pid).orElse(null)
        if (handle != null && processIdentityMatches(handle, state.processStartedAt)) {
            stopProcess(handle, state.pid, File(managedWorkDirectory, "cockroach.out"))
        }
        if (managedWorkDirectory.exists() && !managedWorkDirectory.deleteRecursively()) {
            throw IllegalStateException(
                "Could not delete shared CockroachDB work directory " +
                    managedWorkDirectory.absolutePath,
            )
        }
    }

    private fun stopProcess(handle: ProcessHandle, pid: Long, logFile: File) {
        var interrupted = false
        handle.destroyForcibly()
        if (handle.isAlive) {
            try {
                handle.onExit().get(FALLBACK_PROCESS_STOP_FORCE_SECONDS, TimeUnit.SECONDS)
            } catch (failure: TimeoutException) {
                throw IllegalStateException(
                    "Shared CockroachDB process $pid remained alive after forcible shutdown; " +
                        "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
                    failure,
                )
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (handle.isAlive) {
            throw IllegalStateException(
                "Shared CockroachDB process $pid remained alive after forcible shutdown; " +
                    "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
            )
        }
        if (interrupted) Thread.currentThread().interrupt()
    }

    private fun isUsable(state: FallbackNodeState): Boolean =
        isLiveProcess(state) && canConnect(state.jdbcUrl)

    private fun isLiveProcess(state: FallbackNodeState): Boolean {
        val handle = ProcessHandle.of(state.pid).orElse(null) ?: return false
        return processIdentityMatches(handle, state.processStartedAt) && handle.isAlive
    }

    private fun discoverUsableNode(): FallbackNodeState? {
        val discovered = stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .mapNotNull(::readDiscoveredNode)
            .filter(::isUsable)
        val selected = discovered.firstOrNull() ?: return null
        discovered.drop(1).forEach(::stopNode)
        return selected
    }

    private fun readDiscoveredNode(workDirectory: File): FallbackNodeState? {
        return try {
            val managedWorkDirectory = requireManagedWorkDirectory(workDirectory)
            val pid = File(managedWorkDirectory, "cockroach.pid").readText().trim().toLong()
            val handle = ProcessHandle.of(pid).orElse(null) ?: return null
            val processStartedAt = handle.info().startInstant().orElse(null) ?: return null
            val listeningUrl = File(managedWorkDirectory, "listening-url").readText().trim()
            FallbackNodeState(
                pid = pid,
                processStartedAt = processStartedAt,
                jdbcUrl = fallbackListeningUrlToJdbcUrl(listeningUrl),
                workDirectory = managedWorkDirectory,
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun canConnect(jdbcUrl: String): Boolean = try {
        Class.forName("org.postgresql.Driver")
        val boundedUrl = jdbcUrl + if (jdbcUrl.contains('?')) {
            "&connectTimeout=2"
        } else {
            "?connectTimeout=2"
        }
        DriverManager.getConnection(boundedUrl, "root", "").use { connection ->
            connection.isValid(2)
        }
    } catch (_: Exception) {
        false
    }

    private fun configureFallbackSingleNodeTestCluster(jdbcUrl: String) {
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

    private fun processIdentityMatches(handle: ProcessHandle, startedAt: Instant): Boolean =
        handle.info().startInstant().orElse(null) == startedAt

    private fun readState(): FallbackNodeState? {
        if (!stateFile.isFile) return null
        return try {
            val properties = Properties().apply {
                stateFile.inputStream().use(::load)
            }
            val jdbcUrl = requireNotNull(properties.getProperty("jdbcUrl"))
            jdbcUrlForDatabase(jdbcUrl, "state_validation")
            val workDirectory = requireManagedWorkDirectory(
                File(requireNotNull(properties.getProperty("workDirectory"))),
            )
            val discoveredJdbcUrl = fallbackListeningUrlToJdbcUrl(
                File(workDirectory, "listening-url").readText().trim(),
            )
            require(jdbcUrl == discoveredJdbcUrl) {
                "Shared CockroachDB state JDBC URL '$jdbcUrl' does not match the node's " +
                    "listening URL '$discoveredJdbcUrl'."
            }
            FallbackNodeState(
                pid = requireNotNull(properties.getProperty("pid")).toLong(),
                processStartedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("processStartedAtMillis")).toLong(),
                ),
                jdbcUrl = jdbcUrl,
                workDirectory = workDirectory,
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun writeState(state: FallbackNodeState) {
        val stagingFile = File(stateDirectory, "node.properties.part")
        val properties = Properties().apply {
            setProperty("pid", state.pid.toString())
            setProperty("processStartedAtMillis", state.processStartedAt.toEpochMilli().toString())
            setProperty("jdbcUrl", state.jdbcUrl)
            setProperty("workDirectory", state.workDirectory.absolutePath)
        }
        stagingFile.outputStream().use { properties.store(it, null) }
        try {
            Files.move(
                stagingFile.toPath(),
                stateFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(
                stagingFile.toPath(),
                stateFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
            )
        }
    }

    private fun requireManagedWorkDirectory(workDirectory: File): File {
        val managedRoot = stateDirectory.canonicalFile
        val canonical = workDirectory.canonicalFile
        require(canonical.parentFile == managedRoot && canonical.name.startsWith("node-")) {
            "Shared CockroachDB work directory must be a node-* child of " +
                "${managedRoot.absolutePath}, but was ${workDirectory.absolutePath}."
        }
        return canonical
    }

    private fun writeLease(leaseName: String) {
        check(leasesDirectory.isDirectory || leasesDirectory.mkdirs()) {
            "Could not create shared CockroachDB lease directory ${leasesDirectory.absolutePath}"
        }
        File(leasesDirectory, leaseName).writeText(currentProcessIdentity())
    }

    private fun purgeStaleLeases() {
        leasesDirectory.listFiles().orEmpty().filter { it.isFile }.forEach { lease ->
            val identity = runCatching { lease.readText().trim() }.getOrNull()
            if (identity == null || !isLiveProcessIdentity(identity)) {
                deleteIfPresent(lease, "stale CockroachDB lease")
            }
        }
    }

    private fun currentProcessIdentity(): String {
        val current = ProcessHandle.current()
        val startedAt = requireNotNull(current.info().startInstant().orElse(null)) {
            "The current test JVM ${current.pid()} did not expose its start time"
        }
        return "${current.pid()}|${startedAt.toEpochMilli()}"
    }

    private fun isLiveProcessIdentity(identity: String): Boolean {
        val parts = identity.split('|')
        if (parts.size != 2) return false
        val pid = parts[0].toLongOrNull() ?: return false
        val startedAtMillis = parts[1].toLongOrNull() ?: return false
        val handle = ProcessHandle.of(pid).orElse(null) ?: return false
        return handle.isAlive &&
            handle.info().startInstant().orElse(null)?.toEpochMilli() == startedAtMillis
    }

    private fun <T> withStateLock(block: () -> T): T = synchronized(processLocalLock) {
        check(stateDirectory.isDirectory || stateDirectory.mkdirs()) {
            "Could not create shared CockroachDB state directory ${stateDirectory.absolutePath}"
        }
        RandomAccessFile(lockFile, "rw").use { lockAccess ->
            lockAccess.channel.lock().use {
                block()
            }
        }
    }

    private fun deleteIfPresent(file: File, description: String) {
        if (file.exists() && !file.delete()) {
            throw IllegalStateException(
                "Could not delete $description at ${file.absolutePath}",
            )
        }
    }
}

private data class FallbackNodeState(
    val pid: Long,
    val processStartedAt: Instant,
    val jdbcUrl: String,
    val workDirectory: File,
)

private fun fallbackWaitUntil(clock: Clock, deadline: Long) {
    val latch = CountDownLatch(1)
    val scheduled = clock.schedule(deadline) { latch.countDown() }
    try {
        latch.await()
    } finally {
        clock.unschedule(scheduled)
    }
}

private fun fallbackListeningUrlToJdbcUrl(listeningUrl: String): String {
    val match = Regex("""^postgresql://[^@]+@([^/]+)/([^?]+)(\?.*)?$""").matchEntire(listeningUrl)
        ?: throw IllegalArgumentException(
            "The CockroachDB listening URL must have the form " +
                "postgresql://user@host:port/database?parameters, but was $listeningUrl",
        )
    return "jdbc:postgresql://${match.groupValues[1]}/${match.groupValues[2]}" +
        match.groupValues[3]
}
