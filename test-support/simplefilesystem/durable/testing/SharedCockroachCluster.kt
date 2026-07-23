package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val SHARED_COCKROACH_JDBC_URL_ENV =
    "SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL"
private const val COCKROACH_STARTUP_TIMEOUT_MILLIS = 120_000L
private const val PROCESS_STOP_FORCE_SECONDS = 5L

/**
 * Attaches one isolated logical database to a shared real CockroachDB node.
 *
 * Kompile and buildtest run each test file in its own JVM. When the suite launcher supplies an
 * endpoint, this class uses that node. Under buildtest's direct per-file dispatch, a filesystem
 * lock elects the first test JVM to start one node and lease files keep it alive until the last
 * test database closes.
 */
class SharedCockroachCluster(
    private val clock: Clock = SystemClock(),
) : Closeable {
    private val databaseName = "durable_test_${UUID.randomUUID().toString().replace("-", "")}"
    private var adminJdbcUrl: String? = null
    private var testJdbcUrl: String? = null
    private var managedNodeLease = false

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started"
        }
        val configuredJdbcUrl = System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)
            ?.takeIf { it.isNotBlank() }
        val sharedJdbcUrl = if (configuredJdbcUrl != null) {
            configuredJdbcUrl
        } else {
            SharedCockroachNode.acquire(databaseName, clock).also {
                managedNodeLease = true
            }
        }
        val databaseJdbcUrl = jdbcUrlForDatabase(sharedJdbcUrl, databaseName)

        try {
            Class.forName("org.postgresql.Driver")
            DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE ${quoteIdentifier(databaseName)}")
                }
            }
        } catch (failure: Throwable) {
            if (managedNodeLease) {
                try {
                    SharedCockroachNode.release(databaseName)
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressed(cleanupFailure)
                }
                managedNodeLease = false
            }
            throw failure
        }

        adminJdbcUrl = sharedJdbcUrl
        testJdbcUrl = databaseJdbcUrl
        return this
    }

    fun jdbcUrl(): String = testJdbcUrl
        ?: throw IllegalStateException(
            "Cannot create a JDBC URL because this SharedCockroachCluster has not been started; " +
                "call start() first",
        )

    @Synchronized
    override fun close() {
        val configuredJdbcUrl = adminJdbcUrl ?: return
        var failure: Throwable? = null
        try {
            // A managed node is disposable and every database name is unique, so its process exit
            // reclaims completed databases in one bounded operation. A caller-owned fixture may
            // outlive this test and therefore still needs eager per-database cleanup.
            if (!managedNodeLease) {
                DriverManager.getConnection(configuredJdbcUrl, username, password).use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute("DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE")
                    }
                }
            }
        } catch (dropFailure: Throwable) {
            failure = dropFailure
        } finally {
            adminJdbcUrl = null
            testJdbcUrl = null
            if (managedNodeLease) {
                try {
                    SharedCockroachNode.release(databaseName)
                } catch (cleanupFailure: Throwable) {
                    val originalFailure = failure
                    if (originalFailure == null) {
                        failure = cleanupFailure
                    } else {
                        originalFailure.addSuppressed(cleanupFailure)
                    }
                } finally {
                    managedNodeLease = false
                }
            }
        }
        failure?.let { throw it }
    }
}

private object SharedCockroachNode {
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
        // A recorded node was connectivity-checked before its state file became visible, and this
        // caller immediately opens a real JDBC connection to create its isolated database. Probing
        // it again here serializes every concurrent test JVM behind a duplicate network round trip
        // while the cross-process state lock is held. PID/start-time identity is sufficient for the
        // fast path; orphan discovery still performs a full JDBC probe before adopting a node.
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

    private fun startNode(clock: Clock): NodeState {
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
            .directory(workDirectory)
            .redirectOutput(logFile)
            .redirectErrorStream(true)
            .start()

        try {
            val deadline = clock.currentTimeMillis() + COCKROACH_STARTUP_TIMEOUT_MILLIS
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
                            "$COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            logFile.readText(),
                    )
                }
                waitUntil(clock, clock.currentTimeMillis() + 100L)
            }

            val jdbcUrl = listeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
            val state = NodeState(
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
            configureSingleNodeTestCluster(state.jdbcUrl)
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

    private fun stopNode(state: NodeState) {
        val handle = ProcessHandle.of(state.pid).orElse(null)
        if (handle != null && processIdentityMatches(handle, state.processStartedAt)) {
            stopProcess(handle, state.pid, File(state.workDirectory, "cockroach.out"))
        }
        if (state.workDirectory.exists() && !state.workDirectory.deleteRecursively()) {
            throw IllegalStateException(
                "Could not delete shared CockroachDB work directory " +
                    state.workDirectory.absolutePath,
            )
        }
    }

    private fun stopProcess(handle: ProcessHandle, pid: Long, logFile: File) {
        var interrupted = false
        // The fallback node has an in-memory store and reaches this point only after its final
        // isolated database lease is gone. A graceful CockroachDB shutdown can consume most of a
        // direct test's remaining 30-second budget under CPU contention, while forcible shutdown
        // discards exactly the same process-local state immediately.
        handle.destroyForcibly()

        if (handle.isAlive) {
            try {
                handle.onExit().get(PROCESS_STOP_FORCE_SECONDS, TimeUnit.SECONDS)
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

    private fun isUsable(state: NodeState): Boolean {
        return isLiveProcess(state) && canConnect(state.jdbcUrl)
    }

    private fun isLiveProcess(state: NodeState): Boolean {
        val handle = ProcessHandle.of(state.pid).orElse(null) ?: return false
        return processIdentityMatches(handle, state.processStartedAt) && handle.isAlive
    }

    private fun discoverUsableNode(): NodeState? {
        val discovered = stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .mapNotNull(::readDiscoveredNode)
            .filter(::isUsable)
        val selected = discovered.firstOrNull() ?: return null
        discovered.drop(1).forEach(::stopNode)
        return selected
    }

    private fun readDiscoveredNode(workDirectory: File): NodeState? {
        return try {
            val pid = File(workDirectory, "cockroach.pid").readText().trim().toLong()
            val handle = ProcessHandle.of(pid).orElse(null) ?: return null
            val processStartedAt = handle.info().startInstant().orElse(null) ?: return null
            val listeningUrl = File(workDirectory, "listening-url").readText().trim()
            NodeState(
                pid = pid,
                processStartedAt = processStartedAt,
                jdbcUrl = listeningUrlToJdbcUrl(listeningUrl),
                workDirectory = workDirectory,
            )
        } catch (_: Exception) {
            // Incomplete node directories are expected after a process dies during startup. Only
            // a complete directory whose pid and JDBC endpoint are both live can be adopted.
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
        // A failed localhost probe means a persisted node record is stale. The caller replaces
        // that disposable node, and any replacement failure is propagated with its full output.
        false
    }

    private fun configureSingleNodeTestCluster(jdbcUrl: String) {
        // Load-based range splits exist to distribute hot ranges among nodes. This disposable
        // fixture has exactly one node, so a split cannot redistribute load and only makes the
        // bounded transition queries cross more ranges while sixteen test JVMs contend for two
        // CPUs. Size-based safety splits remain enabled.
        DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("SET CLUSTER SETTING kv.range_split.by_load_enabled = false")
            }
        }
    }

    private fun processIdentityMatches(handle: ProcessHandle, startedAt: Instant): Boolean =
        handle.info().startInstant().orElse(null) == startedAt

    private fun readState(): NodeState? {
        if (!stateFile.isFile) return null
        return try {
            val properties = Properties().apply {
                stateFile.inputStream().use(::load)
            }
            NodeState(
                pid = requireNotNull(properties.getProperty("pid")).toLong(),
                processStartedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("processStartedAtMillis")).toLong(),
                ),
                jdbcUrl = requireNotNull(properties.getProperty("jdbcUrl")),
                workDirectory = File(requireNotNull(properties.getProperty("workDirectory"))),
            )
        } catch (_: Exception) {
            // A partial/corrupt state file can be left by a killed test JVM. The node-discovery
            // path recovers its pid and URL directly from CockroachDB's atomic output files.
            null
        }
    }

    private fun writeState(state: NodeState) {
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

    private fun writeLease(leaseName: String) {
        check(leasesDirectory.isDirectory || leasesDirectory.mkdirs()) {
            "Could not create shared CockroachDB lease directory ${leasesDirectory.absolutePath}"
        }
        File(leasesDirectory, leaseName).writeText(currentProcessIdentity())
    }

    private fun purgeStaleLeases() {
        leasesDirectory.listFiles().orEmpty().filter { it.isFile }.forEach { lease ->
            // An interrupted JVM may leave a partial lease. Treat unreadable lease content as
            // stale, then fail loudly below if the stale file itself cannot be removed.
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

private data class NodeState(
    val pid: Long,
    val processStartedAt: Instant,
    val jdbcUrl: String,
    val workDirectory: File,
)

private fun waitUntil(clock: Clock, deadline: Long) {
    val latch = CountDownLatch(1)
    val scheduled = clock.schedule(deadline) { latch.countDown() }
    try {
        latch.await()
    } finally {
        clock.unschedule(scheduled)
    }
}

private fun listeningUrlToJdbcUrl(listeningUrl: String): String {
    val match = Regex("""^postgresql://[^@]+@([^/]+)/([^?]+)(\?.*)?$""").matchEntire(listeningUrl)
        ?: throw IllegalArgumentException(
            "The CockroachDB listening URL must have the form " +
                "postgresql://user@host:port/database?parameters, but was $listeningUrl",
        )
    return "jdbc:postgresql://${match.groupValues[1]}/${match.groupValues[2]}" +
        match.groupValues[3]
}

private fun jdbcUrlForDatabase(adminJdbcUrl: String, databaseName: String): String {
    val match = Regex("""^(jdbc:postgresql://[^/]+)/[^?]+(\?.*)?$""").matchEntire(adminJdbcUrl)
        ?: throw IllegalArgumentException(
            "The shared CockroachDB JDBC URL must have the form " +
                "jdbc:postgresql://host:port/database?parameters, but was $adminJdbcUrl",
        )
    return match.groupValues[1] + "/" + databaseName + match.groupValues[2]
}

private fun quoteIdentifier(identifier: String): String =
    "\"" + identifier.replace("\"", "\"\"") + "\""
