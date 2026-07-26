package simplefilesystem.durable.testing

import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.UUID
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
        try {
            val databaseJdbcUrl = jdbcUrlForDatabase(sharedJdbcUrl, databaseName)
            Class.forName("org.postgresql.Driver")
            DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE ${quoteIdentifier(databaseName)}")
                }
            }
            adminJdbcUrl = sharedJdbcUrl
            testJdbcUrl = databaseJdbcUrl
            return this
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
    private val startingStateFile get() = File(stateDirectory, "node-starting.properties")
    private val leasesDirectory get() = File(stateDirectory, "leases")

    fun acquire(leaseName: String, clock: Clock): String {
        val outcome = withStateLock {
            purgeStaleLeases()
            val recorded = readState()
            val existing = recorded?.takeIf(::isLiveNode) ?: discoverUsableNode()
            if (existing != null) {
                writeState(existing)
                deleteIfPresent(startingStateFile, "obsolete CockroachDB startup state")
                writeLease(leaseName)
                return@withStateLock Acquisition.Ready(existing.jdbcUrl)
            }
            if (recorded != null) {
                cleanupManagedNodeDirectory(recorded.workDirectory)
            }

            val starting =
                readStartingState()?.takeIf(::isLiveDaemon) ?: discoverStartingDaemon()
            if (starting != null) {
                writeStartingState(starting)
                writeLease(leaseName)
                return@withStateLock Acquisition.Starting(starting)
            }

            deleteIfPresent(stateFile, "stale CockroachDB node state")
            deleteIfPresent(startingStateFile, "stale CockroachDB startup state")
            val adoptableWorkDirectory = discoverAdoptableNodeWorkDirectory()
            cleanupManagedNodeDirectories(adoptableWorkDirectory)
            val workDirectory = adoptableWorkDirectory ?: Files.createTempDirectory(
                stateDirectory.toPath(),
                "node-",
            ).toFile()
            val launched = launchDaemon(workDirectory)
            writeStartingState(launched)
            writeLease(leaseName)
            Acquisition.Starting(launched)
        }
        return when (outcome) {
            is Acquisition.Ready -> outcome.jdbcUrl
            is Acquisition.Starting -> {
                try {
                    waitForReadyNode(outcome.state, clock).jdbcUrl
                } catch (failure: Throwable) {
                    try {
                        release(leaseName)
                    } catch (cleanupFailure: Throwable) {
                        failure.addSuppressed(cleanupFailure)
                    }
                    throw failure
                }
            }
        }
    }

    fun release(leaseName: String) = withStateLock {
        deleteIfPresent(File(leasesDirectory, leaseName), "CockroachDB lease")
        purgeStaleLeases()
        val remainingLeases = leasesDirectory.listFiles().orEmpty().filter { it.isFile }
        if (remainingLeases.isEmpty()) {
            cleanupManagedNodeDirectories()
            deleteIfPresent(stateFile, "CockroachDB node state")
            deleteIfPresent(startingStateFile, "CockroachDB startup state")
            if (leasesDirectory.isDirectory && !leasesDirectory.delete()) {
                throw IllegalStateException(
                    "Could not delete empty shared CockroachDB lease directory " +
                        leasesDirectory.absolutePath,
                )
            }
        }
    }

    private fun launchDaemon(workDirectory: File): StartingNodeState {
        val fixtureJar = File(
            CockroachSuiteFixtureRuntime::class.java.protectionDomain.codeSource.location.toURI(),
        )
        check(fixtureJar.isFile) {
            "The shared CockroachDB fixture runtime must be a jar file, but was " +
                "${fixtureJar.absolutePath}."
        }
        val javaBinary = File(System.getProperty("java.home"), "bin/java")
        val process = ProcessBuilder(
            javaBinary.absolutePath,
            "-Xmx128m",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachNodeDaemonMainKt",
            stateDirectory.absolutePath,
            workDirectory.absolutePath,
        )
            .directory(workDirectory)
            .redirectOutput(File(workDirectory, "daemon.out"))
            .redirectErrorStream(true)
            .start()
        return StartingNodeState(
            daemonPid = process.pid(),
            daemonStartedAt = requireNotNull(process.info().startInstant().orElse(null)) {
                "The shared CockroachDB daemon ${process.pid()} did not expose its start time."
            },
            workDirectory = requireManagedWorkDirectory(workDirectory),
        )
    }

    private fun waitForReadyNode(starting: StartingNodeState, clock: Clock): NodeState {
        FileSystems.getDefault().newWatchService().use { watcher ->
            stateDirectory.toPath().register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE,
            )
            val deadline = clock.currentTimeMillis() + COCKROACH_STARTUP_TIMEOUT_MILLIS
            while (true) {
                readState()?.takeIf { state ->
                    state.daemonPid == starting.daemonPid &&
                        state.daemonStartedAt == starting.daemonStartedAt &&
                        state.workDirectory == starting.workDirectory &&
                        isLiveNode(state)
                }?.let { return it }
                if (!isLiveDaemon(starting)) {
                    throw IllegalStateException(
                        "Shared CockroachDB daemon ${starting.daemonPid} exited before publishing " +
                            "readiness; output:\n" +
                            File(starting.workDirectory, "daemon.out")
                                .takeIf(File::isFile)?.readText().orEmpty(),
                    )
                }
                val remaining = deadline - clock.currentTimeMillis()
                if (remaining <= 0L) {
                    throw IllegalStateException(
                        "Shared CockroachDB daemon ${starting.daemonPid} did not publish readiness " +
                            "within $COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            File(starting.workDirectory, "daemon.out")
                                .takeIf(File::isFile)?.readText().orEmpty(),
                    )
                }
                try {
                    val key = watcher.poll(remaining, TimeUnit.MILLISECONDS)
                        ?: throw IllegalStateException(
                            "Shared CockroachDB daemon ${starting.daemonPid} did not publish " +
                                "readiness within $COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; " +
                                "output:\n" +
                                File(starting.workDirectory, "daemon.out")
                                    .takeIf(File::isFile)?.readText().orEmpty(),
                        )
                    key.pollEvents()
                    check(key.reset()) {
                        "Could not continue watching ${stateDirectory.absolutePath} for shared " +
                            "CockroachDB readiness."
                    }
                } catch (failure: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw IllegalStateException(
                        "Interrupted while waiting for shared CockroachDB daemon " +
                            "${starting.daemonPid} to publish readiness.",
                        failure,
                    )
                } catch (failure: ClosedWatchServiceException) {
                    throw IllegalStateException(
                        "The readiness watcher for shared CockroachDB daemon " +
                            "${starting.daemonPid} closed before ${stateFile.absolutePath} appeared.",
                        failure,
                    )
                }
            }
        }
    }

    private fun cleanupManagedNodeDirectories(preserve: File? = null) {
        val preserved = preserve?.let(::requireManagedWorkDirectory)
        stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .map(::requireManagedWorkDirectory)
            .filter { it != preserved }
            .forEach(::cleanupManagedNodeDirectory)
    }

    private fun cleanupManagedNodeDirectory(workDirectory: File) {
        val daemonIdentity = readProcessIdentity(File(workDirectory, "daemon.properties"))
        val daemonHandle = daemonIdentity?.liveHandle()
        val descendants = daemonHandle?.descendants()?.use { handles ->
            handles.iterator().asSequence().mapNotNull { handle ->
                handle.info().startInstant().orElse(null)?.let { startedAt ->
                    NodeProcessIdentity(handle.pid(), startedAt)
                }
            }.toList()
        }.orEmpty()
        daemonIdentity?.let { identity ->
            stopProcess(identity, File(workDirectory, "daemon.out"))
        }
        descendants.forEach { identity ->
            stopProcess(identity, File(workDirectory, "cockroach.out"))
        }
        readProcessIdentity(File(workDirectory, "cockroach.properties"))?.let { identity ->
            stopProcess(identity, File(workDirectory, "cockroach.out"))
        }
        if (workDirectory.exists() && !workDirectory.deleteRecursively()) {
            throw IllegalStateException(
                "Could not delete shared CockroachDB work directory " +
                    workDirectory.absolutePath,
            )
        }
    }

    private fun stopProcess(identity: NodeProcessIdentity, logFile: File) {
        val handle = identity.liveHandle() ?: return
        var interrupted = false
        handle.destroyForcibly()
        if (handle.isAlive) {
            try {
                handle.onExit().get(PROCESS_STOP_FORCE_SECONDS, TimeUnit.SECONDS)
            } catch (failure: TimeoutException) {
                throw IllegalStateException(
                    "Shared CockroachDB process ${identity.pid} remained alive after forcible shutdown; " +
                        "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
                    failure,
                )
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (handle.isAlive) {
            throw IllegalStateException(
                "Shared CockroachDB process ${identity.pid} remained alive after forcible shutdown; " +
                    "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
            )
        }
        if (interrupted) Thread.currentThread().interrupt()
    }

    private fun isUsable(state: NodeState): Boolean = isLiveNode(state) && canConnect(state.jdbcUrl)

    private fun isLiveNode(state: NodeState): Boolean =
        NodeProcessIdentity(state.pid, state.processStartedAt).liveHandle() != null &&
            NodeProcessIdentity(state.daemonPid, state.daemonStartedAt).liveHandle() != null

    private fun isLiveDaemon(state: StartingNodeState): Boolean =
        NodeProcessIdentity(state.daemonPid, state.daemonStartedAt).liveHandle() != null

    private fun discoverUsableNode(): NodeState? {
        val discovered = stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .mapNotNull(::readDiscoveredNode)
            .filter(::isUsable)
        val selected = discovered.firstOrNull() ?: return null
        discovered.drop(1).forEach { cleanupManagedNodeDirectory(it.workDirectory) }
        return selected
    }

    private fun discoverAdoptableNodeWorkDirectory(): File? {
        return stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .map(::requireManagedWorkDirectory)
            .firstOrNull { workDirectory ->
                val identity = readProcessIdentity(File(workDirectory, "cockroach.properties"))
                    ?: return@firstOrNull false
                if (identity.liveHandle() == null) return@firstOrNull false
                val listeningUrl = File(workDirectory, "listening-url")
                    .takeIf { it.isFile && it.length() > 0L }
                    ?.readText()
                    ?.trim()
                    ?: return@firstOrNull false
                runCatching { listeningUrlToJdbcUrl(listeningUrl) }
                    .getOrNull()
                    ?.let(::canConnect) == true
            }
    }

    private fun discoverStartingDaemon(): StartingNodeState? {
        return stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .map(::requireManagedWorkDirectory)
            .mapNotNull { workDirectory ->
                readProcessIdentity(File(workDirectory, "daemon.properties"))?.let { identity ->
                    StartingNodeState(
                        daemonPid = identity.pid,
                        daemonStartedAt = identity.startedAt,
                        workDirectory = workDirectory,
                    )
                }
            }
            .firstOrNull(::isLiveDaemon)
    }

    private fun readDiscoveredNode(workDirectory: File): NodeState? {
        return try {
            val managedWorkDirectory = requireManagedWorkDirectory(workDirectory)
            val cockroachIdentity = readProcessIdentity(
                File(managedWorkDirectory, "cockroach.properties"),
            ) ?: return null
            val daemonIdentity = readProcessIdentity(
                File(managedWorkDirectory, "daemon.properties"),
            ) ?: return null
            val listeningUrl = File(managedWorkDirectory, "listening-url").readText().trim()
            NodeState(
                pid = cockroachIdentity.pid,
                processStartedAt = cockroachIdentity.startedAt,
                daemonPid = daemonIdentity.pid,
                daemonStartedAt = daemonIdentity.startedAt,
                jdbcUrl = listeningUrlToJdbcUrl(listeningUrl),
                workDirectory = managedWorkDirectory,
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

    private fun readState(): NodeState? {
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
            val discoveredJdbcUrl = listeningUrlToJdbcUrl(
                File(workDirectory, "listening-url").readText().trim(),
            )
            require(jdbcUrl == discoveredJdbcUrl) {
                "Shared CockroachDB state JDBC URL '$jdbcUrl' does not match the node's " +
                    "listening URL '$discoveredJdbcUrl'."
            }
            NodeState(
                pid = requireNotNull(properties.getProperty("pid")).toLong(),
                processStartedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("processStartedAtMillis")).toLong(),
                ),
                daemonPid = requireNotNull(properties.getProperty("daemonPid")).toLong(),
                daemonStartedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("daemonStartedAtMillis")).toLong(),
                ),
                jdbcUrl = jdbcUrl,
                workDirectory = workDirectory,
            )
        } catch (_: Exception) {
            // A partial/corrupt state file can be left by a killed test JVM. The node-discovery
            // path recovers its pid and URL directly from CockroachDB's atomic output files.
            null
        }
    }

    private fun readStartingState(): StartingNodeState? {
        if (!startingStateFile.isFile) return null
        return try {
            val properties = Properties().apply {
                startingStateFile.inputStream().use(::load)
            }
            StartingNodeState(
                daemonPid = requireNotNull(properties.getProperty("daemonPid")).toLong(),
                daemonStartedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("daemonStartedAtMillis")).toLong(),
                ),
                workDirectory = requireManagedWorkDirectory(
                    File(requireNotNull(properties.getProperty("workDirectory"))),
                ),
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun writeState(state: NodeState) {
        val properties = Properties().apply {
            setProperty("pid", state.pid.toString())
            setProperty("processStartedAtMillis", state.processStartedAt.toEpochMilli().toString())
            setProperty("daemonPid", state.daemonPid.toString())
            setProperty("daemonStartedAtMillis", state.daemonStartedAt.toEpochMilli().toString())
            setProperty("jdbcUrl", state.jdbcUrl)
            setProperty("workDirectory", state.workDirectory.absolutePath)
        }
        writePropertiesAtomically(stateFile, properties)
    }

    private fun writeStartingState(state: StartingNodeState) {
        val properties = Properties().apply {
            setProperty("daemonPid", state.daemonPid.toString())
            setProperty("daemonStartedAtMillis", state.daemonStartedAt.toEpochMilli().toString())
            setProperty("workDirectory", state.workDirectory.absolutePath)
        }
        writePropertiesAtomically(startingStateFile, properties)
    }

    private fun writePropertiesAtomically(file: File, properties: Properties) {
        val stagingFile = File(stateDirectory, "${file.name}.part")
        stagingFile.outputStream().use { properties.store(it, null) }
        try {
            Files.move(
                stagingFile.toPath(),
                file.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(
                stagingFile.toPath(),
                file.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
            )
        }
    }

    private fun requireManagedWorkDirectory(workDirectory: File): File {
        val managedRoot = stateDirectory.canonicalFile
        val canonical = workDirectory.canonicalFile
        require(canonical.parentFile == managedRoot && canonical.name.startsWith("node-")) {
            "Shared CockroachDB work directory must be a node-* child of ${managedRoot.absolutePath}, " +
                "but was ${workDirectory.absolutePath}."
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

    private fun readProcessIdentity(file: File): NodeProcessIdentity? {
        if (!file.isFile) return null
        return try {
            val properties = Properties().apply {
                file.inputStream().use(::load)
            }
            NodeProcessIdentity(
                pid = requireNotNull(properties.getProperty("pid")).toLong(),
                startedAt = Instant.ofEpochMilli(
                    requireNotNull(properties.getProperty("startedAtMillis")).toLong(),
                ),
            )
        } catch (_: Exception) {
            null
        }
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

private sealed interface Acquisition {
    data class Ready(val jdbcUrl: String) : Acquisition
    data class Starting(val state: StartingNodeState) : Acquisition
}

private data class NodeState(
    val pid: Long,
    val processStartedAt: Instant,
    val daemonPid: Long,
    val daemonStartedAt: Instant,
    val jdbcUrl: String,
    val workDirectory: File,
)

private data class StartingNodeState(
    val daemonPid: Long,
    val daemonStartedAt: Instant,
    val workDirectory: File,
)

private data class NodeProcessIdentity(
    val pid: Long,
    val startedAt: Instant,
) {
    fun liveHandle(): ProcessHandle? {
        val handle = ProcessHandle.of(pid).orElse(null) ?: return null
        return handle.takeIf {
            it.isAlive && it.info().startInstant().orElse(null) == startedAt
        }
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
