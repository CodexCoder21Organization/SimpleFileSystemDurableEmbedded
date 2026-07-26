package simplefilesystem.durable.testing

import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val SHARED_COCKROACH_JDBC_URL_ENV =
    "SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL"

/**
 * Explicit file barriers used only by child-process integration tests. Production callers leave
 * this null, so no test hook is consulted through ambient environment or process-global state.
 */
data class SharedCockroachFixtureControl(
    val directory: File,
)

data class SharedCockroachFixtureDiagnostics(
    val protocolVersion: String,
    val stateDirectory: File,
    val lockFile: File,
    val electionToken: String,
    val daemonPid: Long,
    val daemonStartedAtMillis: Long,
    val cockroachPid: Long,
    val cockroachStartedAtMillis: Long,
    val cockroachProcessGroupId: Long,
)

/**
 * Attaches one isolated logical database to a shared real CockroachDB node.
 *
 * Kompile and buildtest run each test file in its own JVM. When the suite launcher supplies an
 * endpoint, this class uses that node. Under buildtest's direct per-file dispatch, a versioned,
 * workspace-scoped filesystem protocol elects a detached daemon and lease files keep its node
 * alive until the last test database closes.
 */
class SharedCockroachCluster(
    private val clock: Clock = SystemClock(),
    private val fixtureControl: SharedCockroachFixtureControl? = null,
) : Closeable {
    private val databaseName = "durable_test_${UUID.randomUUID().toString().replace("-", "")}"
    private var adminJdbcUrl: String? = null
    private var testJdbcUrl: String? = null
    private var managedNodeLease = false
    private var managedDiagnostics: SharedCockroachFixtureDiagnostics? = null

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started"
        }
        val configuredJdbcUrl = System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)
            ?.takeIf { it.isNotBlank() }
        val acquired = if (configuredJdbcUrl != null) {
            null
        } else {
            SharedCockroachNode.acquire(databaseName, clock, fixtureControl).also {
                managedNodeLease = true
            }
        }
        val sharedJdbcUrl = configuredJdbcUrl ?: requireNotNull(acquired).jdbcUrl
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
            managedDiagnostics = acquired?.diagnostics
            return this
        } catch (failure: Throwable) {
            if (managedNodeLease) {
                try {
                    SharedCockroachNode.release(databaseName, fixtureControl)
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

    fun diagnostics(): SharedCockroachFixtureDiagnostics = managedDiagnostics
        ?: throw IllegalStateException(
            "Managed fixture diagnostics are unavailable because this SharedCockroachCluster is " +
                "not started with its workspace-scoped managed node.",
        )

    @Synchronized
    override fun close() {
        val configuredJdbcUrl = adminJdbcUrl ?: return
        var failure: Throwable? = null
        try {
            if (!managedNodeLease) {
                DriverManager.getConnection(configuredJdbcUrl, username, password).use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(
                            "DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE",
                        )
                    }
                }
            }
        } catch (dropFailure: Throwable) {
            failure = dropFailure
        } finally {
            adminJdbcUrl = null
            testJdbcUrl = null
            managedDiagnostics = null
            if (managedNodeLease) {
                try {
                    SharedCockroachNode.release(databaseName, fixtureControl)
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
    private val stateDirectory = sharedCockroachStateDirectory()
    private val lockFile get() = File(stateDirectory, "state.lock")
    private val ownerFile get() = File(stateDirectory, "node-owner.properties")
    private val stateFile get() = File(stateDirectory, "node.properties")
    private val heartbeatFile get() = File(stateDirectory, "node-heartbeat.properties")
    private val failureFile get() = File(stateDirectory, "node-failure.properties")
    private val leasesDirectory get() = File(stateDirectory, "leases")

    fun acquire(
        leaseName: String,
        clock: Clock,
        fixtureControl: SharedCockroachFixtureControl?,
    ): ManagedNodeAcquisition {
        var leaseWritten = false
        try {
            while (true) {
                val outcome = withStateLock {
                    validateProtocolNamespace()
                    purgeStaleLeases()
                    readOwnerClaim()?.let { recordedOwner ->
                        readStartupFailure(recordedOwner.token)?.let { startupFailure ->
                            throw startupFailureException(
                                startupFailure,
                                recordedOwner.workDirectory,
                            )
                        }
                    }
                    val owner = reconcileOwner()
                    val node = owner?.daemon?.let { daemon ->
                        readOrRecoverNode(owner, daemon)
                    }
                    if (node != null && isLiveNode(owner, node)) {
                        writeLease(leaseName)
                        leaseWritten = true
                        waitAtStateLockBarrier(fixtureControl, "acquire-after-lease")
                        return@withStateLock Acquisition.Ready(node)
                    }
                    if (owner != null) {
                        writeLease(leaseName)
                        leaseWritten = true
                        waitAtStateLockBarrier(fixtureControl, "acquire-after-lease")
                        return@withStateLock Acquisition.Starting(owner.token, owner.workDirectory)
                    }

                    cleanupUnclaimedWorkDirectories()
                    deleteIfPresent(stateFile, "stale shared CockroachDB node state")
                    deleteIfPresent(heartbeatFile, "stale shared CockroachDB heartbeat")
                    deleteIfPresent(failureFile, "stale shared CockroachDB startup failure")
                    val workDirectory = Files.createTempDirectory(
                        stateDirectory.toPath(),
                        "node-",
                    ).toFile()
                    val current = processIdentity(
                        ProcessHandle.current(),
                        "shared CockroachDB election owner",
                    )
                    val createdAtMillis = System.currentTimeMillis()
                    val claim = SharedCockroachOwnerClaim(
                        token = UUID.randomUUID().toString(),
                        electionOwner = current,
                        createdAtMillis = createdAtMillis,
                        attachDeadlineMillis =
                            createdAtMillis + SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS,
                        workDirectory = requireManagedWorkDirectory(stateDirectory, workDirectory),
                        daemon = null,
                    )
                    writeOwnerClaim(claim)
                    writeLease(leaseName)
                    leaseWritten = true
                    try {
                        launchDaemon(claim, fixtureControl)
                    } catch (failure: Throwable) {
                        try {
                            deleteIfPresent(File(leasesDirectory, leaseName), "failed startup lease")
                            leaseWritten = false
                            deleteIfPresent(ownerFile, "failed shared CockroachDB owner claim")
                            cleanupManagedWorkDirectory(claim.workDirectory, claim.token)
                        } catch (cleanupFailure: Throwable) {
                            failure.addSuppressed(cleanupFailure)
                        }
                        throw failure
                    }
                    waitAtStateLockBarrier(fixtureControl, "acquire-after-lease")
                    Acquisition.Starting(claim.token, claim.workDirectory)
                }
                when (outcome) {
                    is Acquisition.Ready -> return outcome.node.toAcquisition()
                    is Acquisition.Starting -> {
                        val ready = waitForReadyNode(outcome, clock)
                        if (ready != null) return ready.toAcquisition()
                    }
                }
            }
        } catch (failure: Throwable) {
            if (leaseWritten) {
                try {
                    release(leaseName, fixtureControl)
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressed(cleanupFailure)
                }
            }
            throw failure
        }
    }

    fun release(
        leaseName: String,
        fixtureControl: SharedCockroachFixtureControl?,
    ) = withStateLock {
        validateProtocolNamespace()
        deleteIfPresent(File(leasesDirectory, leaseName), "shared CockroachDB lease")
        purgeStaleLeases()
        reconcileOwner()
        waitAtStateLockBarrier(fixtureControl, "release-before-count")
        val remainingLeases = leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
        if (remainingLeases.isEmpty()) {
            cleanupAllManagedProcesses()
            deleteIfPresent(stateFile, "shared CockroachDB node state")
            deleteIfPresent(heartbeatFile, "shared CockroachDB heartbeat")
            deleteIfPresent(failureFile, "shared CockroachDB startup failure")
            deleteIfPresent(ownerFile, "shared CockroachDB owner claim")
            if (leasesDirectory.isDirectory && !leasesDirectory.delete()) {
                throw IllegalStateException(
                    "Could not delete empty shared CockroachDB lease directory " +
                        leasesDirectory.absolutePath,
                )
            }
        }
    }

    private fun ManagedNodeAcquisitionRecord.toAcquisition(): ManagedNodeAcquisition =
        ManagedNodeAcquisition(
            jdbcUrl = node.jdbcUrl,
            diagnostics = SharedCockroachFixtureDiagnostics(
                protocolVersion = SHARED_COCKROACH_PROTOCOL_VERSION,
                stateDirectory = stateDirectory.canonicalFile,
                lockFile = lockFile.canonicalFile,
                electionToken = owner.token,
                daemonPid = node.daemon.pid,
                daemonStartedAtMillis = node.daemon.startedAt.toEpochMilli(),
                cockroachPid = node.cockroach.pid,
                cockroachStartedAtMillis = node.cockroach.startedAt.toEpochMilli(),
                cockroachProcessGroupId = node.processGroupId,
            ),
        )

    private fun SharedCockroachNodeRecord.toAcquisition(): ManagedNodeAcquisition =
        ManagedNodeAcquisitionRecord(
            owner = requireNotNull(readOwnerClaim()) {
                "Shared CockroachDB node ${cockroach.pid} was ready without an owner claim."
            },
            node = this,
        ).toAcquisition()

    private fun launchDaemon(
        claim: SharedCockroachOwnerClaim,
        fixtureControl: SharedCockroachFixtureControl?,
    ) {
        val fixtureJar = File(
            CockroachSuiteFixtureRuntime::class.java.protectionDomain.codeSource.location.toURI(),
        )
        check(fixtureJar.isFile) {
            "The shared CockroachDB fixture runtime must be a jar file, but was " +
                "${fixtureJar.absolutePath}."
        }
        val javaBinary = File(System.getProperty("java.home"), "bin/java")
        ProcessBuilder(
            javaBinary.absolutePath,
            "-Xmx128m",
            "-cp",
            fixtureJar.absolutePath,
            "simplefilesystem.durable.testing.SharedCockroachNodeDaemonMainKt",
            stateDirectory.absolutePath,
            claim.workDirectory.absolutePath,
            claim.token,
            fixtureControl?.directory?.canonicalPath ?: "-",
        )
            .directory(claim.workDirectory)
            .redirectOutput(File(claim.workDirectory, "daemon.out"))
            .redirectErrorStream(true)
            .start()
    }

    private fun waitForReadyNode(
        starting: Acquisition.Starting,
        clock: Clock,
    ): ManagedNodeAcquisitionRecord? {
        FileSystems.getDefault().newWatchService().use { watcher ->
            stateDirectory.toPath().register(
                watcher,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE,
            )
            val deadline = clock.currentTimeMillis() + SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS
            while (true) {
                var ownerStillStarting = true
                val ready = withStateLock {
                    validateProtocolNamespace()
                    readOwnerClaim()?.takeIf { it.token == starting.token }?.let { recordedOwner ->
                        readStartupFailure(starting.token)?.let { startupFailure ->
                            throw startupFailureException(
                                startupFailure,
                                recordedOwner.workDirectory,
                            )
                        }
                    }
                    val owner = reconcileOwner()
                    if (owner == null || owner.token != starting.token) {
                        ownerStillStarting = false
                        return@withStateLock null
                    }
                    val daemon = owner.daemon
                    if (daemon != null) {
                        val node = readOrRecoverNode(owner, daemon)
                        if (node != null && isLiveNode(owner, node)) {
                            return@withStateLock ManagedNodeAcquisitionRecord(owner, node)
                        }
                        if (daemon.liveHandle() == null) ownerStillStarting = false
                    } else if (owner.electionOwner.liveHandle() == null) {
                        ownerStillStarting = false
                    }
                    null
                }
                if (ready != null) return ready
                if (!ownerStillStarting) return null
                val remaining = deadline - clock.currentTimeMillis()
                if (remaining <= 0L) {
                    throw IllegalStateException(
                        "Shared CockroachDB owner token '${starting.token}' did not publish readiness " +
                            "within $SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            File(starting.workDirectory, "daemon.out")
                                .takeIf(File::isFile)?.readText().orEmpty(),
                    )
                }
                try {
                    val key = watcher.poll(remaining, TimeUnit.MILLISECONDS)
                        ?: throw IllegalStateException(
                            "Shared CockroachDB owner token '${starting.token}' did not publish " +
                                "readiness within $SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS " +
                                "milliseconds; output:\n" +
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
                        "Interrupted while waiting for shared CockroachDB owner token " +
                            "'${starting.token}' to publish readiness.",
                        failure,
                    )
                } catch (failure: ClosedWatchServiceException) {
                    throw IllegalStateException(
                        "The readiness watcher for shared CockroachDB owner token " +
                            "'${starting.token}' closed before ${stateFile.absolutePath} appeared.",
                        failure,
                    )
                }
            }
        }
    }

    private fun reconcileOwner(): SharedCockroachOwnerClaim? {
        val owner = readOwnerClaim() ?: run {
            if (stateFile.exists() || heartbeatFile.exists()) {
                cleanupAllManagedProcesses()
                deleteIfPresent(stateFile, "unowned shared CockroachDB node state")
                deleteIfPresent(heartbeatFile, "unowned shared CockroachDB heartbeat")
            }
            return null
        }
        val now = System.currentTimeMillis()
        val daemon = owner.daemon
        val ownerIsLive = if (daemon == null) {
            owner.electionOwner.liveHandle() != null && now <= owner.attachDeadlineMillis
        } else {
            val heartbeat = readHeartbeat()
            val managedCockroachIsLive = if (!stateFile.isFile) {
                true
            } else {
                val readyNode = readNodeRecord()
                val managedIdentity =
                    readyNode?.takeIf {
                        it.token == owner.token &&
                            it.workDirectory == owner.workDirectory
                    }?.cockroach
                        ?: readIdentity(
                            File(owner.workDirectory, "cockroach.properties"),
                            "shared CockroachDB process identity",
                            owner.token,
                        )
                managedIdentity?.liveHandle() != null
            }
            daemon.liveHandle() != null &&
                managedCockroachIsLive &&
                heartbeat != null &&
                heartbeat.token == owner.token &&
                heartbeat.daemon == daemon &&
                heartbeat.writtenAtMillis <= now + SHARED_COCKROACH_HEARTBEAT_STALE_MILLIS &&
                now - heartbeat.writtenAtMillis <= SHARED_COCKROACH_HEARTBEAT_STALE_MILLIS
        }
        if (ownerIsLive) return owner

        // PREVENTED: no daemon can start CockroachDB until it attaches to this token while the
        // lock is held. RECOVERED: SIGKILL cannot run a shutdown hook, so a later acquire/release
        // uses the process-group identity below to reap a CockroachDB tree left by a dead daemon.
        cleanupManagedWorkDirectory(owner.workDirectory, owner.token)
        deleteIfPresent(stateFile, "dead-owner shared CockroachDB node state")
        deleteIfPresent(heartbeatFile, "dead-owner shared CockroachDB heartbeat")
        deleteIfPresent(failureFile, "dead-owner shared CockroachDB startup failure")
        deleteIfPresent(ownerFile, "dead shared CockroachDB owner claim")
        return null
    }

    private fun readOrRecoverNode(
        owner: SharedCockroachOwnerClaim,
        daemon: SharedProcessIdentity,
    ): SharedCockroachNodeRecord? {
        val recorded = readNodeRecord()
        if (recorded != null &&
            recorded.token == owner.token &&
            recorded.daemon == daemon &&
            recorded.workDirectory == owner.workDirectory
        ) {
            return recorded
        }
        val cockroach = try {
            readIdentity(
                File(owner.workDirectory, "cockroach.properties"),
                "shared CockroachDB process identity",
                owner.token,
            )
        } catch (_: IllegalStateException) {
            return null
        } ?: return null
        val listeningUrlFile = File(owner.workDirectory, "listening-url")
        if (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) return null
        val jdbcUrl = try {
            listeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
        } catch (_: Exception) {
            return null
        }
        if (!canConnect(jdbcUrl)) return null
        val recovered = SharedCockroachNodeRecord(
            token = owner.token,
            cockroach = cockroach,
            processGroupId = cockroach.pid,
            daemon = daemon,
            jdbcUrl = jdbcUrl,
            workDirectory = owner.workDirectory,
        )
        writeNodeRecord(recovered)
        return recovered
    }

    private fun isLiveNode(
        owner: SharedCockroachOwnerClaim,
        node: SharedCockroachNodeRecord,
    ): Boolean {
        val heartbeat = readHeartbeat() ?: return false
        val now = System.currentTimeMillis()
        return owner.daemon == node.daemon &&
            node.daemon.liveHandle() != null &&
            node.cockroach.liveHandle() != null &&
            node.processGroupId == node.cockroach.pid &&
            heartbeat.token == owner.token &&
            heartbeat.daemon == node.daemon &&
            now - heartbeat.writtenAtMillis <= SHARED_COCKROACH_HEARTBEAT_STALE_MILLIS &&
            heartbeat.writtenAtMillis <= now + SHARED_COCKROACH_HEARTBEAT_STALE_MILLIS
    }

    private fun cleanupUnclaimedWorkDirectories() {
        stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .forEach { workDirectory ->
                cleanupManagedWorkDirectory(
                    requireManagedWorkDirectory(stateDirectory, workDirectory),
                    expectedToken = null,
                )
            }
    }

    private fun cleanupAllManagedProcesses() {
        val failures = mutableListOf<Throwable>()
        val owner = try {
            readOwnerClaim()
        } catch (failure: Throwable) {
            failures += failure
            null
        }
        stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .forEach { workDirectory ->
                try {
                    val managedWorkDirectory = requireManagedWorkDirectory(
                        stateDirectory,
                        workDirectory,
                    )
                    cleanupManagedWorkDirectory(
                        managedWorkDirectory,
                        owner?.takeIf { it.workDirectory == managedWorkDirectory }?.token,
                    )
                } catch (failure: Throwable) {
                    failures += failure
                }
            }
        throwCleanupFailures("shared CockroachDB process cleanup", failures)
    }

    private fun cleanupManagedWorkDirectory(
        workDirectory: File,
        expectedToken: String?,
    ) {
        val failures = mutableListOf<Throwable>()
        var daemon: SharedProcessIdentity? = null
        var cockroach: SharedProcessIdentity? = null
        try {
            daemon = readIdentity(
                File(workDirectory, "daemon.properties"),
                "shared CockroachDB daemon identity",
                expectedToken,
            )
        } catch (failure: Throwable) {
            failures += failure
        }
        try {
            cockroach = readIdentity(
                File(workDirectory, "cockroach.properties"),
                "shared CockroachDB process identity",
                expectedToken,
            )
        } catch (failure: Throwable) {
            failures += failure
        }
        if (daemon != null) {
            try {
                stopProcess(daemon, File(workDirectory, "daemon.out"), "daemon")
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        if (cockroach != null) {
            try {
                stopCockroachProcessGroup(
                    cockroach,
                    cockroach.pid,
                    File(workDirectory, "cockroach.out"),
                )
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        val verifiedProcessesDead =
            daemon?.liveHandle() == null && cockroach?.liveHandle() == null
        if (failures.isEmpty() && verifiedProcessesDead) {
            if (workDirectory.exists() && !workDirectory.deleteRecursively()) {
                failures += IllegalStateException(
                    "Could not delete shared CockroachDB work directory " +
                        workDirectory.absolutePath,
                )
            }
        }
        throwCleanupFailures(
            "cleanup of shared CockroachDB work directory ${workDirectory.absolutePath}",
            failures,
        )
    }

    private fun stopCockroachProcessGroup(
        identity: SharedProcessIdentity,
        processGroupId: Long,
        logFile: File,
    ) {
        val handle = identity.liveHandle() ?: return
        check(processGroupId == identity.pid) {
            "Cannot stop shared CockroachDB process group $processGroupId because its verified " +
                "leader PID was ${identity.pid}."
        }
        val kill = ProcessBuilder(
            "/bin/kill",
            "-KILL",
            "--",
            "-$processGroupId",
        ).start()
        val exitCode = kill.waitFor()
        check(exitCode == 0 || !handle.isAlive) {
            "Could not signal shared CockroachDB process group $processGroupId; /bin/kill exited " +
                "with code $exitCode and process ${identity.pid} remained alive."
        }
        waitForProcessExit(identity, handle, logFile, "process group $processGroupId")
    }

    private fun stopProcess(
        identity: SharedProcessIdentity,
        logFile: File,
        description: String,
    ) {
        val handle = identity.liveHandle() ?: return
        handle.destroyForcibly()
        waitForProcessExit(identity, handle, logFile, description)
    }

    private fun waitForProcessExit(
        identity: SharedProcessIdentity,
        handle: ProcessHandle,
        logFile: File,
        description: String,
    ) {
        if (handle.isAlive) {
            try {
                handle.onExit().get(
                    SHARED_COCKROACH_PROCESS_STOP_SECONDS,
                    TimeUnit.SECONDS,
                )
            } catch (failure: TimeoutException) {
                throw IllegalStateException(
                    "Shared CockroachDB $description ${identity.pid} remained alive after forcible " +
                        "shutdown; output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
                    failure,
                )
            } catch (failure: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IllegalStateException(
                    "Interrupted while waiting for shared CockroachDB $description " +
                        "${identity.pid} to exit.",
                    failure,
                )
            }
        }
        check(!handle.isAlive) {
            "Shared CockroachDB $description ${identity.pid} remained alive after forcible " +
                "shutdown; output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}"
        }
    }

    private fun validateProtocolNamespace() {
        loadVersionedProperties(ownerFile, "shared CockroachDB owner claim")
        loadVersionedProperties(stateFile, "shared CockroachDB node state")
        loadVersionedProperties(heartbeatFile, "shared CockroachDB heartbeat")
        loadVersionedProperties(failureFile, "shared CockroachDB startup failure")
        leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
            .forEach { lease ->
            loadVersionedProperties(lease, "shared CockroachDB lease")
        }
        stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .forEach { workDirectory ->
                loadVersionedProperties(
                    File(workDirectory, "daemon.properties"),
                    "shared CockroachDB daemon identity",
                )
                loadVersionedProperties(
                    File(workDirectory, "cockroach.properties"),
                    "shared CockroachDB process identity",
                )
            }
    }

    private fun readOwnerClaim(): SharedCockroachOwnerClaim? {
        val properties = loadVersionedProperties(
            ownerFile,
            "shared CockroachDB owner claim",
        ) ?: return null
        val token = properties.getProperty("token") ?: throw IllegalStateException(
            "Shared CockroachDB owner claim ${ownerFile.absolutePath} did not contain token.",
        )
        val daemonPid = properties.getProperty("daemonPid")
        val daemonStartedAt = properties.getProperty("daemonStartedAtMillis")
        if ((daemonPid == null) != (daemonStartedAt == null)) {
            throw IllegalStateException(
                "Shared CockroachDB owner claim ${ownerFile.absolutePath} must contain both " +
                    "daemonPid and daemonStartedAtMillis, but daemonPid='$daemonPid' and " +
                    "daemonStartedAtMillis='$daemonStartedAt'.",
            )
        }
        return SharedCockroachOwnerClaim(
            token = token,
            electionOwner = parseIdentity(
                properties,
                "ownerPid",
                "ownerStartedAtMillis",
                "shared CockroachDB owner claim",
                ownerFile,
            ),
            createdAtMillis = requiredLong(properties, "createdAtMillis", ownerFile),
            attachDeadlineMillis = requiredLong(properties, "attachDeadlineMillis", ownerFile),
            workDirectory = requireManagedWorkDirectory(
                stateDirectory,
                File(
                    properties.getProperty("workDirectory") ?: throw IllegalStateException(
                        "Shared CockroachDB owner claim ${ownerFile.absolutePath} did not contain " +
                            "workDirectory.",
                    ),
                ),
            ),
            daemon = if (daemonPid == null) {
                null
            } else {
                parseIdentity(
                    properties,
                    "daemonPid",
                    "daemonStartedAtMillis",
                    "shared CockroachDB owner claim",
                    ownerFile,
                )
            },
        )
    }

    private fun writeOwnerClaim(claim: SharedCockroachOwnerClaim) {
        writePropertiesAtomically(
            ownerFile,
            versionedProperties().apply {
                setProperty("token", claim.token)
                setProperty("ownerPid", claim.electionOwner.pid.toString())
                setProperty(
                    "ownerStartedAtMillis",
                    claim.electionOwner.startedAt.toEpochMilli().toString(),
                )
                setProperty("createdAtMillis", claim.createdAtMillis.toString())
                setProperty("attachDeadlineMillis", claim.attachDeadlineMillis.toString())
                setProperty("workDirectory", claim.workDirectory.absolutePath)
                claim.daemon?.let { daemon ->
                    setProperty("daemonPid", daemon.pid.toString())
                    setProperty(
                        "daemonStartedAtMillis",
                        daemon.startedAt.toEpochMilli().toString(),
                    )
                }
            },
        )
    }

    private fun readNodeRecord(): SharedCockroachNodeRecord? {
        val properties = loadVersionedProperties(
            stateFile,
            "shared CockroachDB node state",
        ) ?: return null
        return try {
            val jdbcUrl = requireNotNull(properties.getProperty("jdbcUrl"))
            jdbcUrlForDatabase(jdbcUrl, "state_validation")
            SharedCockroachNodeRecord(
                token = requireNotNull(properties.getProperty("token")),
                cockroach = parseIdentity(
                    properties,
                    "pid",
                    "processStartedAtMillis",
                    "shared CockroachDB node state",
                    stateFile,
                ),
                processGroupId = requiredLong(properties, "processGroupId", stateFile),
                daemon = parseIdentity(
                    properties,
                    "daemonPid",
                    "daemonStartedAtMillis",
                    "shared CockroachDB node state",
                    stateFile,
                ),
                jdbcUrl = jdbcUrl,
                workDirectory = requireManagedWorkDirectory(
                    stateDirectory,
                    File(requireNotNull(properties.getProperty("workDirectory"))),
                ),
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun writeNodeRecord(node: SharedCockroachNodeRecord) {
        writePropertiesAtomically(
            stateFile,
            versionedProperties().apply {
                setProperty("token", node.token)
                setProperty("pid", node.cockroach.pid.toString())
                setProperty(
                    "processStartedAtMillis",
                    node.cockroach.startedAt.toEpochMilli().toString(),
                )
                setProperty("processGroupId", node.processGroupId.toString())
                setProperty("daemonPid", node.daemon.pid.toString())
                setProperty(
                    "daemonStartedAtMillis",
                    node.daemon.startedAt.toEpochMilli().toString(),
                )
                setProperty("jdbcUrl", node.jdbcUrl)
                setProperty("workDirectory", node.workDirectory.absolutePath)
            },
        )
    }

    private fun readHeartbeat(): SharedCockroachHeartbeat? {
        val properties = loadVersionedProperties(
            heartbeatFile,
            "shared CockroachDB heartbeat",
        ) ?: return null
        return SharedCockroachHeartbeat(
            token = properties.getProperty("token") ?: throw IllegalStateException(
                "Shared CockroachDB heartbeat ${heartbeatFile.absolutePath} did not contain token.",
            ),
            daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                "shared CockroachDB heartbeat",
                heartbeatFile,
            ),
            writtenAtMillis = requiredLong(properties, "writtenAtMillis", heartbeatFile),
        )
    }

    private fun readStartupFailure(expectedToken: String): SharedCockroachStartupFailure? {
        val properties = loadVersionedProperties(
            failureFile,
            "shared CockroachDB startup failure",
        ) ?: return null
        val token = properties.getProperty("token") ?: throw IllegalStateException(
            "Shared CockroachDB startup failure ${failureFile.absolutePath} did not contain token.",
        )
        if (token != expectedToken) return null
        return SharedCockroachStartupFailure(
            token = token,
            daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                "shared CockroachDB startup failure",
                failureFile,
            ),
            message = properties.getProperty("message") ?: throw IllegalStateException(
                "Shared CockroachDB startup failure ${failureFile.absolutePath} did not contain " +
                    "message.",
            ),
        )
    }

    private fun startupFailureException(
        startupFailure: SharedCockroachStartupFailure,
        workDirectory: File,
    ): IllegalStateException = IllegalStateException(
        "Shared CockroachDB daemon ${startupFailure.daemon.pid} failed startup for owner token " +
            "'${startupFailure.token}': ${startupFailure.message}; output:\n" +
            File(workDirectory, "daemon.out").takeIf(File::isFile)?.readText().orEmpty(),
    )

    private fun writeLease(leaseName: String) {
        check(leasesDirectory.isDirectory || leasesDirectory.mkdirs()) {
            "Could not create shared CockroachDB lease directory ${leasesDirectory.absolutePath}."
        }
        val current = processIdentity(ProcessHandle.current(), "current test JVM")
        writePropertiesAtomically(
            File(leasesDirectory, leaseName),
            versionedProperties().apply {
                setProperty("pid", current.pid.toString())
                setProperty("startedAtMillis", current.startedAt.toEpochMilli().toString())
            },
        )
    }

    private fun purgeStaleLeases() {
        val failures = mutableListOf<Throwable>()
        leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && isAtomicStagingFile(it) }
            .forEach { stagingFile ->
                try {
                    deleteIfPresent(stagingFile, "abandoned shared CockroachDB lease staging file")
                } catch (failure: Throwable) {
                    failures += failure
                }
            }
        leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
            .forEach { lease ->
            try {
                val properties = loadVersionedProperties(
                    lease,
                    "shared CockroachDB lease",
                ) ?: return@forEach
                val identity = try {
                    parseIdentity(
                        properties,
                        "pid",
                        "startedAtMillis",
                        "shared CockroachDB lease",
                        lease,
                    )
                } catch (_: IllegalStateException) {
                    null
                }
                if (identity?.liveHandle() == null) {
                    deleteIfPresent(lease, "stale shared CockroachDB lease")
                }
            } catch (failure: Throwable) {
                failures += failure
            }
            }
        throwCleanupFailures("stale shared CockroachDB lease purge", failures)
    }

    private fun waitAtStateLockBarrier(
        fixtureControl: SharedCockroachFixtureControl?,
        name: String,
    ) {
        val controlDirectory = fixtureControl?.directory?.canonicalFile ?: return
        if (!File(controlDirectory, "pause-$name").isFile) return
        check(controlDirectory.isDirectory || controlDirectory.mkdirs()) {
            "Could not create shared CockroachDB fixture-control directory " +
                controlDirectory.absolutePath
        }
        val pid = ProcessHandle.current().pid()
        writePropertiesAtomically(
            File(controlDirectory, "$name-arrived-$pid.properties"),
            versionedProperties().apply {
                setProperty("pid", pid.toString())
            },
        )
        waitForFileCreation(
            File(controlDirectory, "$name-release-$pid"),
            "shared CockroachDB $name test barrier",
        )
    }

    private fun waitForFileCreation(file: File, description: String) {
        FileSystems.getDefault().newWatchService().use { watcher ->
            file.parentFile.toPath().register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
            while (!file.isFile) {
                val key = try {
                    watcher.take()
                } catch (failure: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw IllegalStateException(
                        "Interrupted while waiting for $description at ${file.absolutePath}.",
                        failure,
                    )
                }
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${file.parentFile.absolutePath} for " +
                        "${file.name}."
                }
            }
        }
    }

    private fun requiredLong(properties: Properties, key: String, file: File): Long {
        val value = properties.getProperty(key) ?: throw IllegalStateException(
            "Shared CockroachDB record ${file.absolutePath} did not contain $key.",
        )
        return value.toLongOrNull() ?: throw IllegalStateException(
            "Shared CockroachDB record ${file.absolutePath} contained non-numeric $key='$value'.",
        )
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

    private fun <T> withStateLock(block: () -> T): T = synchronized(processLocalLock) {
        if (!stateDirectory.isDirectory) stateDirectory.mkdirs()
        check(stateDirectory.isDirectory) {
            "Could not create shared CockroachDB state directory ${stateDirectory.absolutePath}; " +
                "path exists=${stateDirectory.exists()} and " +
                "isDirectory=${stateDirectory.isDirectory}."
        }
        RandomAccessFile(lockFile, "rw").use { lockAccess ->
            lockAccess.channel.lock().use {
                block()
            }
        }
    }

    private fun throwCleanupFailures(description: String, failures: List<Throwable>) {
        if (failures.isEmpty()) return
        val failure = IllegalStateException(
            "$description failed ${failures.size} time(s); process evidence was preserved whenever " +
                "a verified process could not be confirmed dead.",
        )
        failures.forEach(failure::addSuppressed)
        throw failure
    }
}

private sealed interface Acquisition {
    data class Ready(val node: SharedCockroachNodeRecord) : Acquisition
    data class Starting(val token: String, val workDirectory: File) : Acquisition
}

private data class ManagedNodeAcquisitionRecord(
    val owner: SharedCockroachOwnerClaim,
    val node: SharedCockroachNodeRecord,
)

private data class ManagedNodeAcquisition(
    val jdbcUrl: String,
    val diagnostics: SharedCockroachFixtureDiagnostics,
)

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
