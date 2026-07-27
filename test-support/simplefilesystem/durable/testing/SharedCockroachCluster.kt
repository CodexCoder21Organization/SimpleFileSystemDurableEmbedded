package simplefilesystem.durable.testing

import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.ClosedWatchServiceException
import java.nio.file.AtomicMoveNotSupportedException
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
    private var fixtureReadyBeforeStart: Boolean? = null
    private var hostAdmission: Closeable? = null

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started"
        }
        val configuredJdbcUrl = System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)
            ?.takeIf { it.isNotBlank() }
        if (configuredJdbcUrl != null) {
            hostAdmission = SharedCockroachProtocolV2Scenarios.acquireHostAdmission()
        }
        val acquired = if (configuredJdbcUrl != null) {
            null
        } else {
            SharedCockroachNode.acquire(
                leaseName = databaseName,
                clock = clock,
                fixtureControl = fixtureControl,
            ).also {
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
            fixtureReadyBeforeStart = acquired?.fixtureReadyBeforeAcquire ?: true
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
            try {
                hostAdmission?.close()
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            } finally {
                hostAdmission = null
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

    fun fixtureWasReadyBeforeStart(): Boolean = fixtureReadyBeforeStart
        ?: throw IllegalStateException(
            "Shared fixture readiness timing is unavailable because this " +
                "SharedCockroachCluster has not been started.",
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
            fixtureReadyBeforeStart = null
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
            try {
                hostAdmission?.close()
            } catch (cleanupFailure: Throwable) {
                val originalFailure = failure
                if (originalFailure == null) {
                    failure = cleanupFailure
                } else {
                    originalFailure.addSuppressed(cleanupFailure)
                }
            } finally {
                hostAdmission = null
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
    private val warmupProofFile get() = File(stateDirectory, "node-warmup.properties")
    private val heartbeatFile get() = File(stateDirectory, "node-heartbeat.properties")
    private val failureFile get() = File(stateDirectory, "node-failure.properties")
    private val leasesDirectory get() = File(stateDirectory, "leases")
    private val quarantineDirectory get() = File(stateDirectory, "quarantine")

    fun acquire(
        leaseName: String,
        clock: Clock,
        fixtureControl: SharedCockroachFixtureControl?,
        leaseOwner: SharedProcessIdentity? = null,
        fixtureBuildRuleCacheEntries: List<File> = emptyList(),
        invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive: Boolean = false,
    ): ManagedNodeAcquisition {
        var leaseWritten = false
        try {
            while (true) {
                val outcome = withStateLock {
                    quarantineUnknownProtocolRecords()
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
                        readReadyNode(owner, daemon)
                    }
                    if (node != null && isLiveNode(owner, node)) {
                        writeLease(
                            leaseName,
                            leaseOwner,
                            fixtureBuildRuleCacheEntries,
                            invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive,
                        )
                        leaseWritten = true
                        waitAtStateLockBarrier(fixtureControl, "acquire-after-lease")
                        return@withStateLock Acquisition.Ready(node)
                    }
                    if (owner != null) {
                        writeLease(
                            leaseName,
                            leaseOwner,
                            fixtureBuildRuleCacheEntries,
                            invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive,
                        )
                        leaseWritten = true
                        waitAtStateLockBarrier(fixtureControl, "acquire-after-lease")
                        return@withStateLock Acquisition.Starting(owner.token, owner.workDirectory)
                    }

                    cleanupUnclaimedWorkDirectories()
                    deleteIfPresent(stateFile, "stale shared CockroachDB node state")
                    deleteIfPresent(warmupProofFile, "stale shared CockroachDB warmup proof")
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
                        startupDeadlineMillis =
                            createdAtMillis + SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS,
                        workDirectory = requireManagedWorkDirectory(stateDirectory, workDirectory),
                        daemon = null,
                        daemonProcessGroupId = null,
                    )
                    writeOwnerClaim(claim)
                    writeLease(
                        leaseName,
                        leaseOwner,
                        fixtureBuildRuleCacheEntries,
                        invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive,
                    )
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
                    is Acquisition.Ready ->
                        return outcome.node.toAcquisition(fixtureReadyBeforeAcquire = true)
                    is Acquisition.Starting -> {
                        val ready = waitForReadyNode(outcome, clock, fixtureControl)
                        if (ready != null) {
                            return ready.toAcquisition(fixtureReadyBeforeAcquire = false)
                        }
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
        quarantineUnknownProtocolRecords()
        deleteIfPresent(File(leasesDirectory, leaseName), "shared CockroachDB lease")
        purgeStaleLeases()
        reconcileOwner()
        waitAtStateLockBarrier(fixtureControl, "release-before-count")
        val remainingLeases = leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
        if (remainingLeases.isEmpty()) {
            cleanupAllManagedProcesses()
            deleteIfPresent(stateFile, "shared CockroachDB node state")
            deleteIfPresent(warmupProofFile, "shared CockroachDB warmup proof")
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

    private fun ManagedNodeAcquisitionRecord.toAcquisition(
        fixtureReadyBeforeAcquire: Boolean,
    ): ManagedNodeAcquisition =
        ManagedNodeAcquisition(
            jdbcUrl = node.jdbcUrl,
            fixtureReadyBeforeAcquire = fixtureReadyBeforeAcquire,
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

    private fun SharedCockroachNodeRecord.toAcquisition(
        fixtureReadyBeforeAcquire: Boolean,
    ): ManagedNodeAcquisition =
        ManagedNodeAcquisitionRecord(
            owner = requireNotNull(readOwnerClaim()) {
                "Shared CockroachDB node ${cockroach.pid} was ready without an owner claim."
            },
            node = this,
        ).toAcquisition(fixtureReadyBeforeAcquire)

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
            "/usr/bin/setsid",
            javaBinary.absolutePath,
            "-Xmx128m",
            *SHARED_COCKROACH_CHILD_JVM_ARGUMENTS.toTypedArray(),
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
        fixtureControl: SharedCockroachFixtureControl?,
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
                    quarantineUnknownProtocolRecords()
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
                        val node = readReadyNode(owner, daemon)
                        if (node != null && isLiveNode(owner, node)) {
                            return@withStateLock ManagedNodeAcquisitionRecord(owner, node)
                        }
                        waitAtStateLockBarrier(
                            fixtureControl,
                            "waiter-after-readiness-check",
                        )
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
            if (stateFile.exists() || warmupProofFile.exists() || heartbeatFile.exists()) {
                cleanupAllManagedProcesses()
                deleteIfPresent(stateFile, "unowned shared CockroachDB node state")
                deleteIfPresent(warmupProofFile, "unowned shared CockroachDB warmup proof")
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
            val startupWithinDeadline =
                stateFile.isFile || now <= owner.startupDeadlineMillis
            val managedCockroachIsLive = if (!stateFile.isFile) {
                true
            } else {
                val readyNode = when (val recordedNode = readNodeRecord()) {
                    is RecordReadResult.Valid -> recordedNode.value
                    RecordReadResult.Missing -> null
                    is RecordReadResult.Invalid ->
                        when (val warmupProof = readWarmupProof()) {
                            is RecordReadResult.Valid -> warmupProof.value
                            RecordReadResult.Missing -> null
                            is RecordReadResult.Invalid -> null
                        }
                }
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
                startupWithinDeadline &&
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
        deleteIfPresent(warmupProofFile, "dead-owner shared CockroachDB warmup proof")
        deleteIfPresent(heartbeatFile, "dead-owner shared CockroachDB heartbeat")
        deleteIfPresent(failureFile, "dead-owner shared CockroachDB startup failure")
        deleteIfPresent(ownerFile, "dead shared CockroachDB owner claim")
        return null
    }

    private fun readReadyNode(
        owner: SharedCockroachOwnerClaim,
        daemon: SharedProcessIdentity,
    ): SharedCockroachNodeRecord? {
        val recordedResult = readNodeRecord()
        val warmupResult = readWarmupProof()
        val validWarmupProof = (warmupResult as? RecordReadResult.Valid)?.value
        if (recordedResult is RecordReadResult.Invalid &&
            validWarmupProof != null &&
            validWarmupProof.token == owner.token &&
            validWarmupProof.daemon == daemon &&
            validWarmupProof.workDirectory == owner.workDirectory &&
            recordedResult.recoveryIdentity == validWarmupProof.recoveryIdentity()
        ) {
            preserveInvalidReadinessRecord(stateFile)
            writeNodeRecord(validWarmupProof)
            return validWarmupProof
        }
        val invalidRecords = listOf(recordedResult, warmupResult)
            .filterIsInstance<RecordReadResult.Invalid>()
            .map(RecordReadResult.Invalid::failure)
        if (invalidRecords.isNotEmpty()) {
            val failure = IllegalStateException(
                "Shared CockroachDB readiness contained ${invalidRecords.size} invalid durable " +
                    "record(s); every invalid record was preserved.",
            )
            invalidRecords.forEach(failure::addSuppressed)
            throw failure
        }
        val recorded = recordedResult.valueOrThrow()
        if (recorded != null &&
            recorded.token == owner.token &&
            recorded.daemon == daemon &&
            recorded.workDirectory == owner.workDirectory
        ) {
            return recorded
        }
        val warmupProof = warmupResult.valueOrThrow() ?: return null
        if (warmupProof.token != owner.token ||
            warmupProof.daemon != daemon ||
            warmupProof.workDirectory != owner.workDirectory
        ) {
            return null
        }
        writeNodeRecord(warmupProof)
        return warmupProof
    }

    private fun preserveInvalidReadinessRecord(file: File) {
        val invalidDirectory = File(quarantineDirectory, "invalid-readiness")
        check(invalidDirectory.isDirectory || invalidDirectory.mkdirs()) {
            "Could not create invalid shared CockroachDB readiness quarantine directory " +
                invalidDirectory.absolutePath
        }
        val preserved = File(
            invalidDirectory,
            "${file.name}.${UUID.randomUUID()}.invalid",
        )
        try {
            Files.move(
                file.toPath(),
                preserved.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
            )
        } catch (failure: AtomicMoveNotSupportedException) {
            throw IllegalStateException(
                "Could not atomically preserve invalid shared CockroachDB readiness record " +
                    "${file.absolutePath} at ${preserved.absolutePath}: the filesystem does not " +
                    "support the required ATOMIC_MOVE operation.",
                failure,
            )
        }
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
            node.processGroupId == owner.daemonProcessGroupId &&
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
        val evidence = linkedSetOf<ManagedProcessEvidence>()
        collectProcessEvidence(
            evidence,
            failures,
            File(workDirectory, "daemon.properties"),
            "shared CockroachDB daemon identity",
        ) { properties, file ->
            readProcessEvidence(
                properties,
                file,
                "shared CockroachDB daemon identity",
                expectedToken,
                defaultProcessGroupToPid = false,
            )
        }
        collectProcessEvidence(
            evidence,
            failures,
            File(workDirectory, "cockroach.properties"),
            "shared CockroachDB process identity",
        ) { properties, file ->
            readProcessEvidence(
                properties,
                file,
                "shared CockroachDB process identity",
                expectedToken,
                defaultProcessGroupToPid = true,
            )
        }

        if (expectedToken != null) {
            collectOwnerProcessEvidence(
                evidence,
                failures,
                expectedToken,
                workDirectory,
            )
            collectNodeProcessEvidence(
                evidence,
                failures,
                stateFile,
                "shared CockroachDB node state",
                expectedToken,
                workDirectory,
            )
            collectNodeProcessEvidence(
                evidence,
                failures,
                warmupProofFile,
                "shared CockroachDB warmup proof",
                expectedToken,
                workDirectory,
            )
            collectDaemonRecordEvidence(
                evidence,
                failures,
                heartbeatFile,
                "shared CockroachDB heartbeat",
                expectedToken,
            )
            collectDaemonRecordEvidence(
                evidence,
                failures,
                failureFile,
                "shared CockroachDB startup failure",
                expectedToken,
            )
        }

        evidence.filter { process ->
            process.processGroupId != null &&
                process.identity.pid != process.processGroupId
        }.forEach { process ->
            try {
                stopProcess(
                    process.identity,
                    File(workDirectory, "cockroach.out"),
                    "recorded process-group member",
                )
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        evidence.filter { it.processGroupId != null }
            .groupBy { requireNotNull(it.processGroupId) }
            .forEach { (groupId, members) ->
            try {
                val leader = members.singleOrNull { it.identity.pid == groupId }
                    ?: throw IllegalStateException(
                        "Shared CockroachDB process evidence for group $groupId did not contain " +
                            "exactly one verified leader; identities=" +
                            members.map { "${it.identity.pid}@${it.identity.startedAt}" },
                    )
                stopRecordedProcessGroup(
                    leader.identity,
                    groupId,
                    File(workDirectory, "cockroach.out"),
                )
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        evidence.filter { process ->
            process.processGroupId == null ||
                process.identity.pid == process.processGroupId
        }.forEach { process ->
            try {
                stopProcess(
                    process.identity,
                    File(workDirectory, "daemon.out"),
                    "recorded process",
                )
            } catch (failure: Throwable) {
                failures += failure
            }
        }

        evidence.forEach { process ->
            try {
                // Process-group signalling and /proc enumeration are separate observations. Under
                // concurrent process churn a verified leader can still be alive when this final
                // durable-identity pass begins, so reap that exact PID/start-time identity here
                // instead of turning the last observation into a process leak.
                val processLog = if (
                    process.processGroupId != null &&
                    process.identity.pid != process.processGroupId
                ) {
                    File(workDirectory, "cockroach.out")
                } else {
                    File(workDirectory, "daemon.out")
                }
                stopProcess(
                    process.identity,
                    processLog,
                    "durably recorded process",
                )
                check(process.identity.liveHandle() == null) {
                    "Shared CockroachDB process evidence recorded PID ${process.identity.pid} " +
                        "started at ${process.identity.startedAt}, but it remained alive; " +
                        "preserving ${workDirectory.absolutePath}."
                }
            } catch (failure: Throwable) {
                failures += failure
            }
        }
        val verifiedProcessesDead = evidence.all { it.identity.liveHandle() == null }
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

    private fun collectProcessEvidence(
        evidence: MutableSet<ManagedProcessEvidence>,
        failures: MutableList<Throwable>,
        file: File,
        description: String,
        parse: (Properties, File) -> ManagedProcessEvidence?,
    ) {
        try {
            val properties = loadVersionedProperties(file, description) ?: return
            parse(properties, file)?.let(evidence::add)
        } catch (failure: Throwable) {
            failures += failure
        }
    }

    private fun collectOwnerProcessEvidence(
        evidence: MutableSet<ManagedProcessEvidence>,
        failures: MutableList<Throwable>,
        expectedToken: String,
        expectedWorkDirectory: File,
    ) {
        collectProcessEvidence(
            evidence,
            failures,
            ownerFile,
            "shared CockroachDB owner claim",
        ) { properties, file ->
            val token = requiredProperty(
                properties,
                "token",
                file,
                "shared CockroachDB owner claim",
            )
            parseIdentity(
                properties,
                "ownerPid",
                "ownerStartedAtMillis",
                "shared CockroachDB owner claim election owner",
                file,
            )
            val recordedWorkDirectory = requireManagedWorkDirectory(
                stateDirectory,
                File(
                    requiredProperty(
                        properties,
                        "workDirectory",
                        file,
                        "shared CockroachDB owner claim",
                    ),
                ),
            )
            if (properties.getProperty("daemonPid") == null) {
                return@collectProcessEvidence null
            }
            val daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                "shared CockroachDB owner claim daemon",
                file,
            )
            val groupId = requiredLong(properties, "daemonProcessGroupId", file)
            if (token == expectedToken && recordedWorkDirectory == expectedWorkDirectory) {
                ManagedProcessEvidence(daemon, groupId)
            } else {
                null
            }
        }
    }

    private fun collectNodeProcessEvidence(
        evidence: MutableSet<ManagedProcessEvidence>,
        failures: MutableList<Throwable>,
        file: File,
        description: String,
        expectedToken: String,
        expectedWorkDirectory: File,
    ) {
        collectProcessEvidence(evidence, failures, file, description) { properties, source ->
            val token = requiredProperty(properties, "token", source, description)
            val cockroach = parseIdentity(
                properties,
                "pid",
                "processStartedAtMillis",
                "$description CockroachDB process",
                source,
            )
            val groupId = requiredLong(properties, "processGroupId", source)
            val daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                "$description daemon",
                source,
            )
            val recordedWorkDirectory = requireManagedWorkDirectory(
                stateDirectory,
                File(requiredProperty(properties, "workDirectory", source, description)),
            )
            properties.getProperty("jdbcUrl")?.let { jdbcUrl ->
                jdbcUrlForDatabase(jdbcUrl, "cleanup_validation")
            } ?: throw IllegalStateException(
                "$description ${source.absolutePath} did not contain jdbcUrl.",
            )
            if (token == expectedToken && recordedWorkDirectory == expectedWorkDirectory) {
                evidence += ManagedProcessEvidence(daemon, groupId)
                ManagedProcessEvidence(cockroach, groupId)
            } else {
                null
            }
        }
    }

    private fun collectDaemonRecordEvidence(
        evidence: MutableSet<ManagedProcessEvidence>,
        failures: MutableList<Throwable>,
        file: File,
        description: String,
        expectedToken: String,
    ) {
        collectProcessEvidence(evidence, failures, file, description) { properties, source ->
            val token = requiredProperty(properties, "token", source, description)
            val daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                "$description daemon",
                source,
            )
            val groupId = requiredLong(properties, "daemonProcessGroupId", source)
            if (token == expectedToken) ManagedProcessEvidence(daemon, groupId) else null
        }
    }

    private fun stopRecordedProcessGroup(
        groupLeader: SharedProcessIdentity,
        recordedProcessGroupId: Long,
        logFile: File,
    ) {
        check(recordedProcessGroupId == groupLeader.pid) {
            "Cannot stop shared CockroachDB process group $recordedProcessGroupId because its " +
                "verified leader PID was ${groupLeader.pid}."
        }
        val members = liveProcessGroupMembers(recordedProcessGroupId)
        if (members.isEmpty()) return
        val liveLeader = groupLeader.liveHandle()
        if (liveLeader != null) {
            val actualProcessGroupId = processGroupId(
                groupLeader,
                "shared CockroachDB recorded process-group leader",
            )
            check(actualProcessGroupId == recordedProcessGroupId) {
                "Cannot stop recorded shared CockroachDB process group $recordedProcessGroupId " +
                    "because its verified leader PID ${groupLeader.pid} started at " +
                    "${groupLeader.startedAt} currently belongs to process group " +
                    "$actualProcessGroupId."
            }
        } else if (members.any { it.identity.pid == recordedProcessGroupId }) {
            // The recorded leader identity is dead but its PID now leads a live group, proving
            // reuse. Those processes are not owned by this record and must not be signalled.
            return
        }
        val kill = ProcessBuilder(
            "/bin/kill",
            "-KILL",
            "--",
            "-$recordedProcessGroupId",
        ).start()
        val exitCode = kill.waitFor()
        check(
            exitCode == 0 || liveProcessGroupMembers(recordedProcessGroupId).isEmpty(),
        ) {
            "Could not signal shared CockroachDB process group $recordedProcessGroupId; /bin/kill " +
                "exited with code $exitCode and the process group remained alive."
        }
        members.forEach { member ->
            member.identity.liveHandle()?.let { liveHandle ->
                waitForProcessExit(
                    member.identity,
                    liveHandle,
                    logFile,
                    "process-group $recordedProcessGroupId member",
                )
            }
        }
        check(liveProcessGroupMembers(recordedProcessGroupId).isEmpty()) {
            "Shared CockroachDB process group $recordedProcessGroupId still had live members after " +
                "forcible shutdown."
        }
    }

    private fun liveProcessGroupMembers(processGroupId: Long): List<ManagedProcessMember> =
        File("/proc").listFiles().orEmpty().mapNotNull { processDirectory ->
            val pid = processDirectory.name.toLongOrNull() ?: return@mapNotNull null
            val statFile = File(processDirectory, "stat")
            val stat = try {
                statFile.readText()
            } catch (failure: Exception) {
                val handle = ProcessHandle.of(pid).orElse(null)
                if (handle == null || !handle.isAlive) return@mapNotNull null
                throw IllegalStateException(
                    "Could not inspect live PID $pid while enumerating shared CockroachDB process " +
                        "group $processGroupId from ${statFile.absolutePath}: ${failure.message}",
                    failure,
                )
            }
            val commandEnd = stat.lastIndexOf(") ")
            check(commandEnd >= 0) {
                "Linux process state ${statFile.absolutePath} had an unrecognised value '$stat'."
            }
            val fields = stat.substring(commandEnd + 2).trim().split(Regex("\\s+"))
            check(fields.size > 2) {
                "Linux process state ${statFile.absolutePath} did not contain a process-group " +
                    "field: '$stat'."
            }
            val actualGroup = fields[2].toLongOrNull() ?: throw IllegalStateException(
                "Linux process state ${statFile.absolutePath} contained non-numeric process group " +
                    "'${fields[2]}'.",
            )
            if (actualGroup != processGroupId || fields[0] == "Z") return@mapNotNull null
            val handle = ProcessHandle.of(pid).orElse(null) ?: return@mapNotNull null
            if (!handle.isAlive) return@mapNotNull null
            ManagedProcessMember(
                try {
                    processIdentity(handle, "process-group $processGroupId member")
                } catch (failure: IllegalArgumentException) {
                    if (!handle.isAlive) return@mapNotNull null
                    throw IllegalStateException(
                        "Live process-group $processGroupId member PID $pid did not expose its " +
                            "start time.",
                        failure,
                    )
                },
            )
        }

    private fun readProcessEvidence(
        properties: Properties,
        file: File,
        description: String,
        expectedToken: String?,
        defaultProcessGroupToPid: Boolean,
    ): ManagedProcessEvidence? {
        val token = properties.getProperty("token") ?: throw IllegalStateException(
            "$description at ${file.absolutePath} did not contain token.",
        )
        if (expectedToken != null && token != expectedToken) {
            throw IllegalStateException(
                "$description at ${file.absolutePath} contained token '$token', but cleanup " +
                    "expected token '$expectedToken'.",
            )
        }
        val identity = parseIdentity(properties, "pid", "startedAtMillis", description, file)
        val groupValue = properties.getProperty("processGroupId")
        val groupId = if (groupValue == null) {
            identity.pid.takeIf { defaultProcessGroupToPid }
        } else {
            groupValue.toLongOrNull() ?: throw IllegalStateException(
                "$description at ${file.absolutePath} contained non-numeric " +
                    "processGroupId='$groupValue'.",
            )
        }
        return ManagedProcessEvidence(identity, groupId)
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

    private fun quarantineUnknownProtocolRecords() {
        quarantineIfUnknown(ownerFile, "shared CockroachDB owner claim")
        quarantineIfUnknown(stateFile, "shared CockroachDB node state")
        quarantineIfUnknown(warmupProofFile, "shared CockroachDB warmup proof")
        quarantineIfUnknown(heartbeatFile, "shared CockroachDB heartbeat")
        quarantineIfUnknown(failureFile, "shared CockroachDB startup failure")
        leasesDirectory.listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
            .forEach { lease ->
                quarantineIfUnknown(lease, "shared CockroachDB lease")
            }
        stateDirectory.listFiles().orEmpty()
            .filter { it.isDirectory && it.name.startsWith("node-") }
            .forEach { workDirectory ->
                quarantineIfUnknown(
                    File(workDirectory, "daemon.properties"),
                    "shared CockroachDB daemon identity",
                )
                quarantineIfUnknown(
                    File(workDirectory, "cockroach.properties"),
                    "shared CockroachDB process identity",
                )
            }
    }

    private fun quarantineIfUnknown(file: File, description: String) {
        if (!file.isFile) return
        val properties = try {
            Properties().apply {
                file.inputStream().use(::load)
            }
        } catch (failure: Exception) {
            throw IllegalStateException(
                "Could not inspect $description at ${file.absolutePath} for protocol quarantine: " +
                    failure.message,
                failure,
            )
        }
        val foundVersion = properties.getProperty("protocolVersion") ?: "<missing>"
        if (foundVersion == SHARED_COCKROACH_PROTOCOL_VERSION) return
        val safeVersion = foundVersion.replace(Regex("[^A-Za-z0-9._-]"), "_")
        val versionDirectory = File(quarantineDirectory, "protocol-$safeVersion")
        check(versionDirectory.isDirectory || versionDirectory.mkdirs()) {
            "Could not create shared CockroachDB protocol quarantine directory " +
                versionDirectory.absolutePath
        }
        val target = File(
            versionDirectory,
            "${UUID.randomUUID()}-${file.parentFile.name}-${file.name}",
        )
        try {
            Files.move(
                file.toPath(),
                target.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
            )
        } catch (failure: AtomicMoveNotSupportedException) {
            throw IllegalStateException(
                "Cannot quarantine unknown-version $description ${file.absolutePath} at " +
                    "${target.absolutePath}: the filesystem does not support the required " +
                    "ATOMIC_MOVE operation. The unknown record was left unchanged.",
                failure,
            )
        } catch (failure: Exception) {
            throw IllegalStateException(
                "Could not quarantine unknown-version $description ${file.absolutePath} at " +
                    "${target.absolutePath}: ${failure.message}. The unknown record was left " +
                    "unchanged.",
                failure,
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
        val daemonProcessGroupId = properties.getProperty("daemonProcessGroupId")
        if (setOf(daemonPid, daemonStartedAt, daemonProcessGroupId).map { it == null }.distinct().size != 1) {
            throw IllegalStateException(
                "Shared CockroachDB owner claim ${ownerFile.absolutePath} must contain daemonPid, " +
                    "daemonStartedAtMillis and daemonProcessGroupId together, but daemonPid=" +
                    "'$daemonPid', daemonStartedAtMillis='$daemonStartedAt' and " +
                    "daemonProcessGroupId='$daemonProcessGroupId'.",
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
            startupDeadlineMillis = requiredLong(
                properties,
                "startupDeadlineMillis",
                ownerFile,
            ),
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
            daemonProcessGroupId = daemonProcessGroupId?.toLongOrNull()
                ?: daemonProcessGroupId?.let {
                    throw IllegalStateException(
                        "Shared CockroachDB owner claim ${ownerFile.absolutePath} contained " +
                            "non-numeric daemonProcessGroupId='$it'.",
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
                setProperty("startupDeadlineMillis", claim.startupDeadlineMillis.toString())
                setProperty("workDirectory", claim.workDirectory.absolutePath)
                claim.daemon?.let { daemon ->
                    setProperty("daemonPid", daemon.pid.toString())
                    setProperty(
                        "daemonStartedAtMillis",
                        daemon.startedAt.toEpochMilli().toString(),
                    )
                    setProperty(
                        "daemonProcessGroupId",
                        requireNotNull(claim.daemonProcessGroupId) {
                            "Attached shared CockroachDB daemon ${daemon.pid} did not have a " +
                                "recorded process group."
                        }.toString(),
                    )
                }
            },
        )
    }

    private fun readNodeRecord(): RecordReadResult<SharedCockroachNodeRecord> =
        readNodeRecord(stateFile, "shared CockroachDB node state")

    private fun readWarmupProof(): RecordReadResult<SharedCockroachNodeRecord> =
        readNodeRecord(warmupProofFile, "shared CockroachDB warmup proof")

    private fun readNodeRecord(
        file: File,
        description: String,
    ): RecordReadResult<SharedCockroachNodeRecord> {
        if (!file.isFile) return RecordReadResult.Missing
        var recoveryIdentity: SharedCockroachNodeRecoveryIdentity? = null
        return try {
            val properties = loadVersionedProperties(
                file,
                description,
            ) ?: return RecordReadResult.Missing
            val token = requiredProperty(properties, "token", file, description)
            val cockroach = parseIdentity(
                properties,
                "pid",
                "processStartedAtMillis",
                description,
                file,
            )
            val processGroupId = requiredLong(properties, "processGroupId", file)
            val daemon = parseIdentity(
                properties,
                "daemonPid",
                "daemonStartedAtMillis",
                description,
                file,
            )
            recoveryIdentity = SharedCockroachNodeRecoveryIdentity(
                token = token,
                cockroach = cockroach,
                processGroupId = processGroupId,
                daemon = daemon,
            )
            val jdbcUrl = requiredProperty(properties, "jdbcUrl", file, description)
            jdbcUrlForDatabase(jdbcUrl, "state_validation")
            RecordReadResult.Valid(
                SharedCockroachNodeRecord(
                    token = token,
                    cockroach = cockroach,
                    processGroupId = processGroupId,
                    daemon = daemon,
                    jdbcUrl = jdbcUrl,
                    workDirectory = requireManagedWorkDirectory(
                        stateDirectory,
                        File(requiredProperty(properties, "workDirectory", file, description)),
                    ),
                ),
            )
        } catch (failure: Exception) {
            RecordReadResult.Invalid(
                IllegalStateException(
                    "Invalid $description at ${file.absolutePath}: ${failure.message}. The record " +
                        "was preserved.",
                    failure,
                ),
                recoveryIdentity,
            )
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

    private fun writeLease(
        leaseName: String,
        leaseOwner: SharedProcessIdentity?,
        fixtureBuildRuleCacheEntries: List<File>,
        invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive: Boolean,
    ) {
        check(leasesDirectory.isDirectory || leasesDirectory.mkdirs()) {
            "Could not create shared CockroachDB lease directory ${leasesDirectory.absolutePath}."
        }
        val current = leaseOwner
            ?: processIdentity(ProcessHandle.current(), "current test JVM")
        check(current.liveHandle() != null) {
            "Cannot write shared CockroachDB lease '$leaseName' for PID ${current.pid} started at " +
                "${current.startedAt} because that process is not alive."
        }
        writePropertiesAtomically(
            File(leasesDirectory, leaseName),
            versionedProperties().apply {
                setProperty("pid", current.pid.toString())
                setProperty("startedAtMillis", current.startedAt.toEpochMilli().toString())
                fixtureBuildRuleCacheEntries.forEachIndexed { index, cacheEntry ->
                    setProperty("fixtureBuildRuleCacheEntry.$index", cacheEntry.canonicalPath)
                }
                setProperty(
                    "invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive",
                    invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive.toString(),
                )
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
                when (val result = readLease(lease)) {
                    RecordReadResult.Missing -> Unit
                    is RecordReadResult.Invalid -> failures += result.failure
                    is RecordReadResult.Valid -> {
                        try {
                            if (result.value.identity.liveHandle() == null) {
                                invalidateFixtureBuildRuleCacheEntry(result.value.properties, lease)
                                deleteIfPresent(lease, "stale shared CockroachDB lease")
                            }
                        } catch (failure: Throwable) {
                            failures += failure
                        }
                    }
                }
            }
        throwCleanupFailures("stale shared CockroachDB lease purge", failures)
    }

    private fun readLease(
        lease: File,
    ): RecordReadResult<SharedCockroachLease> {
        if (!lease.isFile) return RecordReadResult.Missing
        return try {
            val properties = loadVersionedProperties(
                lease,
                "shared CockroachDB lease",
            ) ?: return RecordReadResult.Missing
            RecordReadResult.Valid(
                SharedCockroachLease(
                    identity = parseIdentity(
                        properties,
                        "pid",
                        "startedAtMillis",
                        "shared CockroachDB lease",
                        lease,
                    ),
                    properties = properties,
                ),
            )
        } catch (failure: Exception) {
            RecordReadResult.Invalid(
                IllegalStateException(
                    "Invalid shared CockroachDB lease at ${lease.absolutePath}: " +
                        "${failure.message}. The lease was preserved.",
                    failure,
                ),
            )
        }
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

    private fun requiredProperty(
        properties: Properties,
        key: String,
        file: File,
        description: String,
    ): String = properties.getProperty(key) ?: throw IllegalStateException(
        "$description ${file.absolutePath} did not contain $key.",
    )

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

private sealed interface RecordReadResult<out T> {
    data object Missing : RecordReadResult<Nothing>
    data class Valid<T>(val value: T) : RecordReadResult<T>
    data class Invalid(
        val failure: IllegalStateException,
        val recoveryIdentity: SharedCockroachNodeRecoveryIdentity? = null,
    ) : RecordReadResult<Nothing>
}

private fun <T> RecordReadResult<T>.valueOrThrow(): T? = when (this) {
    RecordReadResult.Missing -> null
    is RecordReadResult.Valid -> value
    is RecordReadResult.Invalid -> throw failure
}

private sealed interface Acquisition {
    data class Ready(val node: SharedCockroachNodeRecord) : Acquisition
    data class Starting(val token: String, val workDirectory: File) : Acquisition
}

private data class ManagedNodeAcquisitionRecord(
    val owner: SharedCockroachOwnerClaim,
    val node: SharedCockroachNodeRecord,
)

private data class SharedCockroachNodeRecoveryIdentity(
    val token: String,
    val cockroach: SharedProcessIdentity,
    val processGroupId: Long,
    val daemon: SharedProcessIdentity,
)

private fun SharedCockroachNodeRecord.recoveryIdentity() =
    SharedCockroachNodeRecoveryIdentity(
        token = token,
        cockroach = cockroach,
        processGroupId = processGroupId,
        daemon = daemon,
    )

private data class ManagedNodeAcquisition(
    val jdbcUrl: String,
    val fixtureReadyBeforeAcquire: Boolean,
    val diagnostics: SharedCockroachFixtureDiagnostics,
)

private data class ManagedProcessEvidence(
    val identity: SharedProcessIdentity,
    val processGroupId: Long?,
)

private data class ManagedProcessMember(
    val identity: SharedProcessIdentity,
)

private data class SharedCockroachLease(
    val identity: SharedProcessIdentity,
    val properties: Properties,
)

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

internal fun prestartSharedCockroachForSession(
    sessionOwner: SharedProcessIdentity,
    fixtureBuildRuleCacheEntries: List<File>,
    invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive: Boolean = false,
) {
    val leaseName = "kompile-session-${sessionOwner.pid}-${sessionOwner.startedAt.toEpochMilli()}"
    SharedCockroachNode.acquire(
        leaseName = leaseName,
        clock = SystemClock(),
        fixtureControl = null,
        leaseOwner = sessionOwner,
        fixtureBuildRuleCacheEntries = fixtureBuildRuleCacheEntries,
        invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive =
            invalidateFixtureBuildRuleCacheEntriesWhileOwnerLive,
    )
}
