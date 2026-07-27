package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

private const val HEARTBEAT_INTERVAL_MILLIS = 1_000L
private val daemonProcessLocalStateLock = Any()

fun main(args: Array<String>) {
    require(args.size == 4) {
        "SharedCockroachNodeDaemonMain requires <state-directory> <work-directory> " +
            "<election-token> <control-directory-or-dash>, but received ${args.size} argument(s)."
    }
    val stateDirectory = File(args[0]).canonicalFile
    val workDirectory = requireManagedWorkDirectory(stateDirectory, File(args[1]))
    val token = args[2]
    require(token.isNotBlank()) {
        "Shared CockroachDB daemon election token must not be blank, but was '$token'."
    }
    val controlDirectory = args[3].takeUnless { it == "-" }?.let { File(it).canonicalFile }
    check(workDirectory.isDirectory || workDirectory.mkdirs()) {
        "Could not create shared CockroachDB daemon work directory ${workDirectory.absolutePath}."
    }

    val daemonIdentity = processIdentity(ProcessHandle.current(), "shared CockroachDB daemon")
    val daemonProcessGroupId = processGroupId(daemonIdentity, "shared CockroachDB daemon")
    check(daemonProcessGroupId == daemonIdentity.pid) {
        "Shared CockroachDB daemon ${daemonIdentity.pid} must lead its durable process group, but " +
            "Linux reported processGroupId=$daemonProcessGroupId."
    }
    writeIdentity(
        File(workDirectory, "daemon.properties"),
        daemonIdentity,
        token,
        daemonProcessGroupId,
    )
    recordObservedIdentity(
        controlDirectory,
        "daemon",
        token,
        daemonIdentity,
        daemonProcessGroupId,
    )
    val preSpawnClaim = readOwnerClaim(stateDirectory)
        ?: throw IllegalStateException(
            "Shared CockroachDB daemon ${daemonIdentity.pid} received token '$token', but owner " +
                "claim ${File(stateDirectory, "node-owner.properties").absolutePath} was missing.",
        )
    verifyPreAttachOwnership(preSpawnClaim, token, workDirectory)
    waitAtDaemonBarrier(
        controlDirectory = controlDirectory,
        barrierName = "daemon-before-attach",
        token = token,
        stateDirectory = stateDirectory,
        deadlineMillis = preSpawnClaim.attachDeadlineMillis,
    ) {
        val claim = readOwnerClaim(stateDirectory)
            ?: throw IllegalStateException(
                "Shared CockroachDB daemon ${daemonIdentity.pid} lost owner claim for token " +
                    "'$token' before attachment.",
            )
        verifyPreAttachOwnership(claim, token, workDirectory)
    }
    attachDaemon(
        stateDirectory,
        workDirectory,
        token,
        daemonIdentity,
        daemonProcessGroupId,
    )

    try {
        var cockroach: ManagedCockroachProcess? = null
        val cleanupLock = Any()
        var cleaned = false
        fun cleanupCockroach() = synchronized(cleanupLock) {
            if (cleaned) return@synchronized
            cleaned = true
            cockroach?.stop()
        }
        val shutdownHook = Thread(::cleanupCockroach, "shared-cockroach-node-daemon-shutdown")
        Runtime.getRuntime().addShutdownHook(shutdownHook)
        val lifecycle = CountDownLatch(1)
        val ownershipFailure = AtomicReference<Throwable?>(null)
        val cockroachExitObserved = AtomicBoolean(false)
        val heartbeatExecutor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "shared-cockroach-owner-heartbeat").apply {
                isDaemon = true
            }
        }
        heartbeatExecutor.scheduleAtFixedRate(
            {
                try {
                    val managedCockroach = cockroach
                    if (managedCockroach != null &&
                        !managedCockroach.isAlive() &&
                        cockroachExitObserved.compareAndSet(false, true)
                    ) {
                        abandonHeartbeatAfterCockroachExit(
                            stateDirectory,
                            workDirectory,
                            token,
                            daemonIdentity,
                        )
                        lifecycle.countDown()
                        return@scheduleAtFixedRate
                    }
                    val liveLeaseRemains = renewHeartbeat(
                        stateDirectory,
                        workDirectory,
                        token,
                        daemonIdentity,
                    )
                    if (!liveLeaseRemains) {
                        lifecycle.countDown()
                    }
                } catch (failure: Throwable) {
                    if (ownershipFailure.compareAndSet(null, failure)) {
                        try {
                            cleanupCockroach()
                        } catch (cleanupFailure: Throwable) {
                            failure.addSuppressed(cleanupFailure)
                        }
                        lifecycle.countDown()
                    }
                }
            },
            0L,
            HEARTBEAT_INTERVAL_MILLIS,
            TimeUnit.MILLISECONDS,
        )
        try {
            requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
            cockroach = startCockroach(
                stateDirectory,
                workDirectory,
                token,
                daemonIdentity,
                daemonProcessGroupId,
                controlDirectory,
            )
            val cockroachExit = cockroach.onExit {
                cockroachExitObserved.set(true)
                try {
                    abandonHeartbeatAfterCockroachExit(
                        stateDirectory,
                        workDirectory,
                        token,
                        daemonIdentity,
                    )
                } catch (failure: Throwable) {
                    ownershipFailure.compareAndSet(null, failure)
                } finally {
                    lifecycle.countDown()
                }
            }
            recordObservedIdentity(
                controlDirectory,
                "cockroach",
                token,
                cockroach.identity,
                cockroach.processGroupId,
            )
            waitAtDaemonBarrier(
                controlDirectory = controlDirectory,
                barrierName = "cockroach-before-readiness",
                token = token,
                stateDirectory = stateDirectory,
                deadlineMillis = null,
            ) {
                requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
            }
            requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
            configureSingleNodeTestCluster(cockroach.jdbcUrl)
            warmUpDurableSchema(
                adminJdbcUrl = cockroach.jdbcUrl,
                controlDirectory = controlDirectory,
                token = token,
                ownershipDirectory = stateDirectory,
            ) {
                requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
            }
            requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
            publishReadyState(
                stateDirectory = stateDirectory,
                workDirectory = workDirectory,
                token = token,
                daemonIdentity = daemonIdentity,
                cockroach = cockroach,
            )
            lifecycle.await()
            if (cockroachExit.isCompletedExceptionally) cockroachExit.join()
            ownershipFailure.get()?.let { throw it }
        } finally {
            heartbeatExecutor.shutdownNow()
            cleanupCockroach()
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook)
            } catch (_: IllegalStateException) {
                // The JVM is already shutting down, so the registered hook owns final cleanup.
            }
        }
    } catch (failure: Throwable) {
        try {
            publishStartupFailure(
                stateDirectory,
                workDirectory,
                token,
                daemonIdentity,
                failure,
            )
        } catch (publicationFailure: Throwable) {
            failure.addSuppressed(publicationFailure)
        }
        throw failure
    }
}

private fun abandonHeartbeatAfterCockroachExit(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
) {
    withStateLock(stateDirectory) {
        val claim = readOwnerClaim(stateDirectory) ?: return@withStateLock
        if (claim.token == token &&
            claim.workDirectory == workDirectory &&
            claim.daemon == daemonIdentity
        ) {
            deleteIfPresent(
                File(stateDirectory, "node-heartbeat.properties"),
                "heartbeat for exited shared CockroachDB process",
            )
        }
    }
}

private fun attachDaemon(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
    daemonProcessGroupId: Long,
) {
    withStateLock(stateDirectory) {
        val claim = readOwnerClaim(stateDirectory)
            ?: throw IllegalStateException(
                "Shared CockroachDB daemon ${daemonIdentity.pid} cannot attach token '$token' " +
                    "because the owner claim is missing.",
            )
        verifyPreAttachOwnership(claim, token, workDirectory)
        val attached = claim.copy(
            daemon = daemonIdentity,
            daemonProcessGroupId = daemonProcessGroupId,
        )
        writeOwnerClaim(stateDirectory, attached)
        writeHeartbeat(stateDirectory, token, daemonIdentity)
    }
}

private fun renewHeartbeat(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
): Boolean =
    withStateLock(stateDirectory) {
        requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
        if (!purgeStaleLeasesAndCheckLive(stateDirectory)) {
            deleteIfPresent(
                File(stateDirectory, "node-heartbeat.properties"),
                "heartbeat for shared CockroachDB fixture without live leases",
            )
            return@withStateLock false
        }
        writeHeartbeat(stateDirectory, token, daemonIdentity)
        true
    }

private fun purgeStaleLeasesAndCheckLive(stateDirectory: File): Boolean {
    val leasesDirectory = File(stateDirectory, "leases")
    val failures = mutableListOf<Throwable>()
    var liveLeaseRemains = false
    leasesDirectory.listFiles().orEmpty()
        .filter { it.isFile && isAtomicStagingFile(it) }
        .forEach { stagingFile ->
            try {
                deleteIfPresent(
                    stagingFile,
                    "abandoned shared CockroachDB lease staging file",
                )
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
                val identity = parseIdentity(
                    properties,
                    "pid",
                    "startedAtMillis",
                    "shared CockroachDB lease",
                    lease,
                )
                if (identity.liveHandle() == null) {
                    invalidateFixtureBuildRuleCacheEntry(properties, lease)
                    deleteIfPresent(lease, "stale shared CockroachDB lease")
                } else {
                    liveLeaseRemains = true
                }
            } catch (failure: Throwable) {
                failures += failure
            }
        }
    if (failures.isNotEmpty()) {
        System.err.println(
            "Shared CockroachDB daemon could not verify ${failures.size} durable lease " +
                "record(s); invalid evidence was preserved.",
        )
        failures.forEach(Throwable::printStackTrace)
    }
    return liveLeaseRemains
}

private fun verifyPreAttachOwnership(
    claim: SharedCockroachOwnerClaim,
    token: String,
    workDirectory: File,
) {
    check(claim.token == token) {
        "Shared CockroachDB daemon token '$token' was superseded by owner token '${claim.token}'."
    }
    check(claim.workDirectory == workDirectory) {
        "Shared CockroachDB daemon token '$token' was assigned work directory " +
            "${claim.workDirectory.absolutePath}, not ${workDirectory.absolutePath}."
    }
    check(claim.daemon == null) {
        "Shared CockroachDB daemon token '$token' was already attached to daemon " +
            "${claim.daemon?.pid}."
    }
    check(claim.daemonProcessGroupId == null) {
        "Shared CockroachDB daemon token '$token' was already attached to process group " +
            "${claim.daemonProcessGroupId}."
    }
    check(claim.electionOwner.liveHandle() != null) {
        "Shared CockroachDB daemon token '$token' belongs to dead election owner " +
            "${claim.electionOwner.pid}."
    }
    val now = System.currentTimeMillis()
    check(now <= claim.attachDeadlineMillis) {
        "Shared CockroachDB daemon token '$token' missed its attachment deadline " +
            "${claim.attachDeadlineMillis}; current time was $now."
    }
}

private fun requireAttachedOwnership(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
) {
    val claim = readOwnerClaim(stateDirectory)
        ?: throw IllegalStateException(
            "Shared CockroachDB daemon ${daemonIdentity.pid} lost owner claim for token '$token'.",
        )
    check(claim.token == token) {
        "Shared CockroachDB daemon token '$token' was superseded by owner token '${claim.token}'."
    }
    check(claim.workDirectory == workDirectory) {
        "Shared CockroachDB daemon token '$token' owner claim changed work directory from " +
            "${workDirectory.absolutePath} to ${claim.workDirectory.absolutePath}."
    }
    check(claim.daemon == daemonIdentity) {
        "Shared CockroachDB daemon token '$token' owner claim recorded daemon " +
            "${claim.daemon?.pid} started at ${claim.daemon?.startedAt}, but this daemon is " +
            "${daemonIdentity.pid} started at ${daemonIdentity.startedAt}."
    }
    check(claim.daemonProcessGroupId == daemonIdentity.pid) {
        "Shared CockroachDB daemon token '$token' owner claim recorded process group " +
            "${claim.daemonProcessGroupId}, but daemon ${daemonIdentity.pid} must lead its group."
    }
    val readyFile = File(stateDirectory, "node.properties")
    val readyProperties = loadVersionedProperties(
        readyFile,
        "shared CockroachDB node state",
    )
    val startupCompleted = readyProperties?.let { properties ->
        val readyToken = properties.getProperty("token") ?: throw IllegalStateException(
            "Shared CockroachDB node state ${readyFile.absolutePath} did not contain token.",
        )
        check(readyToken == token) {
            "Shared CockroachDB daemon token '$token' found readiness for token '$readyToken' in " +
                "${readyFile.absolutePath}."
        }
        true
    } ?: false
    if (!startupCompleted) {
        val now = System.currentTimeMillis()
        check(now <= claim.startupDeadlineMillis) {
            "Shared CockroachDB daemon token '$token' missed its absolute startup deadline " +
                "${claim.startupDeadlineMillis}; current time was $now and readiness " +
                "${readyFile.absolutePath} had not been published."
        }
    }
}

private fun startCockroach(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
    daemonProcessGroupId: Long,
    controlDirectory: File?,
): ManagedCockroachProcess {
    listOf("listening-url", "cockroach.pid", "cockroach.properties").forEach { name ->
        deleteIfPresent(File(workDirectory, name), "stale shared CockroachDB startup file")
    }
    val binaryProvider = LocalCockroachCluster()
    val binary = try {
        binaryProvider.binary()
    } finally {
        binaryProvider.close()
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
        .also { builder ->
            builder.environment()["GOMAXPROCS"] = "1"
        }
        .directory(workDirectory)
        .redirectOutput(logFile)
        .redirectErrorStream(true)
        .start()
    val identity = processIdentity(process.toHandle(), "shared CockroachDB process-group leader")
    val childProcessGroupId = processGroupId(identity, "shared CockroachDB process")
    check(childProcessGroupId == daemonProcessGroupId) {
        "Shared CockroachDB child ${identity.pid} escaped durable daemon process group " +
            "$daemonProcessGroupId into processGroupId=$childProcessGroupId."
    }
    waitAtDaemonBarrier(
        controlDirectory = controlDirectory,
        barrierName = "cockroach-after-start-before-identity",
        token = token,
        stateDirectory = stateDirectory,
        deadlineMillis = null,
        arrivalIdentity = identity,
        arrivalProcessGroupId = childProcessGroupId,
    ) {
        requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
    }
    val managed = ManagedCockroachProcess(
        handle = process.toHandle(),
        identity = identity,
        processGroupId = childProcessGroupId,
        jdbcUrl = "",
        logFile = logFile,
    )
    writeIdentity(
        File(workDirectory, "cockroach.properties"),
        identity,
        token,
        childProcessGroupId,
    )
    process.onExit().thenRun { watcher.close() }
    try {
        val deadlineNanos = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS)
        while (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) {
            val remainingNanos = deadlineNanos - System.nanoTime()
            if (remainingNanos <= 0L) {
                throw IllegalStateException(
                    "The shared CockroachDB node did not become ready within " +
                        "$SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            }
            try {
                val key = watcher.poll(remainingNanos, TimeUnit.NANOSECONDS)
                    ?: throw IllegalStateException(
                        "The shared CockroachDB node did not become ready within " +
                            "$SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            logFile.takeIf(File::isFile)?.readText().orEmpty(),
                    )
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${workDirectory.absolutePath} for CockroachDB " +
                        "readiness."
                }
            } catch (_: ClosedWatchServiceException) {
                throw IllegalStateException(
                    "The shared CockroachDB process-group leader ${identity.pid} exited before " +
                        "becoming ready; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            }
        }
        val jdbcUrl = daemonListeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
        check(canConnect(jdbcUrl)) {
            "The shared CockroachDB node wrote ${listeningUrlFile.absolutePath}, but a JDBC " +
                "connection to $jdbcUrl could not be established."
        }
        return managed.copyWithJdbcUrl(jdbcUrl)
    } catch (failure: Throwable) {
        try {
            managed.stop()
        } catch (cleanupFailure: Throwable) {
            failure.addSuppressed(cleanupFailure)
        }
        throw failure
    } finally {
        watcher.close()
    }
}

private fun publishReadyState(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
    cockroach: ManagedCockroachProcess,
) {
    withStateLock(stateDirectory) {
        requireAttachedOwnership(stateDirectory, workDirectory, token, daemonIdentity)
        val leases = File(stateDirectory, "leases").listFiles().orEmpty()
            .filter { it.isFile && !isAtomicStagingFile(it) }
        check(leases.isNotEmpty()) {
            "Shared CockroachDB daemon ${daemonIdentity.pid} completed startup for token '$token' " +
                "without a live lease."
        }
        val readyProperties = versionedProperties().apply {
            setProperty("token", token)
            setProperty("pid", cockroach.identity.pid.toString())
            setProperty(
                "processStartedAtMillis",
                cockroach.identity.startedAt.toEpochMilli().toString(),
            )
            setProperty("processGroupId", cockroach.processGroupId.toString())
            setProperty("daemonPid", daemonIdentity.pid.toString())
            setProperty(
                "daemonStartedAtMillis",
                daemonIdentity.startedAt.toEpochMilli().toString(),
            )
            setProperty("jdbcUrl", cockroach.jdbcUrl)
            setProperty("workDirectory", workDirectory.absolutePath)
        }
        writePropertiesAtomically(
            File(stateDirectory, "node-warmup.properties"),
            readyProperties,
        )
        writePropertiesAtomically(
            File(stateDirectory, "node.properties"),
            readyProperties,
        )
        writeHeartbeat(stateDirectory, token, daemonIdentity)
    }
}

private fun publishStartupFailure(
    stateDirectory: File,
    workDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
    failure: Throwable,
) {
    withStateLock(stateDirectory) {
        val claim = readOwnerClaim(stateDirectory) ?: return@withStateLock
        if (claim.token != token ||
            claim.workDirectory != workDirectory ||
            claim.daemon != daemonIdentity
        ) {
            return@withStateLock
        }
        val readyProperties = loadVersionedProperties(
            File(stateDirectory, "node.properties"),
            "shared CockroachDB node state",
        )
        if (readyProperties?.getProperty("token") == token) return@withStateLock
        deleteIfPresent(
            File(stateDirectory, "node.properties"),
            "failed shared CockroachDB node state",
        )
        deleteIfPresent(
            File(stateDirectory, "node-warmup.properties"),
            "failed shared CockroachDB warmup proof",
        )
        deleteIfPresent(
            File(stateDirectory, "node-heartbeat.properties"),
            "failed shared CockroachDB heartbeat",
        )
        writePropertiesAtomically(
            File(stateDirectory, "node-failure.properties"),
            versionedProperties().apply {
                setProperty("token", token)
                setProperty("daemonPid", daemonIdentity.pid.toString())
                setProperty(
                    "daemonStartedAtMillis",
                    daemonIdentity.startedAt.toEpochMilli().toString(),
                )
                setProperty("daemonProcessGroupId", daemonIdentity.pid.toString())
                setProperty(
                    "message",
                    failure.message ?: failure::class.java.name,
                )
            },
        )
    }
}

private fun readOwnerClaim(stateDirectory: File): SharedCockroachOwnerClaim? {
    val ownerFile = File(stateDirectory, "node-owner.properties")
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
    check(setOf(daemonPid, daemonStartedAt, daemonProcessGroupId).map { it == null }.distinct().size == 1) {
        "Shared CockroachDB owner claim ${ownerFile.absolutePath} must contain daemonPid, " +
            "daemonStartedAtMillis and daemonProcessGroupId together, but daemonPid='$daemonPid', " +
            "daemonStartedAtMillis='$daemonStartedAt' and daemonProcessGroupId=" +
            "'$daemonProcessGroupId'."
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
        daemonProcessGroupId = daemonProcessGroupId?.let {
            requiredLong(properties, "daemonProcessGroupId", ownerFile)
        },
    )
}

private fun writeOwnerClaim(
    stateDirectory: File,
    claim: SharedCockroachOwnerClaim,
) {
    writePropertiesAtomically(
        File(stateDirectory, "node-owner.properties"),
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
                    requireNotNull(claim.daemonProcessGroupId).toString(),
                )
            }
        },
    )
}

private fun writeHeartbeat(
    stateDirectory: File,
    token: String,
    daemonIdentity: SharedProcessIdentity,
) {
    writePropertiesAtomically(
        File(stateDirectory, "node-heartbeat.properties"),
        versionedProperties().apply {
            setProperty("token", token)
            setProperty("daemonPid", daemonIdentity.pid.toString())
            setProperty(
                "daemonStartedAtMillis",
                daemonIdentity.startedAt.toEpochMilli().toString(),
            )
            setProperty("daemonProcessGroupId", daemonIdentity.pid.toString())
            setProperty("writtenAtMillis", System.currentTimeMillis().toString())
        },
    )
}

private fun waitAtDaemonBarrier(
    controlDirectory: File?,
    barrierName: String,
    token: String,
    stateDirectory: File,
    deadlineMillis: Long?,
    arrivalIdentity: SharedProcessIdentity? = null,
    arrivalProcessGroupId: Long? = null,
    verifyOwnership: () -> Unit,
) {
    if (controlDirectory == null || !File(controlDirectory, "pause-$barrierName").isFile) return
    check(controlDirectory.isDirectory || controlDirectory.mkdirs()) {
        "Could not create shared CockroachDB fixture-control directory " +
            controlDirectory.absolutePath
    }
    writePropertiesAtomically(
        File(controlDirectory, "$barrierName-arrived-$token.properties"),
        versionedProperties().apply {
            setProperty("token", token)
            setProperty(
                "pid",
                (arrivalIdentity?.pid ?: ProcessHandle.current().pid()).toString(),
            )
            arrivalIdentity?.let {
                setProperty("startedAtMillis", it.startedAt.toEpochMilli().toString())
            }
            arrivalProcessGroupId?.let {
                setProperty("processGroupId", it.toString())
            }
        },
    )
    val releaseFile = File(controlDirectory, "$barrierName-release-$token")
    FileSystems.getDefault().newWatchService().use { watcher ->
        controlDirectory.toPath().register(
            watcher,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE,
        )
        stateDirectory.toPath().register(
            watcher,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE,
        )
        while (!releaseFile.isFile) {
            verifyOwnership()
            val key = try {
                if (deadlineMillis == null) {
                    watcher.take()
                } else {
                    val remaining = deadlineMillis - System.currentTimeMillis()
                    check(remaining > 0L) {
                        "Shared CockroachDB daemon token '$token' did not leave $barrierName before " +
                            "attachment deadline $deadlineMillis."
                    }
                    watcher.poll(remaining, TimeUnit.MILLISECONDS)
                        ?: throw IllegalStateException(
                            "Shared CockroachDB daemon token '$token' did not leave $barrierName " +
                                "before attachment deadline $deadlineMillis.",
                        )
                }
            } catch (failure: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IllegalStateException(
                    "Interrupted while shared CockroachDB daemon token '$token' waited at " +
                        "$barrierName.",
                    failure,
                )
            }
            key.pollEvents()
            check(key.reset()) {
                "Could not continue watching the shared CockroachDB $barrierName barrier for " +
                    "token '$token'."
            }
        }
    }
    verifyOwnership()
}

private fun recordObservedIdentity(
    controlDirectory: File?,
    processType: String,
    token: String,
    identity: SharedProcessIdentity,
    processGroupId: Long?,
) {
    if (controlDirectory == null) return
    check(controlDirectory.isDirectory || controlDirectory.mkdirs()) {
        "Could not create shared CockroachDB fixture-control directory " +
            controlDirectory.absolutePath
    }
    writePropertiesAtomically(
        File(
            controlDirectory,
            "$processType-$token-${identity.pid}-${identity.startedAt.toEpochMilli()}.properties",
        ),
        versionedProperties().apply {
            setProperty("token", token)
            setProperty("pid", identity.pid.toString())
            setProperty("startedAtMillis", identity.startedAt.toEpochMilli().toString())
            processGroupId?.let { setProperty("processGroupId", it.toString()) }
        },
    )
}

private fun configureSingleNodeTestCluster(jdbcUrl: String) {
    DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("SET CLUSTER SETTING kv.range_split.by_load_enabled = false")
        }
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

private fun daemonListeningUrlToJdbcUrl(listeningUrl: String): String {
    val match = Regex("""^postgresql://[^@]+@([^/]+)/([^?]+)(\?.*)?$""").matchEntire(listeningUrl)
        ?: throw IllegalArgumentException(
            "The CockroachDB listening URL must have the form " +
                "postgresql://user@host:port/database?parameters, but was $listeningUrl",
        )
    return "jdbc:postgresql://${match.groupValues[1]}/${match.groupValues[2]}" +
        match.groupValues[3]
}

private fun requiredLong(properties: Properties, key: String, file: File): Long {
    val value = properties.getProperty(key) ?: throw IllegalStateException(
        "Shared CockroachDB record ${file.absolutePath} did not contain $key.",
    )
    return value.toLongOrNull() ?: throw IllegalStateException(
        "Shared CockroachDB record ${file.absolutePath} contained non-numeric $key='$value'.",
    )
}

private fun <T> withStateLock(stateDirectory: File, block: () -> T): T {
    return synchronized(daemonProcessLocalStateLock) {
        val lockFile = File(stateDirectory, "state.lock")
        RandomAccessFile(lockFile, "rw").use { lockAccess ->
            lockAccess.channel.lock().use { block() }
        }
    }
}

private data class ManagedCockroachProcess(
    private val handle: ProcessHandle,
    val identity: SharedProcessIdentity,
    val processGroupId: Long,
    val jdbcUrl: String,
    private val logFile: File,
) {
    fun copyWithJdbcUrl(jdbcUrl: String): ManagedCockroachProcess = copy(jdbcUrl = jdbcUrl)

    fun onExit(action: () -> Unit): CompletableFuture<Void> = handle.onExit().thenRun(action)

    fun isAlive(): Boolean = identity.liveHandle() != null

    fun stop() {
        val liveHandle = identity.liveHandle() ?: return
        liveHandle.destroyForcibly()
        if (liveHandle.isAlive) {
            try {
                liveHandle.onExit().get(
                    SHARED_COCKROACH_PROCESS_STOP_SECONDS,
                    TimeUnit.SECONDS,
                )
            } catch (failure: TimeoutException) {
                throw IllegalStateException(
                    "Shared CockroachDB process ${identity.pid} in durable process group " +
                        "$processGroupId remained alive after forcible " +
                        "shutdown; output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}",
                    failure,
                )
            } catch (failure: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IllegalStateException(
                    "Interrupted while waiting for shared CockroachDB process group " +
                        "$processGroupId to exit.",
                    failure,
                )
            }
        }
        check(!liveHandle.isAlive) {
            "Shared CockroachDB process ${identity.pid} in durable process group $processGroupId " +
                "remained alive after forcible " +
                "shutdown; output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}"
        }
    }
}
