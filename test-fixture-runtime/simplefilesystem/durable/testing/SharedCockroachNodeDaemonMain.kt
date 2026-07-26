package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.time.Instant
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val NODE_STARTUP_TIMEOUT_MILLIS = 120_000L

fun main(args: Array<String>) {
    require(args.size == 2) {
        "SharedCockroachNodeDaemonMain requires <state-directory> <work-directory>, " +
            "but received ${args.size} argument(s)."
    }
    val stateDirectory = File(args[0]).canonicalFile
    val workDirectory = File(args[1]).canonicalFile
    require(workDirectory.parentFile == stateDirectory && workDirectory.name.startsWith("node-")) {
        "Shared CockroachDB daemon work directory must be a node-* child of " +
            "${stateDirectory.absolutePath}, but was ${workDirectory.absolutePath}."
    }
    check(workDirectory.isDirectory || workDirectory.mkdirs()) {
        "Could not create shared CockroachDB daemon work directory ${workDirectory.absolutePath}."
    }

    val daemonIdentity = processIdentity(ProcessHandle.current(), "shared CockroachDB daemon")
    writeIdentity(File(workDirectory, "daemon.properties"), daemonIdentity)
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
    try {
        cockroach = adoptCockroach(workDirectory) ?: startCockroach(workDirectory)
        configureSingleNodeTestCluster(cockroach.jdbcUrl)
        warmUpDurableSchema(cockroach.jdbcUrl)
        publishReadyState(
            stateDirectory = stateDirectory,
            workDirectory = workDirectory,
            daemonIdentity = daemonIdentity,
            cockroach = cockroach,
        )
        CountDownLatch(1).await()
    } finally {
        cleanupCockroach()
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
        } catch (_: IllegalStateException) {
            // The JVM is already shutting down, so the registered hook owns final cleanup.
        }
    }
}

private fun adoptCockroach(workDirectory: File): ManagedCockroachProcess? {
    val identity = readIdentity(File(workDirectory, "cockroach.properties")) ?: return null
    val handle = ProcessHandle.of(identity.pid).orElse(null) ?: return null
    if (!identity.matches(handle)) return null
    val listeningUrlFile = File(workDirectory, "listening-url")
    if (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) return null
    val jdbcUrl = daemonListeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
    if (!canConnect(jdbcUrl)) return null
    return ManagedCockroachProcess(handle, identity, jdbcUrl, File(workDirectory, "cockroach.out"))
}

private fun startCockroach(workDirectory: File): ManagedCockroachProcess {
    listOf("listening-url", "cockroach.pid", "cockroach.properties").forEach { name ->
        val file = File(workDirectory, name)
        if (file.exists() && !file.delete()) {
            throw IllegalStateException(
                "Could not delete stale shared CockroachDB startup file ${file.absolutePath}.",
            )
        }
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
        .directory(workDirectory)
        .redirectOutput(logFile)
        .redirectErrorStream(true)
        .start()
    val identity = processIdentity(process.toHandle(), "shared CockroachDB process")
    writeIdentity(File(workDirectory, "cockroach.properties"), identity)
    process.onExit().thenRun { watcher.close() }
    try {
        val deadlineNanos = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(NODE_STARTUP_TIMEOUT_MILLIS)
        while (!listeningUrlFile.isFile || listeningUrlFile.length() == 0L) {
            val remainingNanos = deadlineNanos - System.nanoTime()
            if (remainingNanos <= 0L) {
                throw IllegalStateException(
                    "The shared CockroachDB node did not become ready within " +
                        "$NODE_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            }
            try {
                val key = watcher.poll(remainingNanos, TimeUnit.NANOSECONDS)
                    ?: throw IllegalStateException(
                        "The shared CockroachDB node did not become ready within " +
                            "$NODE_STARTUP_TIMEOUT_MILLIS milliseconds; output:\n" +
                            logFile.takeIf(File::isFile)?.readText().orEmpty(),
                    )
                key.pollEvents()
                check(key.reset()) {
                    "Could not continue watching ${workDirectory.absolutePath} for CockroachDB readiness."
                }
            } catch (_: ClosedWatchServiceException) {
                throw IllegalStateException(
                    "The shared CockroachDB node exited with code ${process.exitValue()} " +
                        "before becoming ready; output:\n" +
                        logFile.takeIf(File::isFile)?.readText().orEmpty(),
                )
            }
        }
        val jdbcUrl = daemonListeningUrlToJdbcUrl(listeningUrlFile.readText().trim())
        check(canConnect(jdbcUrl)) {
            "The shared CockroachDB node wrote ${listeningUrlFile.absolutePath}, but a JDBC " +
                "connection to $jdbcUrl could not be established."
        }
        return ManagedCockroachProcess(process.toHandle(), identity, jdbcUrl, logFile)
    } catch (failure: Throwable) {
        ManagedCockroachProcess(
            process.toHandle(),
            identity,
            "",
            logFile,
        ).stop()
        throw failure
    } finally {
        watcher.close()
    }
}

private fun configureSingleNodeTestCluster(jdbcUrl: String) {
    DriverManager.getConnection(jdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("SET CLUSTER SETTING kv.range_split.by_load_enabled = false")
        }
    }
}

private fun publishReadyState(
    stateDirectory: File,
    workDirectory: File,
    daemonIdentity: DaemonProcessIdentity,
    cockroach: ManagedCockroachProcess,
) {
    RandomAccessFile(File(stateDirectory, "state.lock"), "rw").use { lockAccess ->
        lockAccess.channel.lock().use {
            val startingFile = File(stateDirectory, "node-starting.properties")
            val starting = loadProperties(startingFile)
            val recordedDaemonPid = starting?.getProperty("daemonPid")?.toLongOrNull()
            val recordedDaemonStartedAt = starting?.getProperty("daemonStartedAtMillis")?.toLongOrNull()
            val recordedWorkDirectory = starting?.getProperty("workDirectory")
            check(
                recordedDaemonPid == daemonIdentity.pid &&
                    recordedDaemonStartedAt == daemonIdentity.startedAt.toEpochMilli() &&
                    recordedWorkDirectory == workDirectory.absolutePath,
            ) {
                "Shared CockroachDB startup ownership changed before daemon ${daemonIdentity.pid} " +
                    "could publish readiness: recorded daemon PID was $recordedDaemonPid, " +
                    "recorded start time was $recordedDaemonStartedAt, and recorded work directory " +
                    "was '$recordedWorkDirectory'."
            }
            val leases = File(stateDirectory, "leases").listFiles().orEmpty().filter(File::isFile)
            check(leases.isNotEmpty()) {
                "Shared CockroachDB daemon ${daemonIdentity.pid} completed startup without a live lease."
            }
            val properties = Properties().apply {
                setProperty("pid", cockroach.identity.pid.toString())
                setProperty(
                    "processStartedAtMillis",
                    cockroach.identity.startedAt.toEpochMilli().toString(),
                )
                setProperty("daemonPid", daemonIdentity.pid.toString())
                setProperty(
                    "daemonStartedAtMillis",
                    daemonIdentity.startedAt.toEpochMilli().toString(),
                )
                setProperty("jdbcUrl", cockroach.jdbcUrl)
                setProperty("workDirectory", workDirectory.absolutePath)
            }
            writePropertiesAtomically(File(stateDirectory, "node.properties"), properties)
            if (startingFile.exists() && !startingFile.delete()) {
                throw IllegalStateException(
                    "Could not delete shared CockroachDB startup state ${startingFile.absolutePath}.",
                )
            }
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

private fun processIdentity(handle: ProcessHandle, description: String): DaemonProcessIdentity {
    val startedAt = requireNotNull(handle.info().startInstant().orElse(null)) {
        "The $description ${handle.pid()} did not expose its start time."
    }
    return DaemonProcessIdentity(handle.pid(), startedAt)
}

private fun writeIdentity(file: File, identity: DaemonProcessIdentity) {
    val properties = Properties().apply {
        setProperty("pid", identity.pid.toString())
        setProperty("startedAtMillis", identity.startedAt.toEpochMilli().toString())
    }
    writePropertiesAtomically(file, properties)
}

private fun readIdentity(file: File): DaemonProcessIdentity? {
    val properties = loadProperties(file) ?: return null
    return try {
        DaemonProcessIdentity(
            pid = requireNotNull(properties.getProperty("pid")).toLong(),
            startedAt = Instant.ofEpochMilli(
                requireNotNull(properties.getProperty("startedAtMillis")).toLong(),
            ),
        )
    } catch (_: Exception) {
        null
    }
}

private fun loadProperties(file: File): Properties? {
    if (!file.isFile) return null
    return try {
        Properties().apply {
            file.inputStream().use(::load)
        }
    } catch (_: Exception) {
        null
    }
}

private fun writePropertiesAtomically(file: File, properties: Properties) {
    val stagingFile = File(file.parentFile, "${file.name}.part")
    stagingFile.outputStream().use { properties.store(it, null) }
    try {
        java.nio.file.Files.move(
            stagingFile.toPath(),
            file.toPath(),
            java.nio.file.StandardCopyOption.ATOMIC_MOVE,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
    } catch (_: java.nio.file.AtomicMoveNotSupportedException) {
        java.nio.file.Files.move(
            stagingFile.toPath(),
            file.toPath(),
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
    }
}

private data class DaemonProcessIdentity(
    val pid: Long,
    val startedAt: Instant,
) {
    fun matches(handle: ProcessHandle): Boolean =
        handle.isAlive && handle.info().startInstant().orElse(null) == startedAt
}

private class ManagedCockroachProcess(
    private val handle: ProcessHandle,
    val identity: DaemonProcessIdentity,
    val jdbcUrl: String,
    private val logFile: File,
) {
    fun stop() {
        handle.destroyForcibly()
        if (handle.isAlive) {
            handle.onExit().join()
        }
        check(!handle.isAlive) {
            "Shared CockroachDB process ${identity.pid} remained alive after forcible shutdown; " +
                "output:\n${logFile.takeIf(File::isFile)?.readText().orEmpty()}"
        }
    }
}
