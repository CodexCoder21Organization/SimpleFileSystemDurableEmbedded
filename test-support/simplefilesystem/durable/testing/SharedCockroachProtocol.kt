package simplefilesystem.durable.testing

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.Path
import java.security.MessageDigest
import java.time.Instant
import java.util.Properties

internal const val SHARED_COCKROACH_PROTOCOL_VERSION = "2"
// SHA-256 cache keys for buildCockroachTestFixtureFatJar() and its protocol-scenario wrapper.
// The build script computes the same keys from their fully-qualified invocation strings.
internal val SHARED_COCKROACH_FIXTURE_BUILD_RULE_CACHE_KEYS = setOf(
    "6c6010b439cafd36cf71e51b5b01af46a2e46dcd791949f8853ef9463528c7d1",
    "258e67621f998da08dde3db4ad475dc29bf45d8b85bf17451ba07ec7424b4f16",
)
internal const val SHARED_COCKROACH_STARTUP_TIMEOUT_MILLIS = 120_000L
internal const val SHARED_COCKROACH_HEARTBEAT_STALE_MILLIS = 120_000L
internal const val SHARED_COCKROACH_PROCESS_STOP_SECONDS = 5L
internal val SHARED_COCKROACH_CHILD_JVM_ARGUMENTS = listOf(
    "-XX:+UseSerialGC",
    "-XX:ActiveProcessorCount=1",
    "-XX:TieredStopAtLevel=1",
)

internal fun sharedCockroachStateDirectory(): File {
    val workspace = File(System.getProperty("user.dir")).canonicalFile
    val digest = MessageDigest.getInstance("SHA-256")
        .digest(workspace.absolutePath.toByteArray(StandardCharsets.UTF_8))
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
        .take(16)
    return File(
        System.getProperty("java.io.tmpdir"),
        "simplefilesystem-durable-shared-cockroach-v$SHARED_COCKROACH_PROTOCOL_VERSION-$digest",
    )
}

internal data class SharedProcessIdentity(
    val pid: Long,
    val startedAt: Instant,
) {
    fun liveHandle(): ProcessHandle? {
        val handle = ProcessHandle.of(pid).orElse(null) ?: return null
        if (!handle.isAlive || handle.info().startInstant().orElse(null) != startedAt) return null
        val procStat = File("/proc/$pid/stat")
        if (procStat.isFile) {
            val stat = try {
                procStat.readText()
            } catch (failure: Exception) {
                if (!handle.isAlive) return null
                throw IllegalStateException(
                    "Could not verify Linux process state for PID $pid started at $startedAt " +
                        "from ${procStat.absolutePath}: ${failure.message}",
                    failure,
                )
            }
            val commandEnd = stat.lastIndexOf(") ")
            check(commandEnd >= 0 && commandEnd + 2 < stat.length) {
                "Linux process state ${procStat.absolutePath} had an unrecognised value '$stat'."
            }
            if (stat[commandEnd + 2] == 'Z') return null
        }
        return handle
    }
}

internal data class SharedCockroachOwnerClaim(
    val token: String,
    val electionOwner: SharedProcessIdentity,
    val createdAtMillis: Long,
    val attachDeadlineMillis: Long,
    val startupDeadlineMillis: Long,
    val workDirectory: File,
    val daemon: SharedProcessIdentity?,
    val daemonProcessGroupId: Long?,
)

internal data class SharedCockroachNodeRecord(
    val token: String,
    val cockroach: SharedProcessIdentity,
    val processGroupId: Long,
    val daemon: SharedProcessIdentity,
    val jdbcUrl: String,
    val workDirectory: File,
)

internal data class SharedCockroachHeartbeat(
    val token: String,
    val daemon: SharedProcessIdentity,
    val writtenAtMillis: Long,
)

internal data class SharedCockroachStartupFailure(
    val token: String,
    val daemon: SharedProcessIdentity,
    val message: String,
)

internal fun processIdentity(handle: ProcessHandle, description: String): SharedProcessIdentity {
    val startedAt = requireNotNull(handle.info().startInstant().orElse(null)) {
        "The $description ${handle.pid()} did not expose its process start time."
    }
    return SharedProcessIdentity(handle.pid(), startedAt)
}

internal fun processGroupId(identity: SharedProcessIdentity, description: String): Long {
    check(identity.liveHandle() != null) {
        "Cannot inspect the $description process group because PID ${identity.pid} started at " +
            "${identity.startedAt} is not alive."
    }
    val statFile = File("/proc/${identity.pid}/stat")
    val stat = try {
        statFile.readText()
    } catch (failure: Exception) {
        throw IllegalStateException(
            "Could not read the $description process group for PID ${identity.pid} started at " +
                "${identity.startedAt} from ${statFile.absolutePath}: ${failure.message}",
            failure,
        )
    }
    val commandEnd = stat.lastIndexOf(") ")
    check(commandEnd >= 0) {
        "Linux process state ${statFile.absolutePath} had an unrecognised value '$stat'."
    }
    val fieldsAfterCommand = stat.substring(commandEnd + 2).trim().split(Regex("\\s+"))
    check(fieldsAfterCommand.size > 2) {
        "Linux process state ${statFile.absolutePath} did not contain a process-group field: '$stat'."
    }
    return fieldsAfterCommand[2].toLongOrNull() ?: throw IllegalStateException(
        "Linux process state ${statFile.absolutePath} contained non-numeric process group " +
            "'${fieldsAfterCommand[2]}'.",
    )
}

internal fun requireManagedWorkDirectory(stateDirectory: File, workDirectory: File): File {
    val managedRoot = stateDirectory.canonicalFile
    val canonical = workDirectory.canonicalFile
    require(canonical.parentFile == managedRoot && canonical.name.startsWith("node-")) {
        "Shared CockroachDB work directory must be a node-* child of " +
            "${managedRoot.absolutePath}, but was ${workDirectory.absolutePath}."
    }
    return canonical
}

internal fun loadVersionedProperties(file: File, description: String): Properties? {
    if (!file.isFile) return null
    val properties = try {
        Properties().apply {
            file.inputStream().use(::load)
        }
    } catch (failure: Exception) {
        throw IllegalStateException(
            "Could not read $description at ${file.absolutePath}: ${failure.message}",
            failure,
        )
    }
    val found = properties.getProperty("protocolVersion") ?: "<missing>"
    if (found != SHARED_COCKROACH_PROTOCOL_VERSION) {
        throw IllegalStateException(
            "Cannot use $description at ${file.absolutePath}: found shared CockroachDB protocol " +
                "version '$found', but this fixture requires version " +
                "'$SHARED_COCKROACH_PROTOCOL_VERSION'. The record was left unchanged.",
        )
    }
    return properties
}

internal fun versionedProperties(): Properties = Properties().apply {
    setProperty("protocolVersion", SHARED_COCKROACH_PROTOCOL_VERSION)
}

internal fun fixtureBuildRuleCacheEntries(properties: Properties, leaseFile: File): List<File> =
    properties.stringPropertyNames()
        .filter { it.startsWith("fixtureBuildRuleCacheEntry.") }
        .sorted()
        .map { propertyName ->
            val configuredPath = properties.getProperty(propertyName)
            val configured = File(configuredPath)
            require(configured.isAbsolute) {
                "Shared CockroachDB lease ${leaseFile.absolutePath} contained relative fixture " +
                    "build-rule cache entry '$configuredPath'."
            }
            val canonical = configured.canonicalFile
            require(
                canonical.name.endsWith(".json") &&
                    canonical.name.removeSuffix(".json") in
                    SHARED_COCKROACH_FIXTURE_BUILD_RULE_CACHE_KEYS &&
                    canonical.parentFile?.name == "buildRuleResultIndex",
            ) {
                "Shared CockroachDB lease ${leaseFile.absolutePath} contained fixture build-rule " +
                    "cache entry '${canonical.absolutePath}', but expected a known fixture rule " +
                    "entry in a buildRuleResultIndex directory."
            }
            canonical
        }

internal fun invalidateFixtureBuildRuleCacheEntry(properties: Properties, leaseFile: File) {
    fixtureBuildRuleCacheEntries(properties, leaseFile).forEach { cacheEntry ->
        deleteIfPresent(
            cacheEntry,
            "stale Kompile session shared CockroachDB fixture build-rule cache entry",
        )
    }
}

internal fun writePropertiesAtomically(file: File, properties: Properties) {
    writePropertiesAtomically(
        file.toPath(),
        properties,
        file.parentFile.toPath(),
    )
}

internal fun writePropertiesAtomically(
    file: Path,
    properties: Properties,
    stagingDirectory: Path,
) {
    require(
        properties.getProperty("protocolVersion") == SHARED_COCKROACH_PROTOCOL_VERSION,
    ) {
        "Cannot write shared CockroachDB record ${file.toAbsolutePath()}: protocolVersion was " +
            "'${properties.getProperty("protocolVersion")}', but expected " +
            "'$SHARED_COCKROACH_PROTOCOL_VERSION'."
    }
    val stagingFile = stagingDirectory.resolve(
        "${file.fileName}.${ProcessHandle.current().pid()}.part",
    )
    Files.newOutputStream(stagingFile).use { properties.store(it, null) }
    try {
        Files.move(
            stagingFile,
            file,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
    } catch (failure: AtomicMoveNotSupportedException) {
        val publicationFailure = IllegalStateException(
            "Cannot atomically publish shared CockroachDB record ${file.toAbsolutePath()} from " +
                "staging path ${stagingFile.toAbsolutePath()}: the filesystem does not support " +
                "the required ATOMIC_MOVE operation. The record was not published because " +
                "recovery depends on atomic ownership records.",
            failure,
        )
        try {
            Files.deleteIfExists(stagingFile)
        } catch (cleanupFailure: Throwable) {
            publicationFailure.addSuppressed(cleanupFailure)
        }
        throw publicationFailure
    }
}

internal fun isAtomicStagingFile(file: File): Boolean =
    file.name.endsWith(".part") &&
        file.name.substringBeforeLast(".part").substringAfterLast('.').toLongOrNull() != null

internal fun writeIdentity(
    file: File,
    identity: SharedProcessIdentity,
    token: String,
    processGroupId: Long? = null,
) {
    writePropertiesAtomically(
        file,
        versionedProperties().apply {
            setProperty("token", token)
            setProperty("pid", identity.pid.toString())
            setProperty("startedAtMillis", identity.startedAt.toEpochMilli().toString())
            processGroupId?.let { setProperty("processGroupId", it.toString()) }
        },
    )
}

internal fun readIdentity(
    file: File,
    description: String,
    expectedToken: String? = null,
): SharedProcessIdentity? {
    val properties = loadVersionedProperties(file, description) ?: return null
    val token = properties.getProperty("token") ?: throw IllegalStateException(
        "$description at ${file.absolutePath} did not contain token.",
    )
    if (expectedToken != null && token != expectedToken) {
        throw IllegalStateException(
            "$description at ${file.absolutePath} contained token '$token', but the live owner " +
                "claim contains token '$expectedToken'.",
        )
    }
    return parseIdentity(properties, "pid", "startedAtMillis", description, file)
}

internal fun parseIdentity(
    properties: Properties,
    pidKey: String,
    startedAtKey: String,
    description: String,
    file: File,
): SharedProcessIdentity {
    val pidValue = properties.getProperty(pidKey) ?: throw IllegalStateException(
        "$description at ${file.absolutePath} did not contain $pidKey.",
    )
    val startedAtValue = properties.getProperty(startedAtKey) ?: throw IllegalStateException(
        "$description at ${file.absolutePath} contained $pidKey='$pidValue' without $startedAtKey.",
    )
    val pid = pidValue.toLongOrNull() ?: throw IllegalStateException(
        "$description at ${file.absolutePath} contained non-numeric $pidKey='$pidValue'.",
    )
    val startedAtMillis = startedAtValue.toLongOrNull() ?: throw IllegalStateException(
        "$description at ${file.absolutePath} contained non-numeric " +
            "$startedAtKey='$startedAtValue'.",
    )
    return SharedProcessIdentity(pid, Instant.ofEpochMilli(startedAtMillis))
}

internal fun deleteIfPresent(file: File, description: String) {
    if (file.exists() && !file.delete()) {
        throw IllegalStateException("Could not delete $description at ${file.absolutePath}.")
    }
}
