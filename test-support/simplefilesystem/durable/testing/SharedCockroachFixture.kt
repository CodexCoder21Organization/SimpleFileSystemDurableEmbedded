package simplefilesystem.durable.testing

import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Instant
import java.util.Properties

internal const val SHARED_COCKROACH_FIXTURE_PROTOCOL = "single-owner-v1"

data class SharedCockroachFixtureDiagnostics(
    val protocolVersion: String,
    val stateDirectory: File,
    val lockFile: File,
    val sessionOwnerPid: Long,
    val sessionOwnerStartedAtMillis: Long,
    val fixtureOwnerPid: Long,
    val fixtureOwnerStartedAtMillis: Long,
    val cockroachPid: Long,
    val cockroachStartedAtMillis: Long,
)

internal data class SharedProcessIdentity(
    val pid: Long,
    val startedAtMillis: Long,
) {
    fun liveHandle(): ProcessHandle? {
        val handle = ProcessHandle.of(pid).orElse(null) ?: return null
        val actualStartedAt = handle.info().startInstant().orElse(null)?.toEpochMilli() ?: return null
        return handle.takeIf { it.isAlive && actualStartedAt == startedAtMillis }
    }
}

internal data class SharedCockroachFixtureRecord(
    val workspacePath: String,
    val sessionOwner: SharedProcessIdentity,
    val fixtureOwner: SharedProcessIdentity,
    val cockroach: SharedProcessIdentity,
    val jdbcUrl: String,
    val workDirectory: File,
) {
    fun toDiagnostics(
        workspace: File = File("."),
    ): SharedCockroachFixtureDiagnostics =
        SharedCockroachFixtureDiagnostics(
            protocolVersion = SHARED_COCKROACH_FIXTURE_PROTOCOL,
            stateDirectory = sharedCockroachStateDirectory(workspace),
            lockFile = sharedCockroachLockFile(workspace),
            sessionOwnerPid = sessionOwner.pid,
            sessionOwnerStartedAtMillis = sessionOwner.startedAtMillis,
            fixtureOwnerPid = fixtureOwner.pid,
            fixtureOwnerStartedAtMillis = fixtureOwner.startedAtMillis,
            cockroachPid = cockroach.pid,
            cockroachStartedAtMillis = cockroach.startedAtMillis,
        )
}

internal fun sharedCockroachStateDirectory(workspace: File = File(".")): File {
    val workspacePath = workspace.canonicalFile.absolutePath
    val digest = MessageDigest.getInstance("SHA-256")
        .digest(workspacePath.toByteArray(Charsets.UTF_8))
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
    return File(
        System.getProperty("java.io.tmpdir"),
        "simplefilesystem-durable-shared-cockroach-v2-${digest.take(24)}",
    )
}

internal fun sharedCockroachLockFile(workspace: File = File(".")): File =
    File(sharedCockroachStateDirectory(workspace), "owner.lock")

internal fun sharedCockroachReadyFile(workspace: File = File(".")): File =
    File(sharedCockroachStateDirectory(workspace), "fixture.properties")

internal fun processIdentity(handle: ProcessHandle, description: String): SharedProcessIdentity {
    val startedAtMillis = handle.info().startInstant().orElse(null)?.toEpochMilli()
        ?: throw IllegalStateException(
            "The $description process ${handle.pid()} did not expose its start time.",
        )
    return SharedProcessIdentity(handle.pid(), startedAtMillis)
}

internal fun readSharedCockroachFixture(workspace: File = File(".")): SharedCockroachFixtureRecord? {
    val file = sharedCockroachReadyFile(workspace)
    if (!file.isFile) return null
    val properties = Properties().apply { file.inputStream().use(::load) }
    val protocol = required(properties, "protocol", file)
    check(protocol == SHARED_COCKROACH_FIXTURE_PROTOCOL) {
        "Shared CockroachDB fixture record ${file.absolutePath} uses protocol '$protocol', but " +
            "this checkout requires '$SHARED_COCKROACH_FIXTURE_PROTOCOL'."
    }
    val workspacePath = required(properties, "workspacePath", file)
    val expectedWorkspacePath = workspace.canonicalFile.absolutePath
    check(workspacePath == expectedWorkspacePath) {
        "Shared CockroachDB fixture record ${file.absolutePath} belongs to workspace " +
            "'$workspacePath', but the current workspace is '$expectedWorkspacePath'."
    }
    val stateDirectory = sharedCockroachStateDirectory(workspace)
    val workDirectory = File(required(properties, "workDirectory", file)).canonicalFile
    check(workDirectory.toPath().startsWith(stateDirectory.canonicalFile.toPath())) {
        "Shared CockroachDB fixture record ${file.absolutePath} contains work directory " +
            "${workDirectory.absolutePath}, which is outside ${stateDirectory.absolutePath}."
    }
    return SharedCockroachFixtureRecord(
        workspacePath = workspacePath,
        sessionOwner = readIdentity(properties, "sessionOwner", file),
        fixtureOwner = readIdentity(properties, "fixtureOwner", file),
        cockroach = readIdentity(properties, "cockroach", file),
        jdbcUrl = required(properties, "jdbcUrl", file),
        workDirectory = workDirectory,
    )
}

internal fun readLiveSharedCockroachFixtureOrNull(
    workspace: File = File("."),
): SharedCockroachFixtureRecord? {
    val file = sharedCockroachReadyFile(workspace)
    val record = readSharedCockroachFixture(workspace) ?: return null
    check(record.sessionOwner.liveHandle() != null) {
        "The shared CockroachDB fixture at ${file.absolutePath} belongs to session process " +
            "${record.sessionOwner.pid}, but that exact process is no longer live. The next build " +
            "phase must start a new fixture before test dispatch."
    }
    check(record.fixtureOwner.liveHandle() != null) {
        "The shared CockroachDB fixture owner ${record.fixtureOwner.pid} recorded at " +
            "${file.absolutePath} is no longer live. The next build phase must replace it before " +
            "test dispatch."
    }
    check(record.cockroach.liveHandle() != null) {
        "The shared CockroachDB process ${record.cockroach.pid} recorded at ${file.absolutePath} " +
            "is no longer live. The next build phase must replace it before test dispatch."
    }
    return record
}

internal fun writeSharedCockroachFixture(record: SharedCockroachFixtureRecord) {
    val properties = Properties().apply {
        setProperty("protocol", SHARED_COCKROACH_FIXTURE_PROTOCOL)
        setProperty("workspacePath", record.workspacePath)
        writeIdentity(this, "sessionOwner", record.sessionOwner)
        writeIdentity(this, "fixtureOwner", record.fixtureOwner)
        writeIdentity(this, "cockroach", record.cockroach)
        setProperty("jdbcUrl", record.jdbcUrl)
        setProperty("workDirectory", record.workDirectory.absolutePath)
    }
    writePropertiesAtomically(sharedCockroachReadyFile(), properties)
}

internal fun deleteSharedCockroachFixtureIfOwnedBy(owner: SharedProcessIdentity) {
    val record = runCatching(::readSharedCockroachFixture).getOrNull() ?: return
    if (record.fixtureOwner == owner) Files.deleteIfExists(sharedCockroachReadyFile().toPath())
}

internal fun writePropertiesAtomically(target: File, properties: Properties) {
    target.parentFile?.let { parent ->
        check(parent.isDirectory || parent.mkdirs()) {
            "Could not create shared CockroachDB fixture directory ${parent.absolutePath}."
        }
    }
    val staging = File(target.parentFile, ".${target.name}.${ProcessHandle.current().pid()}.part")
    try {
        staging.outputStream().use { properties.store(it, null) }
        try {
            Files.move(
                staging.toPath(),
                target.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(staging.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    } finally {
        Files.deleteIfExists(staging.toPath())
    }
}

internal fun writeRawTextAtomically(target: File, text: String) {
    target.parentFile?.let { parent ->
        check(parent.isDirectory || parent.mkdirs()) {
            "Could not create shared CockroachDB fixture directory ${parent.absolutePath}."
        }
    }
    val staging = File(target.parentFile, ".${target.name}.${ProcessHandle.current().pid()}.part")
    try {
        staging.writeText(text)
        try {
            Files.move(
                staging.toPath(),
                target.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(staging.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    } finally {
        Files.deleteIfExists(staging.toPath())
    }
}

private fun readIdentity(properties: Properties, prefix: String, file: File): SharedProcessIdentity =
    SharedProcessIdentity(
        pid = requiredLong(properties, "${prefix}Pid", file),
        startedAtMillis = requiredLong(properties, "${prefix}StartedAtMillis", file),
    )

private fun writeIdentity(
    properties: Properties,
    prefix: String,
    identity: SharedProcessIdentity,
) {
    properties.setProperty("${prefix}Pid", identity.pid.toString())
    properties.setProperty("${prefix}StartedAtMillis", identity.startedAtMillis.toString())
}

private fun required(properties: Properties, key: String, file: File): String =
    properties.getProperty(key) ?: throw IllegalStateException(
        "Shared CockroachDB fixture record ${file.absolutePath} does not contain '$key'.",
    )

private fun requiredLong(properties: Properties, key: String, file: File): Long {
    val value = required(properties, key, file)
    return value.toLongOrNull() ?: throw IllegalStateException(
        "Shared CockroachDB fixture record ${file.absolutePath} contains non-numeric " +
            "'$key=$value'.",
    )
}
