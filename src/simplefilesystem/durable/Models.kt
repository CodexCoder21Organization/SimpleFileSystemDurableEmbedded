package simplefilesystem.durable

import simplefilesystem.FileEntryInfo
import simplefilesystem.FileEntryPage
import simplefilesystem.FileEntryType
import simplefilesystem.FileMetadataInfo
import simplefilesystem.FilesystemInfo
import simplefilesystem.FilesystemPage
import simplefilesystem.PathWatchEvent
import simplefilesystem.PathWatchPage
import simplefilesystem.PathWatchTerminalReason
import sql.DatabaseRow
import java.util.UUID

internal const val BLOCK_SIZE_BYTES: Int = 4 * 1024 * 1024
internal const val BLOB_PIN_OWNER: String = "simplefilesystem-durable-embedded"

data class MaintenanceResult(
    val reapedSessions: Int,
    val purgedFilesystems: Int,
    val resolvedGcIntents: Int,
)

internal data class FilesystemRecord(
    val uuid: UUID,
    val description: String,
    val owner: String?,
    val maxSizeBytes: Long,
    val usedBytes: Long,
    val expiresAtMillis: Long?,
    val createdAtMillis: Long,
    val namespaceRevision: Long,
)

internal data class EntryRecord(
    val filesystemUuid: UUID,
    val path: String,
    val parentPath: String,
    val name: String,
    val kind: String,
    val generationUuid: UUID?,
    val sizeBytes: Long?,
    val contentHash: String?,
    val createdAtMillis: Long?,
    val modifiedAtMillis: Long?,
) {
    val isDirectory: Boolean get() = kind == "DIRECTORY"
    val isFile: Boolean get() = kind == "FILE"
}

internal data class BlockRecord(
    val generationUuid: UUID,
    val ordinal: Int,
    val blobHash: String,
    val sizeBytes: Int,
    val referenceCount: Long,
)

internal data class StagedGeneration(
    val sessionUuid: UUID,
    val filesystemUuid: UUID,
    val path: String,
    val expectedHash: String?,
    val sizeBytes: Long,
    val contentHash: String,
)

internal data class FileGenerationSnapshot(
    val entry: EntryRecord,
    val generationUuid: UUID,
    val readerUuid: UUID,
)

internal data class NamespaceEventStreamRecord(
    val filesystemUuid: UUID,
    val latestRevision: Long,
    val oldestAvailableSinceRevision: Long,
    val terminalReason: PathWatchTerminalReason?,
)

internal data class NamespaceEventRecord(
    val filesystemUuid: UUID,
    val revision: Long,
    val canonicalPath: String,
    val entryType: FileEntryType?,
    val exists: Boolean,
    val isDirectory: Boolean,
    val isRegularFile: Boolean,
    val sizeBytes: Long?,
    val contentHash: String?,
    val terminalReason: PathWatchTerminalReason?,
)

internal data class FilesystemInfoValue(
    override val uuid: String,
    override val description: String,
    override val owner: String?,
    override val maxSizeBytes: Long,
    override val usedBytes: Long,
    override val expiresAtMillis: Long?,
    override val createdAtMillis: Long,
) : FilesystemInfo

internal data class FileEntryInfoValue(
    override val name: String,
    override val path: String,
    override val type: FileEntryType,
    override val isDirectory: Boolean,
    override val isRegularFile: Boolean,
    override val size: Long?,
    override val lastModifiedAtMillis: Long?,
) : FileEntryInfo

internal data class FileMetadataInfoValue(
    override val type: FileEntryType,
    override val isRegularFile: Boolean,
    override val isDirectory: Boolean,
    override val symlinkTarget: String? = null,
    override val size: Long?,
    override val createdAtMillis: Long?,
    override val lastModifiedAtMillis: Long?,
    override val lastAccessedAtMillis: Long? = null,
    override val contentHash: String?,
) : FileMetadataInfo

internal data class FileEntryPageValue(
    override val entries: List<FileEntryInfo>,
    override val nextAfter: String?,
    override val snapshotRevision: Long,
) : FileEntryPage

internal data class FilesystemPageValue(
    override val filesystems: List<FilesystemInfo>,
    override val nextAfter: String?,
    override val snapshotRevision: Long,
) : FilesystemPage

internal data class PathWatchEventValue(
    override val uuid: String,
    override val path: String,
    override val revision: Long,
    override val entryType: FileEntryType?,
    override val exists: Boolean,
    override val isDirectory: Boolean,
    override val isRegularFile: Boolean,
    override val size: Long?,
    override val contentHash: String?,
    override val terminalReason: PathWatchTerminalReason?,
) : PathWatchEvent

internal data class PathWatchPageValue(
    override val events: List<PathWatchEvent>,
    override val nextSinceRevision: Long,
    override val hasMore: Boolean,
    override val latestRevision: Long,
) : PathWatchPage

internal fun DatabaseRow.uuidValue(column: String): UUID = when (val value = results[column]) {
    is UUID -> value
    is String -> UUID.fromString(value)
    else -> error("Column '$column' was expected to contain a UUID, but contained '$value'.")
}

internal fun DatabaseRow.nullableUuidValue(column: String): UUID? = when (val value = results[column]) {
    null -> null
    is UUID -> value
    is String -> UUID.fromString(value)
    else -> error("Column '$column' was expected to contain a nullable UUID, but contained '$value'.")
}

internal fun DatabaseRow.stringValue(column: String): String = results[column] as? String
    ?: error("Column '$column' was expected to contain a string, but contained '${results[column]}'.")

internal fun DatabaseRow.nullableStringValue(column: String): String? = results[column] as? String

internal fun DatabaseRow.longValue(column: String): Long = (results[column] as? Number)?.toLong()
    ?: error("Column '$column' was expected to contain a long, but contained '${results[column]}'.")

internal fun DatabaseRow.nullableLongValue(column: String): Long? = (results[column] as? Number)?.toLong()

internal fun DatabaseRow.intValue(column: String): Int = (results[column] as? Number)?.toInt()
    ?: error("Column '$column' was expected to contain an integer, but contained '${results[column]}'.")

internal fun DatabaseRow.booleanValue(column: String): Boolean = results[column] as? Boolean
    ?: error("Column '$column' was expected to contain a boolean, but contained '${results[column]}'.")

internal fun DatabaseRow.toFilesystemRecord(): FilesystemRecord = FilesystemRecord(
    uuid = uuidValue("uuid"),
    description = stringValue("description"),
    owner = nullableStringValue("owner"),
    maxSizeBytes = longValue("max_size_bytes"),
    usedBytes = longValue("used_bytes"),
    expiresAtMillis = nullableLongValue("expires_at_millis"),
    createdAtMillis = longValue("created_at_millis"),
    namespaceRevision = longValue("namespace_revision"),
)

internal fun DatabaseRow.toEntryRecord(): EntryRecord = EntryRecord(
    filesystemUuid = uuidValue("filesystem_uuid"),
    path = stringValue("path"),
    parentPath = stringValue("parent_path"),
    name = stringValue("name"),
    kind = stringValue("entry_kind"),
    generationUuid = nullableUuidValue("generation_uuid"),
    sizeBytes = nullableLongValue("size_bytes"),
    contentHash = nullableStringValue("content_hash"),
    createdAtMillis = nullableLongValue("created_at_millis"),
    modifiedAtMillis = nullableLongValue("modified_at_millis"),
)

internal fun DatabaseRow.toBlockRecord(): BlockRecord = BlockRecord(
    generationUuid = uuidValue("generation_uuid"),
    ordinal = intValue("ordinal"),
    blobHash = stringValue("blob_hash"),
    sizeBytes = intValue("size_bytes"),
    referenceCount = longValue("reference_count"),
)

internal fun DatabaseRow.toNamespaceEventStreamRecord(): NamespaceEventStreamRecord = NamespaceEventStreamRecord(
    filesystemUuid = uuidValue("filesystem_uuid"),
    latestRevision = longValue("latest_revision"),
    oldestAvailableSinceRevision = longValue("oldest_available_since_revision"),
    terminalReason = nullableStringValue("terminal_reason")?.let(PathWatchTerminalReason::valueOf),
)

internal fun DatabaseRow.toNamespaceEventRecord(): NamespaceEventRecord = NamespaceEventRecord(
    filesystemUuid = uuidValue("filesystem_uuid"),
    revision = longValue("revision"),
    canonicalPath = stringValue("canonical_path"),
    entryType = nullableStringValue("entry_type")?.let(FileEntryType::valueOf),
    exists = booleanValue("exists"),
    isDirectory = booleanValue("is_directory"),
    isRegularFile = booleanValue("is_regular_file"),
    sizeBytes = nullableLongValue("size_bytes"),
    contentHash = nullableStringValue("content_hash"),
    terminalReason = nullableStringValue("terminal_reason")?.let(PathWatchTerminalReason::valueOf),
)

internal fun FilesystemRecord.toInfo(): FilesystemInfo = FilesystemInfoValue(
    uuid = uuid.toString(),
    description = description,
    owner = owner,
    maxSizeBytes = maxSizeBytes,
    usedBytes = usedBytes,
    expiresAtMillis = expiresAtMillis,
    createdAtMillis = createdAtMillis,
)

internal fun EntryRecord.toInfo(): FileEntryInfo = FileEntryInfoValue(
    name = name,
    path = path,
    type = entryType,
    isDirectory = isDirectory,
    isRegularFile = isFile,
    size = sizeBytes,
    lastModifiedAtMillis = modifiedAtMillis,
)

internal fun EntryRecord.toMetadata(): FileMetadataInfo = FileMetadataInfoValue(
    type = entryType,
    isRegularFile = isFile,
    isDirectory = isDirectory,
    size = sizeBytes,
    createdAtMillis = createdAtMillis,
    lastModifiedAtMillis = modifiedAtMillis,
    contentHash = contentHash,
)

internal val EntryRecord.entryType: FileEntryType
    get() = when (kind) {
        "FILE" -> FileEntryType.REGULAR_FILE
        "DIRECTORY" -> FileEntryType.DIRECTORY
        else -> FileEntryType.OTHER
    }

internal fun NamespaceEventRecord.toWatchEvent(): PathWatchEvent = PathWatchEventValue(
    uuid = filesystemUuid.toString(),
    path = canonicalPath,
    revision = revision,
    entryType = entryType,
    exists = exists,
    isDirectory = isDirectory,
    isRegularFile = isRegularFile,
    size = sizeBytes,
    contentHash = contentHash,
    terminalReason = terminalReason,
)
