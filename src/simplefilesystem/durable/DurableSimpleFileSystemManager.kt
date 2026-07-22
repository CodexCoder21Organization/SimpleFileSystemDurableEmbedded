package simplefilesystem.durable

import blobstore.api.BlobstoreService
import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import simplefilesystem.FileContentConflictException
import simplefilesystem.FilesystemExpiredException
import simplefilesystem.FilesystemInfo
import simplefilesystem.FilesystemNotFoundException
import simplefilesystem.InvalidContentHashException
import simplefilesystem.InvalidFilesystemUuidException
import simplefilesystem.InvalidMaxSizeBytesException
import simplefilesystem.InvalidPathException
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import simplefilesystem.QuotaExceededException
import simplefilesystem.SimpleFileSystem
import simplefilesystem.SimpleFileSystemManager
import sql.Database
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.sql.SQLException
import java.util.UUID

class DurableSimpleFileSystemManager(
    internal val blobstoreService: BlobstoreService,
    internal val metadataDatabase: Database,
    internal val clock: Clock = SystemClock(),
) : SimpleFileSystemManager {
    @Volatile
    private var schemaReady: Boolean = false

    override fun createFilesystem(description: String, maxSizeBytes: Long): FilesystemInfo {
        ensureSchema()
        if (maxSizeBytes <= 0L) throw InvalidMaxSizeBytesException(maxSizeBytes)
        val uuid = UUID.randomUUID()
        val now = clock.currentTimeMillis()
        transactionally { transaction ->
            transaction.execute(
                """INSERT INTO filesystems
                    (uuid, description, owner, max_size_bytes, used_bytes, expires_at_millis, created_at_millis)
                    VALUES (?, ?, NULL, ?, 0, NULL, ?)""".trimIndent(),
                uuid,
                description,
                maxSizeBytes,
                now,
            )
            transaction.execute(
                """INSERT INTO entries
                    (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                     size_bytes, content_hash, created_at_millis, modified_at_millis)
                    VALUES (?, '/', '', '', 'DIRECTORY', NULL, NULL, NULL, ?, ?)""".trimIndent(),
                uuid,
                now,
                now,
            )
        }
        return requireFilesystem(uuid).toInfo()
    }

    override fun listFilesystems(): List<FilesystemInfo> {
        ensureSchema()
        return metadataDatabase.getRows("SELECT * FROM filesystems ORDER BY uuid")
            .map { it.toFilesystemRecord().toInfo() }
    }

    override fun getFilesystemInfo(uuid: String): FilesystemInfo = requireFilesystem(parseUuid(uuid)).toInfo()

    override fun openFilesystem(uuid: String): SimpleFileSystem {
        val parsed = parseUuid(uuid)
        requireActiveFilesystem(parsed)
        return DurableSimpleFileSystem(this, parsed)
    }

    override fun deleteFilesystem(uuid: String) {
        val parsed = parseUuid(uuid)
        ensureSchema()
        transactionally { transaction ->
            requireFilesystem(transaction, parsed, lock = true)
            val entries = transaction.getRows(
                "SELECT * FROM entries WHERE filesystem_uuid = ? AND entry_kind = 'FILE' FOR UPDATE",
                parsed,
            ).map { it.toEntryRecord() }
            entries.forEach { entry -> entry.generationUuid?.let { releaseGeneration(transaction, it) } }
            val staged = transaction.getUuids(
                "SELECT session_uuid FROM write_sessions WHERE filesystem_uuid = ?",
                parsed,
            )
            staged.forEach { generation ->
                val hashes = transaction.getStrings(
                    "SELECT blob_hash FROM file_blocks WHERE generation_uuid = ?",
                    generation,
                )
                hashes.forEach { enqueueBlobForGc(transaction, it) }
                transaction.execute("DELETE FROM file_blocks WHERE generation_uuid = ?", generation)
            }
            transaction.execute("DELETE FROM filesystems WHERE uuid = ?", parsed)
        }
    }

    override fun getExpiration(uuid: String): Long? = requireFilesystem(parseUuid(uuid)).expiresAtMillis

    override fun setExpiration(uuid: String, expiresAtMillis: Long?) {
        val parsed = parseUuid(uuid)
        ensureSchema()
        transactionally { transaction ->
            requireFilesystem(transaction, parsed, lock = true)
            transaction.execute(
                "UPDATE filesystems SET expires_at_millis = ? WHERE uuid = ?",
                expiresAtMillis,
                parsed,
            )
        }
    }

    override fun getMaxSizeBytes(uuid: String): Long = requireFilesystem(parseUuid(uuid)).maxSizeBytes

    override fun getUsedBytes(uuid: String): Long = requireFilesystem(parseUuid(uuid)).usedBytes

    internal fun ensureSchema() {
        if (schemaReady) return
        synchronized(this) {
            if (schemaReady) return
            metadataDatabase.execute(
                """CREATE TABLE IF NOT EXISTS filesystems (
                    uuid UUID PRIMARY KEY,
                    description STRING NOT NULL,
                    owner STRING NULL,
                    max_size_bytes INT8 NOT NULL CHECK (max_size_bytes > 0),
                    used_bytes INT8 NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
                    expires_at_millis INT8 NULL,
                    created_at_millis INT8 NOT NULL
                )""".trimIndent(),
            )
            metadataDatabase.execute(
                """CREATE TABLE IF NOT EXISTS entries (
                    filesystem_uuid UUID NOT NULL REFERENCES filesystems(uuid) ON DELETE CASCADE,
                    path STRING NOT NULL,
                    parent_path STRING NOT NULL,
                    name STRING NOT NULL,
                    entry_kind STRING NOT NULL CHECK (entry_kind IN ('FILE', 'DIRECTORY')),
                    generation_uuid UUID NULL,
                    size_bytes INT8 NULL,
                    content_hash STRING NULL,
                    created_at_millis INT8 NULL,
                    modified_at_millis INT8 NULL,
                    PRIMARY KEY (filesystem_uuid, path)
                )""".trimIndent(),
            )
            metadataDatabase.execute(
                "CREATE INDEX IF NOT EXISTS entries_by_parent ON entries (filesystem_uuid, parent_path, name)",
            )
            metadataDatabase.execute(
                """CREATE TABLE IF NOT EXISTS file_blocks (
                    generation_uuid UUID NOT NULL,
                    ordinal INT4 NOT NULL,
                    blob_hash STRING NOT NULL,
                    size_bytes INT4 NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= $BLOCK_SIZE_BYTES),
                    reference_count INT8 NOT NULL DEFAULT 0 CHECK (reference_count >= 0),
                    PRIMARY KEY (generation_uuid, ordinal)
                )""".trimIndent(),
            )
            metadataDatabase.execute(
                "CREATE INDEX IF NOT EXISTS file_blocks_by_blob_hash ON file_blocks (blob_hash, reference_count)",
            )
            metadataDatabase.execute(
                """CREATE TABLE IF NOT EXISTS write_sessions (
                    session_uuid UUID PRIMARY KEY,
                    filesystem_uuid UUID NOT NULL REFERENCES filesystems(uuid) ON DELETE CASCADE,
                    path STRING NOT NULL,
                    expected_hash STRING NULL,
                    bytes_received INT8 NOT NULL,
                    created_at_millis INT8 NOT NULL,
                    state STRING NOT NULL
                )""".trimIndent(),
            )
            metadataDatabase.execute(
                """CREATE TABLE IF NOT EXISTS blob_gc_outbox (
                    blob_hash STRING PRIMARY KEY,
                    action STRING NOT NULL,
                    created_at_millis INT8 NOT NULL
                )""".trimIndent(),
            )
            schemaReady = true
        }
    }

    internal fun <T> transactionally(operation: (Database) -> T): T {
        var retryCount = 0
        while (true) {
            try {
                return metadataDatabase.execute(operation)
            } catch (failure: SQLException) {
                if (!failure.hasSqlState(SERIALIZATION_FAILURE_SQL_STATE) || retryCount >= MAX_TRANSACTION_RETRIES) {
                    throw failure
                }
                retryCount += 1
            }
        }
    }

    private fun SQLException.hasSqlState(expected: String): Boolean {
        var current: Throwable? = this
        while (current != null) {
            if (current is SQLException && current.sqlState == expected) return true
            current = current.cause
        }
        return false
    }

    internal fun parseUuid(uuid: String): UUID {
        val parsed = try {
            UUID.fromString(uuid)
        } catch (_: IllegalArgumentException) {
            throw InvalidFilesystemUuidException(uuid)
        }
        if (parsed.toString() != uuid) throw InvalidFilesystemUuidException(uuid)
        return parsed
    }

    internal fun requireFilesystem(uuid: UUID): FilesystemRecord {
        ensureSchema()
        return requireFilesystem(metadataDatabase, uuid, lock = false)
    }

    internal fun requireFilesystem(database: Database, uuid: UUID, lock: Boolean): FilesystemRecord {
        val suffix = if (lock) " FOR UPDATE" else ""
        val rows = database.getRows("SELECT * FROM filesystems WHERE uuid = ?$suffix", uuid)
        return rows.firstOrNull()?.toFilesystemRecord() ?: throw FilesystemNotFoundException(uuid.toString())
    }

    internal fun requireActiveFilesystem(uuid: UUID): FilesystemRecord {
        val filesystem = requireFilesystem(uuid)
        ensureNotExpired(filesystem)
        return filesystem
    }

    internal fun requireActiveFilesystem(database: Database, uuid: UUID, lock: Boolean): FilesystemRecord {
        val filesystem = requireFilesystem(database, uuid, lock)
        ensureNotExpired(filesystem)
        return filesystem
    }

    private fun ensureNotExpired(filesystem: FilesystemRecord) {
        val expiration = filesystem.expiresAtMillis ?: return
        val now = clock.currentTimeMillis()
        if (now >= expiration) throw FilesystemExpiredException(filesystem.uuid.toString(), expiration, now)
    }

    internal fun normalizePath(path: String): String {
        if (path.isEmpty()) throw InvalidPathException(path, "paths must not be empty and must begin with '/'.")
        if (!path.startsWith('/')) throw InvalidPathException(path, "paths must be absolute and begin with '/'.")
        if ('\u0000' in path) throw InvalidPathException(path, "paths must not contain a NUL character.")
        val components = ArrayDeque<String>()
        path.split('/').forEach { component ->
            when (component) {
                "", "." -> Unit
                ".." -> {
                    if (components.isEmpty()) {
                        throw InvalidPathException(path, "the '..' component escapes the filesystem root.")
                    }
                    components.removeLast()
                }
                else -> components.addLast(component)
            }
        }
        return if (components.isEmpty()) "/" else components.joinToString(prefix = "/", separator = "/")
    }

    internal fun parentPath(path: String): String = if (path == "/") "" else path.substringBeforeLast('/').ifEmpty { "/" }

    internal fun name(path: String): String = if (path == "/") "" else path.substringAfterLast('/')

    internal fun validateExpectedHash(expectedHash: String?) {
        if (expectedHash != null && !EXPECTED_HASH.matches(expectedHash)) {
            throw InvalidContentHashException(expectedHash)
        }
    }

    internal fun findEntry(database: Database, filesystemUuid: UUID, path: String, lock: Boolean = false): EntryRecord? {
        val suffix = if (lock) " FOR UPDATE" else ""
        return database.getRows(
            "SELECT * FROM entries WHERE filesystem_uuid = ? AND path = ?$suffix",
            filesystemUuid,
            path,
        ).firstOrNull()?.toEntryRecord()
    }

    internal fun requireEntry(database: Database, filesystemUuid: UUID, path: String, lock: Boolean = false): EntryRecord =
        findEntry(database, filesystemUuid, path, lock) ?: throw PathNotFoundException(path)

    internal fun requireDirectory(database: Database, filesystemUuid: UUID, path: String, lock: Boolean = false): EntryRecord {
        val entry = requireEntry(database, filesystemUuid, path, lock)
        if (!entry.isDirectory) throw PathTypeMismatchException(path, "DIRECTORY", entry.kind)
        return entry
    }

    internal fun blocks(database: Database, generationUuid: UUID): List<BlockRecord> = database.getRows(
        "SELECT * FROM file_blocks WHERE generation_uuid = ? ORDER BY ordinal",
        generationUuid,
    ).map { it.toBlockRecord() }

    internal fun beginSession(filesystemUuid: UUID, rawPath: String, expectedHash: String?): Pair<UUID, String> {
        ensureSchema()
        validateExpectedHash(expectedHash)
        requireActiveFilesystem(filesystemUuid)
        val path = normalizePath(rawPath)
        if (path == "/") throw InvalidPathException(rawPath, "the filesystem root cannot contain file data.")
        val session = UUID.randomUUID()
        metadataDatabase.execute(
            """INSERT INTO write_sessions
                (session_uuid, filesystem_uuid, path, expected_hash, bytes_received, created_at_millis, state)
                VALUES (?, ?, ?, ?, 0, ?, 'OPEN')""".trimIndent(),
            session,
            filesystemUuid,
            path,
            expectedHash,
            clock.currentTimeMillis(),
        )
        return session to path
    }

    internal fun stageBlock(
        database: Database,
        generationUuid: UUID,
        ordinal: Int,
        bytes: ByteArray,
    ): BlockRecord {
        val hash = sha256(bytes)
        if (!blobstoreService.pinBlob(BLOB_PIN_OWNER, hash)) {
            blobstoreService.putBlob(BLOB_PIN_OWNER, hash, bytes.size.toLong(), ByteArrayInputStream(bytes))
            if (!blobstoreService.pinBlob(BLOB_PIN_OWNER, hash)) {
                throw IllegalStateException(
                    "Blobstore accepted block '$hash' (${bytes.size} bytes) but refused the required " +
                        "'$BLOB_PIN_OWNER' durability pin before metadata commit.",
                )
            }
        }
        database.execute(
            """INSERT INTO file_blocks
                (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                VALUES (?, ?, ?, ?, 0)""".trimIndent(),
            generationUuid,
            ordinal,
            hash,
            bytes.size,
        )
        return BlockRecord(generationUuid, ordinal, hash, bytes.size, 0L)
    }

    internal fun updateSessionBytes(sessionUuid: UUID, bytesReceived: Long) {
        metadataDatabase.execute(
            "UPDATE write_sessions SET bytes_received = ? WHERE session_uuid = ? AND state = 'OPEN'",
            bytesReceived,
            sessionUuid,
        )
    }

    internal fun abortSession(sessionUuid: UUID) {
        metadataDatabase.execute(
            "UPDATE write_sessions SET state = 'ABORTED' WHERE session_uuid = ? AND state = 'OPEN'",
            sessionUuid,
        )
    }

    internal fun commitGeneration(stage: StagedGeneration, unconditional: Boolean) {
        try {
            transactionally { transaction ->
                val filesystem = requireActiveFilesystem(transaction, stage.filesystemUuid, lock = true)
                val current = findEntry(transaction, stage.filesystemUuid, stage.path, lock = true)
                if (current?.isDirectory == true) {
                    throw PathTypeMismatchException(stage.path, "FILE", "DIRECTORY")
                }
                if (!unconditional) verifyConditionalWrite(stage.path, stage.expectedHash, current)
                requireDirectory(transaction, stage.filesystemUuid, parentPath(stage.path), lock = true)
                val oldSize = current?.sizeBytes ?: 0L
                val attemptedUsage = checkedAttemptedUsage(filesystem, stage.path, oldSize, stage.sizeBytes)
                val now = clock.currentTimeMillis()
                if (current == null) {
                    transaction.execute(
                        """INSERT INTO entries
                            (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                             size_bytes, content_hash, created_at_millis, modified_at_millis)
                            VALUES (?, ?, ?, ?, 'FILE', ?, ?, ?, ?, ?)""".trimIndent(),
                        stage.filesystemUuid,
                        stage.path,
                        parentPath(stage.path),
                        name(stage.path),
                        stage.sessionUuid,
                        stage.sizeBytes,
                        stage.contentHash,
                        now,
                        now,
                    )
                } else {
                    transaction.execute(
                        """UPDATE entries SET generation_uuid = ?, size_bytes = ?, content_hash = ?,
                            modified_at_millis = ?, entry_kind = 'FILE'
                            WHERE filesystem_uuid = ? AND path = ?""".trimIndent(),
                        stage.sessionUuid,
                        stage.sizeBytes,
                        stage.contentHash,
                        now,
                        stage.filesystemUuid,
                        stage.path,
                    )
                }
                transaction.execute(
                    "UPDATE file_blocks SET reference_count = 1 WHERE generation_uuid = ?",
                    stage.sessionUuid,
                )
                current?.generationUuid?.let { releaseGeneration(transaction, it) }
                transaction.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                    attemptedUsage,
                    stage.filesystemUuid,
                )
                transaction.execute(
                    "UPDATE write_sessions SET state = 'COMMITTED', bytes_received = ? WHERE session_uuid = ?",
                    stage.sizeBytes,
                    stage.sessionUuid,
                )
            }
        } catch (failure: Throwable) {
            abortSession(stage.sessionUuid)
            throw failure
        }
    }

    internal fun commitAppend(stage: StagedGeneration) {
        try {
            transactionally { transaction ->
                val filesystem = requireActiveFilesystem(transaction, stage.filesystemUuid, lock = true)
                val current = findEntry(transaction, stage.filesystemUuid, stage.path, lock = true)
                if (current?.isDirectory == true) throw PathTypeMismatchException(stage.path, "FILE", "DIRECTORY")
                requireDirectory(transaction, stage.filesystemUuid, parentPath(stage.path), lock = true)
                val oldSize = current?.sizeBytes ?: 0L
                val appendedSize = if (stage.sizeBytes > Long.MAX_VALUE - oldSize) {
                    Long.MAX_VALUE
                } else {
                    oldSize + stage.sizeBytes
                }
                val attemptedUsage = checkedAttemptedUsage(filesystem, stage.path, oldSize, appendedSize)
                val finalGeneration = UUID.randomUUID()
                val assembler = TransactionalBlockAssembler(this, transaction, finalGeneration)
                current?.generationUuid?.let { generation ->
                    blocks(transaction, generation).forEach { block ->
                        blobstoreService.getBlob(BLOB_PIN_OWNER, block.blobHash).use { assembler.writeFrom(it) }
                    }
                }
                blocks(transaction, stage.sessionUuid).forEach { block ->
                    blobstoreService.getBlob(BLOB_PIN_OWNER, block.blobHash).use { assembler.writeFrom(it) }
                }
                val final = assembler.finish()
                val now = clock.currentTimeMillis()
                if (current == null) {
                    transaction.execute(
                        """INSERT INTO entries
                            (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                             size_bytes, content_hash, created_at_millis, modified_at_millis)
                            VALUES (?, ?, ?, ?, 'FILE', ?, ?, ?, ?, ?)""".trimIndent(),
                        stage.filesystemUuid,
                        stage.path,
                        parentPath(stage.path),
                        name(stage.path),
                        finalGeneration,
                        final.first,
                        final.second,
                        now,
                        now,
                    )
                } else {
                    transaction.execute(
                        """UPDATE entries SET generation_uuid = ?, size_bytes = ?, content_hash = ?,
                            modified_at_millis = ? WHERE filesystem_uuid = ? AND path = ?""".trimIndent(),
                        finalGeneration,
                        final.first,
                        final.second,
                        now,
                        stage.filesystemUuid,
                        stage.path,
                    )
                }
                transaction.execute(
                    "UPDATE file_blocks SET reference_count = 1 WHERE generation_uuid = ?",
                    finalGeneration,
                )
                current?.generationUuid?.let { releaseGeneration(transaction, it) }
                blocks(transaction, stage.sessionUuid).forEach { enqueueBlobForGc(transaction, it.blobHash) }
                transaction.execute("DELETE FROM file_blocks WHERE generation_uuid = ?", stage.sessionUuid)
                transaction.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                    attemptedUsage,
                    stage.filesystemUuid,
                )
                transaction.execute(
                    "UPDATE write_sessions SET state = 'COMMITTED', bytes_received = ? WHERE session_uuid = ?",
                    stage.sizeBytes,
                    stage.sessionUuid,
                )
            }
        } catch (failure: Throwable) {
            abortSession(stage.sessionUuid)
            throw failure
        }
    }

    private fun verifyConditionalWrite(path: String, expectedHash: String?, current: EntryRecord?) {
        val observed = current?.contentHash
        val matches = if (expectedHash == null) current == null else current != null && observed == expectedHash
        if (!matches) throw FileContentConflictException(path, expectedHash, observed)
    }

    internal fun checkedAttemptedUsage(
        filesystem: FilesystemRecord,
        path: String,
        oldSize: Long,
        newSize: Long,
    ): Long {
        val baseline = filesystem.usedBytes - oldSize
        val attempted = if (newSize > Long.MAX_VALUE - baseline) Long.MAX_VALUE else baseline + newSize
        if (attempted > filesystem.maxSizeBytes) {
            throw QuotaExceededException(path, filesystem.maxSizeBytes, filesystem.usedBytes, attempted)
        }
        return attempted
    }

    internal fun retainGeneration(database: Database, generationUuid: UUID) {
        database.execute(
            "UPDATE file_blocks SET reference_count = reference_count + 1 WHERE generation_uuid = ?",
            generationUuid,
        )
    }

    internal fun releaseGeneration(database: Database, generationUuid: UUID) {
        val generationBlocks = blocks(database, generationUuid)
        if (generationBlocks.isEmpty()) return
        val references = generationBlocks.first().referenceCount
        if (references <= 0L) return
        if (references == 1L) {
            generationBlocks.forEach { enqueueBlobForGc(database, it.blobHash) }
            database.execute("DELETE FROM file_blocks WHERE generation_uuid = ?", generationUuid)
        } else {
            database.execute(
                "UPDATE file_blocks SET reference_count = reference_count - 1 WHERE generation_uuid = ?",
                generationUuid,
            )
        }
    }

    internal fun enqueueBlobForGc(database: Database, hash: String) {
        database.execute(
            """INSERT INTO blob_gc_outbox (blob_hash, action, created_at_millis)
                VALUES (?, 'UNPIN_IF_UNREFERENCED', ?)
                ON CONFLICT (blob_hash) DO UPDATE
                SET action = excluded.action, created_at_millis = excluded.created_at_millis""".trimIndent(),
            hash,
            clock.currentTimeMillis(),
        )
    }

    internal fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(bytes)
        .joinToString("") { "%02X".format(it.toInt() and 0xFF) }

    private companion object {
        val EXPECTED_HASH = Regex("[0-9A-F]{64}")
        const val SERIALIZATION_FAILURE_SQL_STATE = "40001"
        const val MAX_TRANSACTION_RETRIES = 32
    }
}
