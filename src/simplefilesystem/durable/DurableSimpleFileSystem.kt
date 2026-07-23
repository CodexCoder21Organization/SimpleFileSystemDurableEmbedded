package simplefilesystem.durable

import okio.Buffer
import okio.Source
import okio.buffer
import simplefilesystem.FileEntryInfo
import simplefilesystem.FileEntryPage
import simplefilesystem.FileEntryType
import simplefilesystem.FileSink
import simplefilesystem.FileMetadataInfo
import simplefilesystem.INLINE_PAYLOAD_MAX_BYTES
import simplefilesystem.InvalidByteRangeException
import simplefilesystem.InvalidPathException
import simplefilesystem.InvalidPathReason
import simplefilesystem.DirectoryNotEmptyException
import simplefilesystem.InvalidMoveException
import simplefilesystem.InlinePayloadTooLargeException
import simplefilesystem.MalformedBase64Exception
import simplefilesystem.MalformedUtf8Exception
import simplefilesystem.PathAlreadyExistsException
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import simplefilesystem.QuotaArithmeticOverflowException
import simplefilesystem.SimpleFileSystem
import sql.Database
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.UUID

class DurableSimpleFileSystem internal constructor(
    private val manager: DurableSimpleFileSystemManager,
    private val filesystemUuid: UUID,
) : SimpleFileSystem {
    override fun list(path: String, after: String?, limit: Int): FileEntryPage {
        return manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            val normalized = manager.normalizePath(path)
            val cursor = manager.validateListCursor(normalized, after, recursive = false)
            val pageLimit = manager.validatePageLimit(limit)
            val cursorClause = if (cursor == null) "" else "AND path > ?"
            val pageArguments = mutableListOf<Any>(filesystemUuid, normalized)
            if (cursor != null) pageArguments += cursor
            pageArguments += pageLimit + 1
            val entries = validatedDirectoryListing(
                transaction,
                normalized,
                """SELECT * FROM entries
                    WHERE filesystem_uuid = ? AND parent_path = ? $cursorClause
                    ORDER BY path LIMIT ?""".trimIndent(),
                *pageArguments.toTypedArray(),
            )
            fileEntryPage(entries, pageLimit, filesystem.namespaceRevision)
        }
    }

    override fun listRecursively(path: String, after: String?, limit: Int): FileEntryPage {
        return manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            val normalized = manager.normalizePath(path)
            val cursor = manager.validateListCursor(normalized, after, recursive = true)
            val pageLimit = manager.validatePageLimit(limit)
            val prefix = if (normalized == "/") "/" else "$normalized/"
            val prefixUpperBound = prefixUpperBound(normalized)
            val cursorClause = if (cursor == null) "" else "AND path > ?"
            val pageArguments = mutableListOf<Any>(
                filesystemUuid,
                normalized,
                prefix,
                prefixUpperBound,
            )
            if (cursor != null) pageArguments += cursor
            pageArguments += pageLimit + 1
            val entries = validatedDirectoryListing(
                transaction,
                normalized,
                """SELECT * FROM entries
                    WHERE filesystem_uuid = ? AND path != ? AND path >= ? AND path < ? $cursorClause
                    ORDER BY path LIMIT ?""".trimIndent(),
                *pageArguments.toTypedArray(),
            )
            fileEntryPage(entries, pageLimit, filesystem.namespaceRevision)
        }
    }

    override fun createDirectory(path: String, mustCreate: Boolean) {
        createDirectoriesInternal(path, mustCreate, recursive = false)
    }

    override fun createDirectories(path: String, mustCreate: Boolean) {
        createDirectoriesInternal(path, mustCreate, recursive = true)
    }

    override fun read(path: String): String = Base64.getEncoder().encodeToString(readInlineBytes(path))

    override fun readUtf8(path: String): String = decodeStrictUtf8(path, readInlineBytes(path))

    override fun write(path: String, data: String, ifMatches: String?): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = decodeBase64(path, data)
        manager.validateExpectedHash(ifMatches)
        return commitBytes(sink(path, ifMatches), bytes)
    }

    override fun writeUtf8(path: String, content: String, ifMatches: String?): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = encodeStrictUtf8(path, content)
        manager.validateExpectedHash(ifMatches)
        return commitBytes(sink(path, ifMatches), bytes)
    }

    override fun overwrite(path: String, data: String): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = decodeBase64(path, data)
        return commitBytes(unconditionalSink(path), bytes)
    }

    override fun overwriteUtf8(path: String, content: String): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = encodeStrictUtf8(path, content)
        return commitBytes(unconditionalSink(path), bytes)
    }

    override fun appendingWrite(path: String, data: String, mustExist: Boolean): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = decodeBase64(path, data)
        return commitBytes(appendingSink(path, mustExist), bytes)
    }

    override fun appendingWriteUtf8(path: String, content: String, mustExist: Boolean): FileMetadataInfo {
        requireActive()
        manager.normalizePath(path)
        val bytes = encodeStrictUtf8(path, content)
        return commitBytes(appendingSink(path, mustExist), bytes)
    }

    override fun metadata(path: String): FileMetadataInfo {
        requireActive()
        val normalized = manager.normalizePath(path)
        return manager.transactionally { transaction ->
            manager.requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized)
            manager.requireEntry(transaction, filesystemUuid, normalized).toMetadata()
        }
    }

    override fun metadataOrNull(path: String): FileMetadataInfo? {
        requireActive()
        val normalized = manager.normalizePath(path)
        return manager.transactionally { transaction ->
            manager.requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized)
            manager.findEntry(transaction, filesystemUuid, normalized)?.toMetadata()
        }
    }

    override fun exists(path: String): Boolean {
        requireActive()
        val normalized = manager.normalizePath(path)
        return manager.transactionally { transaction ->
            manager.requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized)
            manager.findEntry(transaction, filesystemUuid, normalized) != null
        }
    }

    override fun delete(path: String, mustExist: Boolean) {
        requireActive()
        val normalized = manager.normalizePath(path)
        if (normalized == "/") {
            throw InvalidPathException(path, InvalidPathReason.ROOT_NOT_ALLOWED_FOR_OPERATION)
        }
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized, lock = true)
            val entry = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (entry == null) {
                if (mustExist) throw PathNotFoundException(normalized)
                return@transactionally
            }
            if (entry.isDirectory) {
                val childCount = transaction.getLong(
                    "SELECT count(*) FROM entries WHERE filesystem_uuid = ? AND parent_path = ?",
                    filesystemUuid,
                    normalized,
                ) ?: 0L
                if (childCount > 0L) throw DirectoryNotEmptyException(normalized, childCount)
            }
            entry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
            transaction.execute(
                "DELETE FROM entries WHERE filesystem_uuid = ? AND path = ?",
                filesystemUuid,
                normalized,
            )
            val released = entry.sizeBytes ?: 0L
            val attemptedUsage = manager.checkedAttemptedUsage(filesystem, normalized, released, 0L)
            transaction.execute(
                "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                attemptedUsage,
                filesystemUuid,
            )
            manager.recordNamespaceMutation(
                transaction,
                filesystem,
                attemptedUsage,
                listOf(normalized, manager.parentPath(normalized)),
            )
        }
    }

    override fun deleteRecursively(path: String, mustExist: Boolean) {
        requireActive()
        val normalized = manager.normalizePath(path)
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized, lock = true)
            val root = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (root == null) {
                if (mustExist) throw PathNotFoundException(normalized)
                return@transactionally
            }
            val prefix = if (normalized == "/") "/" else "$normalized/"
            val prefixUpperBound = prefixUpperBound(normalized)
            var cursor = ""
            var released = 0L
            var deletedAny = false
            val deletedPaths = mutableListOf<String>()
            while (true) {
                val entries = transaction.getRows(
                    """WITH page_boundary AS MATERIALIZED (
                            SELECT max(path) AS last_path
                            FROM (
                                SELECT path FROM entries
                                WHERE filesystem_uuid = ? AND path != '/' AND path > ?
                                  AND (path = ? OR (path >= ? AND path < ?))
                                ORDER BY path LIMIT ? FOR UPDATE
                            ) AS bounded_paths
                        )
                        DELETE FROM entries
                        WHERE filesystem_uuid = ? AND path != '/' AND path > ?
                          AND path <= (SELECT last_path FROM page_boundary)
                          AND (path = ? OR (path >= ? AND path < ?))
                        RETURNING *""".trimIndent(),
                    filesystemUuid,
                    cursor,
                    normalized,
                    prefix,
                    prefixUpperBound,
                    TREE_KEYSET_BATCH_SIZE,
                    filesystemUuid,
                    cursor,
                    normalized,
                    prefix,
                    prefixUpperBound,
                ).map { it.toEntryRecord() }
                if (entries.isEmpty()) break
                entries.forEach { entry ->
                    entry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
                    released = try {
                        Math.addExact(released, entry.sizeBytes ?: 0L)
                    } catch (_: ArithmeticException) {
                        throw QuotaArithmeticOverflowException(
                            normalized,
                            filesystem.usedBytes,
                            released,
                            entry.sizeBytes ?: 0L,
                        )
                    }
                }
                deletedPaths += entries.map { it.path }
                cursor = entries.maxOf { it.path }
                deletedAny = true
                if (entries.size < TREE_KEYSET_BATCH_SIZE) break
            }
            if (deletedAny) {
                val attemptedUsage = manager.checkedAttemptedUsage(filesystem, normalized, released, 0L)
                transaction.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                    attemptedUsage,
                    filesystemUuid,
                )
                manager.recordNamespaceMutation(
                    transaction,
                    filesystem,
                    attemptedUsage,
                    deletedPaths +
                        if (normalized == "/") listOf("/") else listOf(manager.parentPath(normalized)),
                )
            }
        }
    }

    override fun copy(source: String, target: String): FileMetadataInfo {
        requireActive()
        val normalizedSource = manager.normalizePath(source)
        val normalizedTarget = manager.normalizePath(target)
        manager.ensureSchema()
        return manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalizedSource, lock = true)
            val sourceEntry = manager.requireEntry(transaction, filesystemUuid, normalizedSource, lock = true)
            if (!sourceEntry.isFile) {
                throw PathTypeMismatchException(
                    normalizedSource,
                    FileEntryType.REGULAR_FILE,
                    sourceEntry.entryType,
                )
            }
            if (normalizedSource == normalizedTarget) return@transactionally sourceEntry.toMetadata()
            if (normalizedTarget != "/") {
                manager.requireDirectoryPath(
                    transaction,
                    filesystemUuid,
                    manager.parentPath(normalizedTarget),
                    lock = true,
                )
            }
            val targetEntry = manager.findEntry(transaction, filesystemUuid, normalizedTarget, lock = true)
            if (targetEntry != null && !targetEntry.isFile) {
                throw PathTypeMismatchException(
                    normalizedTarget,
                    FileEntryType.REGULAR_FILE,
                    targetEntry.entryType,
                )
            }
            val attempted = manager.checkedAttemptedUsage(
                filesystem,
                normalizedTarget,
                targetEntry?.sizeBytes ?: 0L,
                requireNotNull(sourceEntry.sizeBytes),
            )
            val generation = requireNotNull(sourceEntry.generationUuid)
            if (targetEntry == null) {
                transaction.execute(
                    """INSERT INTO entries
                        (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                         size_bytes, content_hash, created_at_millis, modified_at_millis)
                        VALUES (?, ?, ?, ?, 'FILE', ?, ?, ?, ?, ?)""".trimIndent(),
                    filesystemUuid,
                    normalizedTarget,
                    manager.parentPath(normalizedTarget),
                    manager.name(normalizedTarget),
                    generation,
                    sourceEntry.sizeBytes,
                    sourceEntry.contentHash,
                    manager.clock.currentTimeMillis(),
                    manager.clock.currentTimeMillis(),
                )
                manager.retainGeneration(transaction, generation)
            } else {
                if (targetEntry.generationUuid != generation) {
                    manager.retainGeneration(transaction, generation)
                    targetEntry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
                }
                transaction.execute(
                    """UPDATE entries SET generation_uuid = ?, size_bytes = ?, content_hash = ?,
                        modified_at_millis = ? WHERE filesystem_uuid = ? AND path = ?""".trimIndent(),
                    generation,
                    sourceEntry.sizeBytes,
                    sourceEntry.contentHash,
                    manager.clock.currentTimeMillis(),
                    filesystemUuid,
                    normalizedTarget,
                )
            }
            transaction.execute("UPDATE filesystems SET used_bytes = ? WHERE uuid = ?", attempted, filesystemUuid)
            manager.recordNamespaceMutation(
                transaction,
                filesystem,
                attempted,
                listOf(normalizedTarget) +
                    if (targetEntry == null) listOf(manager.parentPath(normalizedTarget)) else emptyList(),
            )
            manager.requireEntry(transaction, filesystemUuid, normalizedTarget).toMetadata()
        }
    }

    override fun atomicMove(source: String, target: String): FileMetadataInfo {
        requireActive()
        val normalizedSource = manager.normalizePath(source)
        val normalizedTarget = manager.normalizePath(target)
        manager.ensureSchema()
        return manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalizedSource, lock = true)
            val sourceEntry = manager.requireEntry(transaction, filesystemUuid, normalizedSource, lock = true)
            if (normalizedSource == "/" || normalizedTarget == "/") {
                throw InvalidMoveException(
                    normalizedSource,
                    normalizedTarget,
                    "the filesystem root cannot be moved or replaced.",
                )
            }
            if (normalizedSource == normalizedTarget) return@transactionally sourceEntry.toMetadata()
            manager.requireDirectoryPath(
                transaction,
                filesystemUuid,
                manager.parentPath(normalizedTarget),
                lock = true,
            )
            if (sourceEntry.isDirectory && normalizedTarget.startsWith("$normalizedSource/")) {
                throw InvalidMoveException(
                    normalizedSource,
                    normalizedTarget,
                    "a directory cannot be moved into its own descendant.",
                )
            }
            val replaced = manager.findEntry(transaction, filesystemUuid, normalizedTarget, lock = true)
            if (replaced != null && replaced.entryType != sourceEntry.entryType) {
                throw PathTypeMismatchException(normalizedTarget, sourceEntry.entryType, replaced.entryType)
            }
            if (replaced?.isDirectory == true) {
                val childCount = transaction.getLong(
                    "SELECT count(*) FROM entries WHERE filesystem_uuid = ? AND parent_path = ?",
                    filesystemUuid,
                    normalizedTarget,
                ) ?: 0L
                if (childCount > 0L) throw DirectoryNotEmptyException(normalizedTarget, childCount)
            }
            replaced?.generationUuid?.let { manager.releaseGeneration(transaction, it) }
            if (replaced != null) {
                transaction.execute(
                    "DELETE FROM entries WHERE filesystem_uuid = ? AND path = ?",
                    filesystemUuid,
                    replaced.path,
                )
            }
            val sourcePrefix = "$normalizedSource/"
            val sourcePrefixUpperBound = prefixUpperBound(normalizedSource)
            val movedPaths = transaction.getStrings(
                """UPDATE entries
                    SET path = CASE
                            WHEN path = ? THEN ?
                            ELSE ? || substring(path FROM ?)
                        END,
                        parent_path = CASE
                            WHEN path = ? THEN ?
                            ELSE ? || substring(parent_path FROM ?)
                        END,
                        name = CASE WHEN path = ? THEN ? ELSE name END
                    WHERE filesystem_uuid = ?
                      AND (path = ? OR (path >= ? AND path < ?))
                    RETURNING path""".trimIndent(),
                normalizedSource,
                normalizedTarget,
                normalizedTarget,
                normalizedSource.length + 1,
                normalizedSource,
                manager.parentPath(normalizedTarget),
                normalizedTarget,
                normalizedSource.length + 1,
                normalizedSource,
                manager.name(normalizedTarget),
                filesystemUuid,
                normalizedSource,
                sourcePrefix,
                sourcePrefixUpperBound,
            )
            val movedPathEvents = movedPaths.flatMap { newPath ->
                val oldPath = normalizedSource + newPath.removePrefix(normalizedTarget)
                listOf(oldPath, newPath)
            }
            val released = replaced?.sizeBytes ?: 0L
            val attemptedUsage = manager.checkedAttemptedUsage(filesystem, normalizedTarget, released, 0L)
            transaction.execute(
                "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                attemptedUsage,
                filesystemUuid,
            )
            val membershipPaths = listOf(manager.parentPath(normalizedSource)) +
                if (replaced == null) listOf(manager.parentPath(normalizedTarget)) else emptyList()
            manager.recordNamespaceMutation(
                transaction,
                filesystem,
                attemptedUsage,
                movedPathEvents + membershipPaths,
            )
            manager.requireEntry(transaction, filesystemUuid, normalizedTarget).toMetadata()
        }
    }

    override fun source(path: String): Source {
        val snapshot = fileSnapshot(path)
        return GenerationSource(
            manager,
            snapshot.generationUuid,
            snapshot.readerUuid,
            firstOrdinal = 0,
            lastOrdinal = ((requireNotNull(snapshot.entry.sizeBytes) - 1L) / BLOCK_SIZE_BYTES).toInt(),
            initialSkip = 0L,
            byteCount = requireNotNull(snapshot.entry.sizeBytes),
        )
    }

    override fun source(path: String, offset: Long, byteCount: Long): Source {
        val snapshot = fileSnapshot(path)
        val size = requireNotNull(snapshot.entry.sizeBytes)
        if (offset < 0L || byteCount < 0L || offset > size || byteCount > size - offset) {
            manager.releaseReaderSession(snapshot.readerUuid)
            throw InvalidByteRangeException(snapshot.entry.path, offset, byteCount, size)
        }
        if (byteCount == 0L) return GenerationSource(
            manager, snapshot.generationUuid, snapshot.readerUuid, 0, -1, 0L, 0L,
        )
        val firstOrdinal = (offset / BLOCK_SIZE_BYTES).toInt()
        val lastOrdinal = ((offset + byteCount - 1L) / BLOCK_SIZE_BYTES).toInt()
        return GenerationSource(
            manager,
            snapshot.generationUuid,
            snapshot.readerUuid,
            firstOrdinal,
            lastOrdinal,
            initialSkip = offset % BLOCK_SIZE_BYTES,
            byteCount = byteCount,
        )
    }

    override fun sink(path: String, ifMatches: String?): FileSink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = ifMatches,
        unconditional = false,
        append = false,
    )

    override fun appendingSink(path: String): FileSink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = null,
        unconditional = false,
        append = true,
        appendMustExist = false,
    )

    override fun inputStream(path: String): InputStream = source(path).buffer().inputStream()

    private fun unconditionalSink(path: String): FileSink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = null,
        unconditional = true,
        append = false,
    )

    private fun createDirectoriesInternal(path: String, mustCreate: Boolean, recursive: Boolean) {
        requireActive()
        val normalized = manager.normalizePath(path)
        if (normalized == "/") {
            active()
            if (mustCreate) throw PathAlreadyExistsException(normalized)
            return
        }
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.validateIntermediateComponents(transaction, filesystemUuid, normalized, lock = true)
            val existing = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (existing != null) {
                if (mustCreate) throw PathAlreadyExistsException(normalized)
                if (!existing.isDirectory) {
                    throw PathTypeMismatchException(normalized, FileEntryType.DIRECTORY, existing.entryType)
                }
                return@transactionally
            }
            val now = manager.clock.currentTimeMillis()
            if (!recursive) {
                manager.requireDirectoryPath(
                    transaction,
                    filesystemUuid,
                    manager.parentPath(normalized),
                    lock = true,
                )
                insertDirectory(transaction, normalized, now)
                manager.recordNamespaceMutation(
                    transaction,
                    filesystem,
                    affectedPaths = listOf(normalized, manager.parentPath(normalized)),
                )
                return@transactionally
            }
            var current = ""
            val createdPaths = mutableListOf<String>()
            normalized.removePrefix("/").split('/').forEach { component ->
                current += "/$component"
                val componentEntry = manager.findEntry(transaction, filesystemUuid, current, lock = true)
                if (componentEntry == null) {
                    insertDirectory(transaction, current, now)
                    createdPaths += current
                } else if (!componentEntry.isDirectory) {
                    throw PathTypeMismatchException(current, FileEntryType.DIRECTORY, componentEntry.entryType)
                }
            }
            if (createdPaths.isNotEmpty()) {
                manager.recordNamespaceMutation(
                    transaction,
                    filesystem,
                    affectedPaths = createdPaths.flatMap { listOf(it, manager.parentPath(it)) },
                )
            }
        }
    }

    private fun insertDirectory(database: sql.Database, path: String, now: Long) {
        database.execute(
            """INSERT INTO entries
                (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                 size_bytes, content_hash, created_at_millis, modified_at_millis)
                VALUES (?, ?, ?, ?, 'DIRECTORY', NULL, NULL, NULL, ?, ?)""".trimIndent(),
            filesystemUuid,
            path,
            manager.parentPath(path),
            manager.name(path),
            now,
            now,
        )
    }

    private fun appendingSink(path: String, mustExist: Boolean): FileSink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = null,
        unconditional = false,
        append = true,
        appendMustExist = mustExist,
    )

    private fun validatedDirectoryListing(
        transaction: Database,
        normalizedPath: String,
        pageQuery: String,
        vararg pageArguments: Any,
    ): List<EntryRecord> {
        manager.validateIntermediateComponents(transaction, filesystemUuid, normalizedPath)
        val rows = transaction.getRows(
            """WITH requested_directory AS MATERIALIZED (
                    SELECT entry_kind FROM entries
                    WHERE filesystem_uuid = ? AND path = ?
                ),
                page AS MATERIALIZED (
                    $pageQuery
                )
                SELECT requested_directory.entry_kind AS requested_directory_kind, page.*
                FROM (SELECT true) AS anchor
                LEFT JOIN requested_directory ON true
                LEFT JOIN page ON true
                ORDER BY page.path""".trimIndent(),
            filesystemUuid,
            normalizedPath,
            *pageArguments,
        )
        val directoryKind = rows.first().nullableStringValue("requested_directory_kind")
            ?: throw PathNotFoundException(normalizedPath)
        if (directoryKind != "DIRECTORY") {
            val actualType = if (directoryKind == "FILE") FileEntryType.REGULAR_FILE else FileEntryType.OTHER
            throw PathTypeMismatchException(normalizedPath, FileEntryType.DIRECTORY, actualType)
        }
        return rows.filter { it.nullableStringValue("path") != null }.map { it.toEntryRecord() }
    }

    private fun fileSnapshot(rawPath: String): FileGenerationSnapshot {
        requireActive()
        val normalized = manager.normalizePath(rawPath)
        return manager.fileGenerationSnapshot(filesystemUuid, normalized)
    }

    private fun requireActive() {
        manager.requireActiveFilesystem(filesystemUuid)
    }

    private fun readInlineBytes(path: String): ByteArray {
        val snapshot = fileSnapshot(path)
        val size = requireNotNull(snapshot.entry.sizeBytes)
        requireInlineSize(snapshot.entry.path, size)
        val buffer = Buffer()
        val lastOrdinal = if (size == 0L) -1 else ((size - 1L) / BLOCK_SIZE_BYTES).toInt()
        GenerationSource(manager, snapshot.generationUuid, snapshot.readerUuid, 0, lastOrdinal, 0L, size).use { source ->
            while (source.read(buffer, 8192L) != -1L) Unit
        }
        return buffer.readByteArray()
    }

    private fun commitBytes(sink: FileSink, bytes: ByteArray): FileMetadataInfo {
        val buffered = sink.buffer()
        return try {
            buffered.write(bytes)
            buffered.flush()
            sink.commit()
        } finally {
            buffered.close()
        }
    }

    private fun fileEntryPage(
        records: List<EntryRecord>,
        limit: Int,
        revision: Long,
    ): FileEntryPage {
        val selected = records.take(limit)
        return FileEntryPageValue(
            entries = selected.map { it.toInfo() },
            nextAfter = selected.lastOrNull()?.path.takeIf { records.size > limit },
            snapshotRevision = revision,
        )
    }

    private fun decodeBase64(path: String, data: String): ByteArray {
        val invalidOffset = data.indexOfFirst { it !in BASE64_ALPHABET && it != '=' }.takeIf { it >= 0 }
        if (invalidOffset != null) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                invalidOffset.toLong(),
                "character '${data[invalidOffset]}' is not in the standard Base64 alphabet",
            )
        }
        if (data.length % 4 != 0) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                null,
                "standard Base64 input must have a length divisible by four and include required padding",
            )
        }
        val firstPadding = data.indexOf('=').takeIf { it >= 0 }
        val padding = if (firstPadding == null) 0 else data.length - firstPadding
        if (padding > 2 || (firstPadding != null && data.substring(firstPadding).any { it != '=' })) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                firstPadding?.toLong(),
                "padding is permitted only as one or two '=' characters at the end",
            )
        }
        if (data.isNotEmpty() && padding == 1 && base64Value(data[data.length - 2]) and 0x03 != 0) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                (data.length - 2).toLong(),
                "the final Base64 quantum has non-zero unused bits",
            )
        }
        if (data.isNotEmpty() && padding == 2 && base64Value(data[data.length - 3]) and 0x0F != 0) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                (data.length - 3).toLong(),
                "the final Base64 quantum has non-zero unused bits",
            )
        }
        val decodedLength = try {
            Math.subtractExact(Math.multiplyExact((data.length / 4).toLong(), 3L), padding.toLong())
        } catch (_: ArithmeticException) {
            throw QuotaArithmeticOverflowException(path, 0L, 0L, data.length.toLong())
        }
        requireInlineSize(path, decodedLength)
        return try {
            Base64.getDecoder().decode(data)
        } catch (failure: IllegalArgumentException) {
            throw MalformedBase64Exception(
                path,
                data.length.toLong(),
                null,
                failure.message ?: "the input is not strict standard padded Base64",
            )
        }
    }

    private fun encodeStrictUtf8(path: String, content: String): ByteArray {
        firstMalformedUnicodeIndex(content)?.let { index ->
            throw MalformedUtf8Exception(path, null, index, "unpaired UTF-16 surrogate")
        }
        return content.toByteArray(StandardCharsets.UTF_8).also { requireInlineSize(path, it.size.toLong()) }
    }

    private fun decodeStrictUtf8(path: String, bytes: ByteArray): String {
        val decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
        val input = ByteBuffer.wrap(bytes)
        val output = CharBuffer.allocate(bytes.size)
        val result = decoder.decode(input, output, true)
        if (result.isError) {
            throw MalformedUtf8Exception(path, input.position().toLong(), null, "invalid UTF-8 byte sequence")
        }
        val flushed = decoder.flush(output)
        if (flushed.isError) {
            throw MalformedUtf8Exception(path, input.position().toLong(), null, "incomplete UTF-8 byte sequence")
        }
        output.flip()
        return output.toString()
    }

    private fun requireInlineSize(path: String, actualBytes: Long) {
        if (actualBytes > INLINE_PAYLOAD_MAX_BYTES) {
            throw InlinePayloadTooLargeException(path, actualBytes, INLINE_PAYLOAD_MAX_BYTES)
        }
    }

    private fun prefixUpperBound(normalizedPath: String): String =
        if (normalizedPath == "/") "0" else "${normalizedPath}0"

    private fun active() {
        manager.requireActiveFilesystem(filesystemUuid)
    }

}

private const val TREE_KEYSET_BATCH_SIZE = 2_048
private const val BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

private fun base64Value(character: Char): Int = BASE64_ALPHABET.indexOf(character)

private fun firstMalformedUnicodeIndex(text: String): Int? {
    var index = 0
    while (index < text.length) {
        val character = text[index]
        when {
            Character.isHighSurrogate(character) -> {
                if (index + 1 >= text.length || !Character.isLowSurrogate(text[index + 1])) return index
                index += 2
            }
            Character.isLowSurrogate(character) -> return index
            else -> index += 1
        }
    }
    return null
}
