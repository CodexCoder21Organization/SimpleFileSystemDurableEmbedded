package simplefilesystem.durable

import okio.Buffer
import okio.Sink
import okio.Source
import okio.buffer
import simplefilesystem.FileEntryInfo
import simplefilesystem.FileMetadataInfo
import simplefilesystem.InvalidByteRangeException
import simplefilesystem.InvalidPathException
import simplefilesystem.PathAlreadyExistsException
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import simplefilesystem.SimpleFileSystem
import java.io.InputStream
import java.util.Base64
import java.util.UUID

class DurableSimpleFileSystem internal constructor(
    private val manager: DurableSimpleFileSystemManager,
    private val filesystemUuid: UUID,
) : SimpleFileSystem {
    override fun list(path: String): List<FileEntryInfo> {
        active()
        val normalized = manager.normalizePath(path)
        manager.requireDirectory(manager.metadataDatabase, filesystemUuid, normalized)
        return manager.metadataDatabase.getRows(
            "SELECT * FROM entries WHERE filesystem_uuid = ? AND parent_path = ? ORDER BY path",
            filesystemUuid,
            normalized,
        ).map { it.toEntryRecord().toInfo() }
    }

    override fun listRecursively(path: String): List<FileEntryInfo> {
        active()
        val normalized = manager.normalizePath(path)
        manager.requireDirectory(manager.metadataDatabase, filesystemUuid, normalized)
        val prefix = if (normalized == "/") "/" else "$normalized/"
        return manager.metadataDatabase.getRows(
            """SELECT * FROM entries
                WHERE filesystem_uuid = ? AND path != ? AND left(path, ?) = ?
                ORDER BY path""".trimIndent(),
            filesystemUuid,
            normalized,
            prefix.length,
            prefix,
        ).map { it.toEntryRecord().toInfo() }
    }

    override fun createDirectory(path: String, mustCreate: Boolean) {
        createDirectoriesInternal(path, mustCreate, recursive = false)
    }

    override fun createDirectories(path: String, mustCreate: Boolean) {
        createDirectoriesInternal(path, mustCreate, recursive = true)
    }

    override fun read(path: String): String = Base64.getEncoder().encodeToString(readBytes(path))

    override fun readUtf8(path: String): String = readBytes(path).toString(Charsets.UTF_8)

    override fun write(path: String, data: String, ifMatches: String?) {
        val bytes = Base64.getDecoder().decode(data)
        sink(path, ifMatches).buffer().use { it.write(bytes) }
    }

    override fun writeUtf8(path: String, content: String, ifMatches: String?) {
        sink(path, ifMatches).buffer().use { it.writeUtf8(content) }
    }

    override fun overwrite(path: String, data: String) {
        val bytes = Base64.getDecoder().decode(data)
        unconditionalSink(path).buffer().use { it.write(bytes) }
    }

    override fun overwriteUtf8(path: String, content: String) {
        unconditionalSink(path).buffer().use { it.writeUtf8(content) }
    }

    override fun appendingWrite(path: String, data: String, mustExist: Boolean) {
        val normalized = manager.normalizePath(path)
        if (mustExist && !exists(normalized)) throw PathNotFoundException(normalized)
        val bytes = Base64.getDecoder().decode(data)
        appendingSink(normalized).buffer().use { it.write(bytes) }
    }

    override fun appendingWriteUtf8(path: String, content: String, mustExist: Boolean) {
        val normalized = manager.normalizePath(path)
        if (mustExist && !exists(normalized)) throw PathNotFoundException(normalized)
        appendingSink(normalized).buffer().use { it.writeUtf8(content) }
    }

    override fun metadata(path: String): FileMetadataInfo {
        active()
        val normalized = manager.normalizePath(path)
        return manager.requireEntry(manager.metadataDatabase, filesystemUuid, normalized).toMetadata()
    }

    override fun metadataOrNull(path: String): FileMetadataInfo? {
        active()
        val normalized = manager.normalizePath(path)
        return manager.findEntry(manager.metadataDatabase, filesystemUuid, normalized)?.toMetadata()
    }

    override fun exists(path: String): Boolean {
        active()
        val normalized = manager.normalizePath(path)
        return manager.findEntry(manager.metadataDatabase, filesystemUuid, normalized) != null
    }

    override fun delete(path: String, mustExist: Boolean) {
        val normalized = manager.normalizePath(path)
        if (normalized == "/") {
            throw InvalidPathException(path, "delete cannot remove the filesystem root; use deleteRecursively to clear it.")
        }
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            val entry = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (entry == null) {
                if (mustExist) throw PathNotFoundException(normalized)
                return@execute
            }
            if (entry.isDirectory) {
                val child = transaction.getRows(
                    "SELECT path FROM entries WHERE filesystem_uuid = ? AND parent_path = ? LIMIT 1",
                    filesystemUuid,
                    normalized,
                )
                if (child.isNotEmpty()) {
                    throw PathTypeMismatchException(
                        normalized,
                        "EMPTY_DIRECTORY",
                        "NON_EMPTY_DIRECTORY",
                    )
                }
            }
            entry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
            transaction.execute(
                "DELETE FROM entries WHERE filesystem_uuid = ? AND path = ?",
                filesystemUuid,
                normalized,
            )
            val released = entry.sizeBytes ?: 0L
            transaction.execute(
                "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                filesystem.usedBytes - released,
                filesystemUuid,
            )
        }
    }

    override fun deleteRecursively(path: String, mustExist: Boolean) {
        val normalized = manager.normalizePath(path)
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            val root = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (root == null) {
                if (mustExist) throw PathNotFoundException(normalized)
                return@execute
            }
            val entries = subtree(transaction, normalized)
                .filterNot { normalized == "/" && it.path == "/" }
            entries.filter { it.isFile }.forEach { entry ->
                entry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
            }
            val released = entries.sumOf { it.sizeBytes ?: 0L }
            entries.sortedByDescending { it.path.length }.forEach { entry ->
                transaction.execute(
                    "DELETE FROM entries WHERE filesystem_uuid = ? AND path = ?",
                    filesystemUuid,
                    entry.path,
                )
            }
            transaction.execute(
                "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                filesystem.usedBytes - released,
                filesystemUuid,
            )
        }
    }

    override fun copy(source: String, target: String) {
        val normalizedSource = manager.normalizePath(source)
        val normalizedTarget = manager.normalizePath(target)
        if (normalizedTarget == "/") throw InvalidPathException(target, "copy cannot replace the filesystem root.")
        if (normalizedSource == normalizedTarget) return
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            val sourceEntry = manager.requireEntry(transaction, filesystemUuid, normalizedSource, lock = true)
            if (!sourceEntry.isFile) {
                throw PathTypeMismatchException(normalizedSource, "FILE", sourceEntry.kind)
            }
            manager.requireDirectory(transaction, filesystemUuid, manager.parentPath(normalizedTarget), lock = true)
            val targetEntry = manager.findEntry(transaction, filesystemUuid, normalizedTarget, lock = true)
            if (targetEntry?.isDirectory == true) {
                throw PathTypeMismatchException(normalizedTarget, "FILE", "DIRECTORY")
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
            } else if (targetEntry.generationUuid != generation) {
                manager.retainGeneration(transaction, generation)
                targetEntry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
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
        }
    }

    override fun atomicMove(source: String, target: String) {
        val normalizedSource = manager.normalizePath(source)
        val normalizedTarget = manager.normalizePath(target)
        if (normalizedSource == "/" || normalizedTarget == "/") {
            throw InvalidPathException(
                if (normalizedSource == "/") source else target,
                "atomicMove cannot move or replace the filesystem root.",
            )
        }
        if (normalizedSource == normalizedTarget) return
        if (normalizedTarget.startsWith("$normalizedSource/") || normalizedSource.startsWith("$normalizedTarget/")) {
            throw InvalidPathException(
                target,
                "atomicMove cannot move a path into its own subtree or replace one of its ancestors.",
            )
        }
        manager.ensureSchema()
        manager.transactionally { transaction ->
            val filesystem = manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            manager.requireEntry(transaction, filesystemUuid, normalizedSource, lock = true)
            manager.requireDirectory(transaction, filesystemUuid, manager.parentPath(normalizedTarget), lock = true)
            val moving = subtree(transaction, normalizedSource)
            val replaced = manager.findEntry(transaction, filesystemUuid, normalizedTarget, lock = true)
                ?.let { subtree(transaction, normalizedTarget) }
                ?: emptyList()
            replaced.filter { it.isFile }.forEach { entry ->
                entry.generationUuid?.let { manager.releaseGeneration(transaction, it) }
            }
            replaced.sortedByDescending { it.path.length }.forEach { entry ->
                transaction.execute(
                    "DELETE FROM entries WHERE filesystem_uuid = ? AND path = ?",
                    filesystemUuid,
                    entry.path,
                )
            }
            moving.sortedBy { it.path.length }.forEach { entry ->
                val suffix = entry.path.removePrefix(normalizedSource)
                val newPath = normalizedTarget + suffix
                transaction.execute(
                    """UPDATE entries SET path = ?, parent_path = ?, name = ?
                        WHERE filesystem_uuid = ? AND path = ?""".trimIndent(),
                    newPath,
                    manager.parentPath(newPath),
                    manager.name(newPath),
                    filesystemUuid,
                    entry.path,
                )
            }
            val released = replaced.sumOf { it.sizeBytes ?: 0L }
            transaction.execute(
                "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                filesystem.usedBytes - released,
                filesystemUuid,
            )
        }
    }

    override fun source(path: String): Source {
        val snapshot = fileSnapshot(path)
        val generation = requireNotNull(snapshot.generationUuid)
        return GenerationSource(
            manager,
            manager.blocks(manager.metadataDatabase, generation),
            initialSkip = 0L,
            byteCount = requireNotNull(snapshot.sizeBytes),
        )
    }

    override fun source(path: String, offset: Long, byteCount: Long): Source {
        val snapshot = fileSnapshot(path)
        val size = requireNotNull(snapshot.sizeBytes)
        if (offset < 0L || byteCount < 0L || offset > size || byteCount > size - offset) {
            throw InvalidByteRangeException(snapshot.path, offset, byteCount, size)
        }
        if (byteCount == 0L) return GenerationSource(manager, emptyList(), 0L, 0L)
        val generation = requireNotNull(snapshot.generationUuid)
        val firstOrdinal = (offset / BLOCK_SIZE_BYTES).toInt()
        val lastOrdinal = ((offset + byteCount - 1L) / BLOCK_SIZE_BYTES).toInt()
        val relevant = manager.blocks(manager.metadataDatabase, generation)
            .filter { it.ordinal in firstOrdinal..lastOrdinal }
        return GenerationSource(
            manager,
            relevant,
            initialSkip = offset % BLOCK_SIZE_BYTES,
            byteCount = byteCount,
        )
    }

    override fun sink(path: String, ifMatches: String?): Sink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = ifMatches,
        unconditional = false,
        append = false,
    )

    override fun appendingSink(path: String): Sink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = null,
        unconditional = false,
        append = true,
    )

    override fun inputStream(path: String): InputStream = source(path).buffer().inputStream()

    private fun unconditionalSink(path: String): Sink = StagedBlockSink(
        manager = manager,
        filesystemUuid = filesystemUuid,
        rawPath = path,
        expectedHash = null,
        unconditional = true,
        append = false,
    )

    private fun createDirectoriesInternal(path: String, mustCreate: Boolean, recursive: Boolean) {
        val normalized = manager.normalizePath(path)
        if (normalized == "/") {
            if (mustCreate) throw PathAlreadyExistsException(normalized)
            return
        }
        manager.ensureSchema()
        manager.transactionally { transaction ->
            manager.requireActiveFilesystem(transaction, filesystemUuid, lock = true)
            val existing = manager.findEntry(transaction, filesystemUuid, normalized, lock = true)
            if (existing != null) {
                if (!existing.isDirectory) throw PathTypeMismatchException(normalized, "DIRECTORY", existing.kind)
                if (mustCreate) throw PathAlreadyExistsException(normalized)
                return@execute
            }
            val now = manager.clock.currentTimeMillis()
            if (!recursive) {
                manager.requireDirectory(transaction, filesystemUuid, manager.parentPath(normalized), lock = true)
                insertDirectory(transaction, normalized, now)
                return@execute
            }
            var current = ""
            normalized.removePrefix("/").split('/').forEach { component ->
                current += "/$component"
                val componentEntry = manager.findEntry(transaction, filesystemUuid, current, lock = true)
                if (componentEntry == null) {
                    insertDirectory(transaction, current, now)
                } else if (!componentEntry.isDirectory) {
                    throw PathTypeMismatchException(current, "DIRECTORY", componentEntry.kind)
                }
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

    private fun fileSnapshot(rawPath: String): EntryRecord {
        active()
        val normalized = manager.normalizePath(rawPath)
        val entry = manager.requireEntry(manager.metadataDatabase, filesystemUuid, normalized)
        if (!entry.isFile) throw PathTypeMismatchException(normalized, "FILE", entry.kind)
        return entry
    }

    private fun readBytes(path: String): ByteArray {
        val buffer = Buffer()
        source(path).use { source ->
            while (source.read(buffer, 8192L) != -1L) Unit
        }
        return buffer.readByteArray()
    }

    private fun active() {
        manager.requireActiveFilesystem(filesystemUuid)
    }

    private fun subtree(database: sql.Database, root: String): List<EntryRecord> {
        val prefix = if (root == "/") "/" else "$root/"
        return database.getRows(
            """SELECT * FROM entries
                WHERE filesystem_uuid = ? AND (path = ? OR left(path, ?) = ?)
                ORDER BY path""".trimIndent(),
            filesystemUuid,
            root,
            prefix.length,
            prefix,
        ).map { it.toEntryRecord() }
    }
}
