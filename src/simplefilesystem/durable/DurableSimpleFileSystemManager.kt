package simplefilesystem.durable

import blobstore.api.BlobstoreService
import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.Scheduled
import community.kotlin.clocks.simple.SystemClock
import simplefilesystem.FILESYSTEM_DESCRIPTION_MAX_UTF8_BYTES
import simplefilesystem.FileContentConflictException
import simplefilesystem.FileEntryType
import simplefilesystem.FileMetadataInfo
import simplefilesystem.FilesystemExpiredException
import simplefilesystem.FilesystemInfo
import simplefilesystem.FilesystemNotFoundException
import simplefilesystem.FilesystemPage
import simplefilesystem.InvalidCursorException
import simplefilesystem.InvalidContentHashException
import simplefilesystem.InvalidFilesystemDescriptionException
import simplefilesystem.InvalidFilesystemUuidException
import simplefilesystem.InvalidMaxSizeBytesException
import simplefilesystem.InvalidPageLimitException
import simplefilesystem.InvalidPathException
import simplefilesystem.InvalidPathReason
import simplefilesystem.InvalidWatchRevisionException
import simplefilesystem.MAX_PAGE_LIMIT
import simplefilesystem.PATH_MAX_DEPTH
import simplefilesystem.PATH_MAX_UTF8_BYTES
import simplefilesystem.PATH_SEGMENT_MAX_UTF8_BYTES
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathWatchPage
import simplefilesystem.PathWatchResyncRequiredException
import simplefilesystem.PathWatchTerminalReason
import simplefilesystem.PathTypeMismatchException
import simplefilesystem.QuotaArithmeticOverflowException
import simplefilesystem.QuotaExceededException
import simplefilesystem.SimpleFileSystem
import simplefilesystem.SimpleFileSystemException
import simplefilesystem.SimpleFileSystemManager
import sql.Database
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.sql.SQLException
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.TreeSet
import java.util.WeakHashMap

/**
 * Durable CockroachDB/Blobstore implementation of [SimpleFileSystemManager].
 *
 * Every transaction that needs more than one row lock follows one global order: orphan-inventory cursor (when
 * applicable), write sessions in UUID order, reader session, filesystem, namespace entries and generation blocks
 * in primary-key order, Blobstore GC-outbox hashes in lexical order, then manager revision. A path that needs only
 * a suffix of that order starts at that suffix; it never acquires an earlier class after a later one. External
 * Blobstore calls are idempotent and may be repeated by Cockroach serialization retries, while SQL publication and
 * cursor movement remain in the single retried transaction.
 *
 * Large block and subtree transitions retain one logical SQL commit. They keyset-page metadata in fixed-size
 * batches inside that transaction, so no caller can observe an intermediate namespace and heap usage is bounded by
 * [INTERNAL_KEYSET_BATCH_SIZE], independent of file or subtree cardinality.
 */
class DurableSimpleFileSystemManager(
    internal val blobstoreService: BlobstoreService,
    internal val metadataDatabase: Database,
    internal val clock: Clock = SystemClock(),
    private val sessionLeaseMillis: Long = DEFAULT_SESSION_LEASE_MILLIS,
    private val maintenanceIntervalMillis: Long? = null,
    private val maintenanceBatchSize: Int = DEFAULT_MAINTENANCE_BATCH_SIZE,
    private val namespaceEventRetentionCount: Int = DEFAULT_NAMESPACE_EVENT_RETENTION_COUNT,
    private val namespaceEventRetentionMillis: Long = DEFAULT_NAMESPACE_EVENT_RETENTION_MILLIS,
) : SimpleFileSystemManager, AutoCloseable {
    @Volatile
    private var schemaReady: Boolean = false
    @Volatile
    private var closed: Boolean = false
    private var scheduledMaintenance: Scheduled? = null

    init {
        require(sessionLeaseMillis > 0L) {
            "Write-session lease duration must be positive, but was $sessionLeaseMillis milliseconds."
        }
        require(maintenanceIntervalMillis == null || maintenanceIntervalMillis > 0L) {
            "Background maintenance interval must be positive when enabled, but was $maintenanceIntervalMillis milliseconds."
        }
        require(maintenanceBatchSize > 0) {
            "Maintenance batch size must be positive, but was $maintenanceBatchSize."
        }
        require(namespaceEventRetentionCount > 0) {
            "Namespace-event retention count must be positive, but was $namespaceEventRetentionCount."
        }
        require(namespaceEventRetentionMillis > 0L) {
            "Namespace-event retention age must be positive, but was $namespaceEventRetentionMillis milliseconds."
        }
        ensureSchema()
        reconcileInterruptedWork(maintenanceBatchSize)
        if (maintenanceIntervalMillis != null) scheduleNextMaintenance()
    }

    override fun createFilesystem(description: String, maxSizeBytes: Long): FilesystemInfo {
        ensureSchema()
        validateDescription(description)
        if (maxSizeBytes <= 0L) throw InvalidMaxSizeBytesException(maxSizeBytes)
        val uuid = UUID.randomUUID()
        val now = clock.currentTimeMillis()
        if (now < 0L) {
            throw SimpleFileSystemException(
                "Cannot create a filesystem: the injected clock reported currentTimeMillis=$now, but " +
                    "filesystem creation timestamps must be non-negative.",
            )
        }
        transactionally { transaction ->
            transaction.execute(
                """WITH inserted_filesystem AS (
                        INSERT INTO filesystems
                            (uuid, description, owner, max_size_bytes, used_bytes, expires_at_millis,
                             created_at_millis, namespace_revision)
                        VALUES (?, ?, NULL, ?, 0, NULL, ?, 0)
                        RETURNING uuid
                    ),
                    inserted_root AS (
                        INSERT INTO entries
                            (filesystem_uuid, path, parent_path, name, entry_kind, generation_uuid,
                             size_bytes, content_hash, created_at_millis, modified_at_millis)
                        SELECT uuid, '/', '', '', 'DIRECTORY', NULL, NULL, NULL, ?, ?
                        FROM inserted_filesystem
                        RETURNING filesystem_uuid
                    )
                    INSERT INTO namespace_event_streams
                        (filesystem_uuid, latest_revision, oldest_available_since_revision, terminal_reason)
                    SELECT filesystem_uuid, 0, 0, NULL FROM inserted_root""".trimIndent(),
                uuid,
                description,
                maxSizeBytes,
                now,
                now,
                now,
            )
            bumpManagerRevision(transaction)
        }
        return requireFilesystem(uuid).toInfo()
    }

    override fun listFilesystems(after: String?, limit: Int): FilesystemPage {
        ensureSchema()
        val cursor = parseFilesystemCursor(after)
        val pageLimit = validatePageLimit(limit)
        return transactionally { transaction ->
            val revision = managerRevision(transaction)
            val rows = if (cursor == null) {
                transaction.getRows(
                    "SELECT * FROM filesystems ORDER BY created_at_millis, CAST(uuid AS STRING) LIMIT ?",
                    pageLimit + 1,
                )
            } else {
                transaction.getRows(
                    """SELECT * FROM filesystems
                        WHERE created_at_millis > ?
                           OR (created_at_millis = ? AND CAST(uuid AS STRING) > ?)
                        ORDER BY created_at_millis, CAST(uuid AS STRING) LIMIT ?""".trimIndent(),
                    cursor.first,
                    cursor.first,
                    cursor.second.toString(),
                    pageLimit + 1,
                )
            }
            val selected = rows.take(pageLimit).map { it.toFilesystemRecord().toInfo() }
            FilesystemPageValue(
                filesystems = selected,
                nextAfter = selected.lastOrNull()?.let { "${it.createdAtMillis}:${it.uuid}" }
                    .takeIf { rows.size > pageLimit },
                snapshotRevision = revision,
            )
        }
    }

    override fun getFilesystemInfo(uuid: String): FilesystemInfo = requireFilesystem(parseUuid(uuid)).toInfo()

    override fun openFilesystem(uuid: String): SimpleFileSystem {
        val parsed = parseUuid(uuid)
        requireActiveFilesystem(parsed)
        return DurableSimpleFileSystem(this, parsed)
    }

    override fun watchFilesystem(uuid: String, path: String, sinceRevision: Long?, limit: Int): PathWatchPage {
        val parsed = parseUuid(uuid)
        val canonicalPath = normalizePath(path)
        val pageLimit = validatePageLimit(limit)
        ensureSchema()
        return transactionally { transaction ->
            var filesystem = transaction.getRows(
                "SELECT * FROM filesystems WHERE uuid = ? FOR UPDATE",
                parsed,
            ).firstOrNull()?.toFilesystemRecord()
            var stream = requireNamespaceEventStream(transaction, parsed, lock = true)

            if (filesystem != null && filesystem.expiresAtMillis?.let { clock.currentTimeMillis() >= it } == true &&
                stream.terminalReason != PathWatchTerminalReason.FILESYSTEM_EXPIRED
            ) {
                val revision = appendNamespaceEvents(
                    database = transaction,
                    filesystem = filesystem,
                    affectedPaths = emptyList(),
                    terminalReason = PathWatchTerminalReason.FILESYSTEM_EXPIRED,
                )
                filesystem = filesystem.copy(namespaceRevision = revision)
            }

            pruneNamespaceEvents(transaction, parsed)
            stream = requireNamespaceEventStream(transaction, parsed, lock = true)
            val retainedTerminal = transaction.getRows(
                """SELECT * FROM namespace_events WHERE filesystem_uuid = ? AND terminal = true
                    ORDER BY revision DESC LIMIT 1""".trimIndent(),
                parsed,
            ).firstOrNull()?.toNamespaceEventRecord() ?: stream.terminalReason?.let { reason ->
                NamespaceEventRecord(
                    filesystemUuid = parsed,
                    revision = stream.latestRevision,
                    canonicalPath = "/",
                    entryType = null,
                    exists = false,
                    isDirectory = false,
                    isRegularFile = false,
                    sizeBytes = null,
                    contentHash = null,
                    terminalReason = reason,
                )
            }

            val latestRevision = stream.latestRevision
            if (sinceRevision == null) {
                if (filesystem == null && retainedTerminal == null) {
                    throw FilesystemNotFoundException(uuid)
                }
                val initial = if (filesystem == null || stream.terminalReason != null) {
                    requireNotNull(retainedTerminal) {
                        "Terminal namespace-event stream '$parsed' had no retained terminal event."
                    }.toWatchEvent()
                } else {
                    namespaceSnapshot(
                        transaction,
                        parsed,
                        canonicalPath,
                        latestRevision,
                    ).toWatchEvent()
                }
                return@transactionally PathWatchPageValue(
                    events = listOf(initial),
                    nextSinceRevision = latestRevision,
                    hasMore = false,
                    latestRevision = latestRevision,
                )
            }

            if (sinceRevision < 0L || sinceRevision > latestRevision) {
                throw InvalidWatchRevisionException(uuid, sinceRevision, latestRevision)
            }
            if (sinceRevision < stream.oldestAvailableSinceRevision) {
                throw PathWatchResyncRequiredException(
                    uuid,
                    sinceRevision,
                    stream.oldestAvailableSinceRevision,
                    latestRevision,
                )
            }
            if (filesystem == null && retainedTerminal == null) {
                throw FilesystemNotFoundException(uuid)
            }
            val rows = transaction.getRows(
                """SELECT * FROM namespace_events
                    WHERE filesystem_uuid = ? AND revision > ?
                      AND (canonical_path = ? OR terminal = true)
                    ORDER BY revision, canonical_path LIMIT ?""".trimIndent(),
                parsed,
                sinceRevision,
                canonicalPath,
                pageLimit + 1,
            ).map { it.toNamespaceEventRecord().toWatchEvent() }
            val selected = rows.take(pageLimit)
            val hasMore = rows.size > pageLimit
            PathWatchPageValue(
                events = selected,
                nextSinceRevision = if (hasMore) requireNotNull(selected.lastOrNull()).revision else latestRevision,
                hasMore = hasMore,
                latestRevision = latestRevision,
            )
        }
    }

    override fun deleteFilesystem(uuid: String) {
        val parsed = parseUuid(uuid)
        ensureSchema()
        transactionally { transaction ->
            lockWriteSessionsForFilesystem(transaction, parsed)
            val filesystem = requireFilesystem(transaction, parsed, lock = true)
            purgeFilesystem(transaction, filesystem, PathWatchTerminalReason.FILESYSTEM_DELETED)
        }
    }

    override fun getExpiration(uuid: String): Long? = requireFilesystem(parseUuid(uuid)).expiresAtMillis

    override fun setExpiration(uuid: String, expiresAtMillis: Long?) {
        val parsed = parseUuid(uuid)
        ensureSchema()
        transactionally { transaction ->
            val filesystem = requireFilesystem(transaction, parsed, lock = true)
            if (filesystem.expiresAtMillis != expiresAtMillis) {
                transaction.execute(
                    "UPDATE filesystems SET expires_at_millis = ? WHERE uuid = ?",
                    expiresAtMillis,
                    parsed,
                )
                bumpManagerRevision(transaction)
                val stream = requireNamespaceEventStream(transaction, parsed, lock = true)
                if (expiresAtMillis != null && expiresAtMillis <= clock.currentTimeMillis()) {
                    if (stream.terminalReason != PathWatchTerminalReason.FILESYSTEM_EXPIRED) {
                        appendNamespaceEvents(
                            transaction,
                            filesystem,
                            emptyList(),
                            PathWatchTerminalReason.FILESYSTEM_EXPIRED,
                        )
                    }
                } else if (stream.terminalReason == PathWatchTerminalReason.FILESYSTEM_EXPIRED) {
                    transaction.execute(
                        "UPDATE namespace_event_streams SET terminal_reason = NULL WHERE filesystem_uuid = ?",
                        parsed,
                    )
                }
            }
        }
    }

    override fun getMaxSizeBytes(uuid: String): Long = requireFilesystem(parseUuid(uuid)).maxSizeBytes

    override fun getUsedBytes(uuid: String): Long = requireFilesystem(parseUuid(uuid)).usedBytes

    /**
     * Resolves at most [limit] durable Blobstore-unpin intents and returns the number of outbox rows resolved.
     *
     * The hash's outbox row is the mutex shared by the collector and every normal block writer. A writer pins
     * before entering its Cockroach transaction, locks (or creates) that row, pins once more after it owns the
     * lock, publishes the `file_blocks` reference, and removes the stale GC intent in that same transaction.
     * The collector locks the same row, checks every `file_blocks` row (committed generations and staged sessions
     * alike), performs the idempotent unpin only when the count is zero, re-verifies the count, and then removes
     * the intent. A serialization retry that observes a newly committed reference always re-pins before clearing
     * the stale intent, compensating for an unpin performed by an earlier transaction attempt.
     *
     * Consequently the dangerous `writer pin -> collector unpin -> writer commit` ordering is safe: when the
     * collector wins the row lock, the waiting writer's post-lock pin repairs the external pin before its SQL
     * reference commits; when the writer wins, the collector observes that reference and skips unpinning. An
     * outbox transaction may retry after an external unpin, so Blobstore unpin must remain idempotent, while the
     * row's deletion makes repeat maintenance calls no-ops after a successful commit.
     */
    fun processBlobGcOutbox(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): Int {
        validateMaintenanceLimit(limit)
        ensureSchema()
        return resolveBlobGcIntents(limit)
    }

    /** Reclaims expired open sessions and explicitly aborted sessions without changing filesystem quota. */
    fun reapAbandonedWriteSessions(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): Int {
        validateMaintenanceLimit(limit)
        ensureSchema()
        val now = clock.currentTimeMillis()
        val sessions = metadataDatabase.getUuids(
            """SELECT session_uuid FROM write_sessions
                WHERE state = 'ABORTED' OR (state = 'OPEN' AND lease_expires_at_millis <= ?)
                ORDER BY lease_expires_at_millis, session_uuid LIMIT ?""".trimIndent(),
            now,
            limit,
        )
        return sessions.count { sessionUuid -> reapSessionIfEligible(sessionUuid, now) }
    }

    /** Releases generation pins held by readers whose renewable lease has expired. */
    fun reapAbandonedReaderSessions(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): Int {
        validateMaintenanceLimit(limit)
        ensureSchema()
        val now = clock.currentTimeMillis()
        val readers = metadataDatabase.getUuids(
            """SELECT reader_uuid FROM reader_sessions
                WHERE state = 'OPEN' AND lease_expires_at_millis <= ?
                ORDER BY lease_expires_at_millis, reader_uuid LIMIT ?""".trimIndent(),
            now,
            limit,
        )
        return readers.count { readerUuid -> reapReaderIfEligible(readerUuid, now) }
    }

    /** Permanently removes filesystems whose expiration has passed, preserving revival until the row is locked. */
    fun purgeExpiredFilesystems(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): Int {
        validateMaintenanceLimit(limit)
        ensureSchema()
        val now = clock.currentTimeMillis()
        val filesystems = metadataDatabase.getUuids(
            """SELECT uuid FROM filesystems
                WHERE expires_at_millis IS NOT NULL AND expires_at_millis <= ?
                ORDER BY expires_at_millis, uuid LIMIT ?""".trimIndent(),
            now,
            limit,
        )
        return filesystems.count { uuid -> purgeFilesystemIfExpired(uuid, now) }
    }

    /** Reconciles crash leftovers that are safe to resolve at startup; ordinary expiration purge remains explicit. */
    fun reconcileInterruptedWork(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): MaintenanceResult {
        validateMaintenanceLimit(limit)
        ensureSchema()
        val now = clock.currentTimeMillis()
        val pinnedHashes = blobstoreService.listBlobs(BLOB_PIN_OWNER)
        val snapshot = reconciliationSnapshot(limit, now)
        var reaped = 0
        snapshot.maintenanceCandidates.forEach { (kind, uuid) ->
            reaped += when (kind) {
                WRITE_MAINTENANCE_CANDIDATE -> if (reapSessionIfEligible(uuid, now)) 1 else 0
                READER_MAINTENANCE_CANDIDATE -> if (reapReaderIfEligible(uuid, now)) 1 else 0
                else -> error("Unexpected reconciliation candidate kind '$kind'.")
            }
        }
        val inventoriedBlobs = pinnedHashes.isNotEmpty() || snapshot.orphanInventoryHighWater != null
        if (inventoriedBlobs) {
            enqueueUnreferencedPinnedBlobs(limit, pinnedHashes)
        }
        val resolved = if (reaped > 0 || inventoriedBlobs) {
            resolveBlobGcIntents(limit)
        } else if (snapshot.blobGcCandidates.isEmpty()) {
            0
        } else {
            transactionally { transaction ->
                resolveBlobGcIntents(transaction, snapshot.blobGcCandidates)
            }
        }
        return MaintenanceResult(
            reapedSessions = reaped,
            purgedFilesystems = 0,
            resolvedGcIntents = resolved,
        )
    }

    /** Runs one bounded pass of every maintenance operation. */
    fun runMaintenance(limit: Int = DEFAULT_MAINTENANCE_BATCH_SIZE): MaintenanceResult {
        validateMaintenanceLimit(limit)
        ensureSchema()
        val now = clock.currentTimeMillis()
        var reaped = 0
        var purged = 0
        maintenanceCandidates(limit, now, includeExpiredFilesystems = true).forEach { (kind, uuid) ->
            when (kind) {
                WRITE_MAINTENANCE_CANDIDATE -> if (reapSessionIfEligible(uuid, now)) reaped += 1
                READER_MAINTENANCE_CANDIDATE -> if (reapReaderIfEligible(uuid, now)) reaped += 1
                FILESYSTEM_MAINTENANCE_CANDIDATE -> if (purgeFilesystemIfExpired(uuid, now)) purged += 1
                else -> error("Unexpected maintenance candidate kind '$kind'.")
            }
        }
        enqueueUnreferencedPinnedBlobs(limit)
        val resolved = processBlobGcOutbox(limit)
        return MaintenanceResult(reaped, purged, resolved)
    }

    override fun close() {
        if (closed) return
        closed = true
        scheduledMaintenance?.cancel()
        scheduledMaintenance = null
    }

    internal fun ensureSchema() {
        if (schemaReady) return
        synchronized(this) {
            if (schemaReady) return
            if (cachedSchemaVersion(metadataDatabase) != CURRENT_SCHEMA_VERSION) {
                when (val storedVersion = storedSchemaVersion()) {
                    null -> metadataDatabase.execute(SCHEMA_INITIALIZATION_SQL)
                    CURRENT_SCHEMA_VERSION -> Unit
                    else -> throw IllegalStateException(
                        "Cannot open durable filesystem metadata schema version $storedVersion; " +
                            "this implementation supports version $CURRENT_SCHEMA_VERSION.",
                    )
                }
                cacheSchemaVersion(metadataDatabase, CURRENT_SCHEMA_VERSION)
            }
            schemaReady = true
        }
    }

    private fun storedSchemaVersion(): Int? {
        val versionTableExists = metadataDatabase.getRows(
            """SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'simple_filesystem_schema_version'
            )""".trimIndent(),
        ).single().results["exists"] == true
        if (!versionTableExists) return null
        return metadataDatabase.getRows(
            "SELECT schema_version FROM simple_filesystem_schema_version WHERE singleton = true",
        ).firstOrNull()?.intValue("schema_version")
    }

    internal fun <T> transactionally(operation: (Database) -> T): T {
        var retryCount = 0
        while (true) {
            try {
                return metadataDatabase.execute(operation)
            } catch (failure: SQLException) {
                failure.domainFailure()?.let { throw it }
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

    private fun SQLException.domainFailure(): RuntimeException? {
        var current: Throwable? = cause
        while (current != null) {
            if (current is RuntimeException) return current
            current = current.cause
        }
        return null
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
        val segments = if (path.startsWith('/')) path.substring(1).split('/') else emptyList()
        val reason = when {
            path.isEmpty() -> InvalidPathReason.EMPTY
            !path.startsWith('/') -> InvalidPathReason.NOT_ABSOLUTE
            path.length > 1 && path.endsWith('/') -> InvalidPathReason.TRAILING_SEPARATOR
            "//" in path -> InvalidPathReason.REPEATED_SEPARATOR
            segments.any { it == "." } -> InvalidPathReason.DOT_SEGMENT
            segments.any { it == ".." } -> InvalidPathReason.PARENT_SEGMENT
            '\u0000' in path -> InvalidPathReason.NUL_CHARACTER
            firstMalformedUnicodeIndex(path) != null -> InvalidPathReason.MALFORMED_UNICODE
            segments.any { strictUtf8Size(it) > PATH_SEGMENT_MAX_UTF8_BYTES } -> InvalidPathReason.SEGMENT_TOO_LONG
            strictUtf8Size(path) > PATH_MAX_UTF8_BYTES -> InvalidPathReason.PATH_TOO_LONG
            path != "/" && segments.size > PATH_MAX_DEPTH -> InvalidPathReason.TOO_DEEP
            else -> null
        }
        if (reason != null) throw InvalidPathException(path, reason)
        return path
    }

    internal fun parentPath(path: String): String = if (path == "/") "" else path.substringBeforeLast('/').ifEmpty { "/" }

    internal fun name(path: String): String = if (path == "/") "" else path.substringAfterLast('/')

    internal fun validateExpectedHash(expectedHash: String?) {
        if (expectedHash != null && !EXPECTED_HASH.matches(expectedHash)) {
            throw InvalidContentHashException(expectedHash)
        }
    }

    internal fun validatePageLimit(limit: Int): Int {
        if (limit <= 0) throw InvalidPageLimitException(limit)
        return minOf(limit, MAX_PAGE_LIMIT)
    }

    internal fun validateListCursor(directory: String, after: String?, recursive: Boolean): String? {
        if (after == null) return null
        try {
            normalizePath(after)
        } catch (failure: InvalidPathException) {
            throw InvalidCursorException(
                after,
                "expected a canonical absolute path previously returned as nextAfter. " +
                    "The cursor path is invalid: ${failure.reason.name}.",
            )
        }
        val descendant = directory == "/" && after != "/" ||
            directory != "/" && after.startsWith("$directory/")
        val directChild = descendant && parentPath(after) == directory
        if (!descendant || (!recursive && !directChild)) {
            val scope = if (recursive) "a descendant" else "a direct child"
            throw InvalidCursorException(
                after,
                "expected a canonical absolute path previously returned as nextAfter. " +
                    "The cursor must be $scope of directory '$directory'.",
            )
        }
        return after
    }

    internal fun validateIntermediateComponents(
        database: Database,
        filesystemUuid: UUID,
        path: String,
        lock: Boolean = false,
    ) {
        if (path == "/") return
        var current = ""
        path.removePrefix("/").split('/').dropLast(1).forEach { component ->
            current += "/$component"
            val entry = findEntry(database, filesystemUuid, current, lock) ?: return
            if (!entry.isDirectory) {
                throw PathTypeMismatchException(current, FileEntryType.DIRECTORY, entry.entryType)
            }
        }
    }

    internal fun requireDirectoryPath(
        database: Database,
        filesystemUuid: UUID,
        path: String,
        lock: Boolean = false,
    ): EntryRecord {
        validateIntermediateComponents(database, filesystemUuid, path, lock)
        return requireDirectory(database, filesystemUuid, path, lock)
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
        if (!entry.isDirectory) {
            throw PathTypeMismatchException(path, FileEntryType.DIRECTORY, entry.entryType)
        }
        return entry
    }

    internal fun block(generationUuid: UUID, ordinal: Int): BlockRecord? = metadataDatabase.getRows(
        "SELECT * FROM file_blocks WHERE generation_uuid = ? AND ordinal = ? LIMIT 1",
        generationUuid,
        ordinal,
    ).firstOrNull()?.toBlockRecord()

    internal fun fileGenerationSnapshot(filesystemUuid: UUID, path: String): FileGenerationSnapshot {
        ensureSchema()
        return transactionally { transaction ->
            requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            validateIntermediateComponents(transaction, filesystemUuid, path)
            val entry = requireEntry(transaction, filesystemUuid, path)
            if (!entry.isFile) {
                throw PathTypeMismatchException(path, FileEntryType.REGULAR_FILE, entry.entryType)
            }
            val generation = requireNotNull(entry.generationUuid)
            retainGeneration(transaction, generation)
            val readerUuid = UUID.randomUUID()
            transaction.execute(
                """INSERT INTO reader_sessions
                    (reader_uuid, generation_uuid, lease_expires_at_millis, state)
                    VALUES (?, ?, ?, 'OPEN')""".trimIndent(),
                readerUuid,
                generation,
                leaseDeadline(clock.currentTimeMillis()),
            )
            FileGenerationSnapshot(entry, generation, readerUuid)
        }
    }

    internal fun renewReaderSession(readerUuid: UUID) {
        transactionally { transaction ->
            transaction.execute(
                """UPDATE reader_sessions SET lease_expires_at_millis = ?
                    WHERE reader_uuid = ? AND state = 'OPEN'""".trimIndent(),
                leaseDeadline(clock.currentTimeMillis()),
                readerUuid,
            )
        }
    }

    internal fun releaseReaderSession(readerUuid: UUID) {
        transactionally { transaction ->
            val reader = transaction.getRows(
                "SELECT generation_uuid, state FROM reader_sessions WHERE reader_uuid = ? FOR UPDATE",
                readerUuid,
            ).firstOrNull() ?: return@transactionally
            if (reader.stringValue("state") != "OPEN") return@transactionally
            releaseGeneration(transaction, reader.uuidValue("generation_uuid"))
            transaction.execute(
                "UPDATE reader_sessions SET state = 'RELEASED' WHERE reader_uuid = ? AND state = 'OPEN'",
                readerUuid,
            )
        }
    }

    internal fun beginSession(filesystemUuid: UUID, rawPath: String, expectedHash: String?): Pair<UUID, String> {
        ensureSchema()
        val path = normalizePath(rawPath)
        validateExpectedHash(expectedHash)
        val session = UUID.randomUUID()
        transactionally { transaction ->
            requireActiveFilesystem(transaction, filesystemUuid, lock = false)
            if (path != "/") requireDirectoryPath(transaction, filesystemUuid, parentPath(path))
            val now = clock.currentTimeMillis()
            transaction.execute(
                """INSERT INTO write_sessions
                    (session_uuid, filesystem_uuid, path, expected_hash, bytes_received, created_at_millis,
                     lease_expires_at_millis, state)
                    VALUES (?, ?, ?, ?, 0, ?, ?, 'OPEN')""".trimIndent(),
                session,
                filesystemUuid,
                path,
                expectedHash,
                now,
                leaseDeadline(now),
            )
        }
        return session to path
    }

    internal fun stageBlock(
        database: Database,
        generationUuid: UUID,
        ordinal: Int,
        bytes: ByteArray,
    ): BlockRecord {
        val hash = uploadAndPinBlock(bytes)
        return publishPinnedBlock(database, generationUuid, ordinal, hash, bytes.size)
    }

    private fun uploadAndPinBlock(bytes: ByteArray): String {
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
        return hash
    }

    private fun publishPinnedBlock(
        database: Database,
        generationUuid: UUID,
        ordinal: Int,
        hash: String,
        sizeBytes: Int,
    ): BlockRecord {
        prepareBlobReference(database, hash)
        database.execute(
            """WITH inserted_block AS (
                    INSERT INTO file_blocks
                        (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                    VALUES (?, ?, ?, ?, 0)
                    RETURNING generation_uuid
                )
                DELETE FROM blob_gc_outbox
                WHERE blob_hash = ?
                  AND EXISTS (SELECT 1 FROM inserted_block)""".trimIndent(),
            generationUuid,
            ordinal,
            hash,
            sizeBytes,
            hash,
        )
        return BlockRecord(generationUuid, ordinal, hash, sizeBytes, 0L)
    }

    internal fun stageSessionBlock(
        sessionUuid: UUID,
        ordinal: Int,
        bytes: ByteArray,
        bytesReceived: Long,
    ): BlockRecord {
        val hash = uploadAndPinBlock(bytes)
        return try {
            transactionally { transaction ->
                renewSessionLeaseRow(transaction, sessionUuid, bytesReceived)
                val block = publishPinnedBlock(transaction, sessionUuid, ordinal, hash, bytes.size)
                block
            }
        } catch (failure: Throwable) {
            try {
                metadataDatabase.execute { transaction -> enqueueBlobForGc(transaction, hash) }
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    internal fun renewSessionLeaseIfDue(sessionUuid: UUID, renewAtMillis: Long): Long {
        val now = clock.currentTimeMillis()
        if (now < renewAtMillis) return renewAtMillis
        transactionally { transaction ->
            renewSessionLeaseRow(transaction, sessionUuid, bytesReceived = null, observedNow = now)
        }
        return leaseRenewalThreshold(now)
    }

    internal fun sessionLeaseRenewalThreshold(): Long = leaseRenewalThreshold(clock.currentTimeMillis())

    internal fun updateSessionBytes(sessionUuid: UUID, bytesReceived: Long) {
        transactionally { transaction ->
            requireRenewableSession(transaction, sessionUuid)
            transaction.execute(
                """UPDATE write_sessions SET bytes_received = ?, lease_expires_at_millis = ?
                    WHERE session_uuid = ? AND state = 'OPEN'""".trimIndent(),
                bytesReceived,
                leaseDeadline(clock.currentTimeMillis()),
                sessionUuid,
            )
        }
    }

    internal fun abortSession(sessionUuid: UUID) {
        metadataDatabase.execute(
            "UPDATE write_sessions SET state = 'ABORTED' WHERE session_uuid = ? AND state = 'OPEN'",
            sessionUuid,
        )
    }

    internal fun commitGeneration(stage: StagedGeneration, unconditional: Boolean): FileMetadataInfo {
        return try {
            transactionally { transaction ->
                claimAndVerifyStagedGeneration(transaction, stage)
                val filesystem = requireActiveFilesystem(transaction, stage.filesystemUuid, lock = true)
                if (stage.path != "/") {
                    requireDirectoryPath(
                        transaction,
                        stage.filesystemUuid,
                        parentPath(stage.path),
                        lock = true,
                    )
                }
                val current = findEntry(transaction, stage.filesystemUuid, stage.path, lock = true)
                if (current != null && !current.isFile) {
                    throw PathTypeMismatchException(stage.path, FileEntryType.REGULAR_FILE, current.entryType)
                }
                if (!unconditional) verifyConditionalWrite(stage.path, stage.expectedHash, current)
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
                recordNamespaceMutation(
                    transaction,
                    filesystem,
                    attemptedUsage,
                    listOf(stage.path) + if (current == null) listOf(parentPath(stage.path)) else emptyList(),
                )
                val committed = transaction.execute(
                    """UPDATE write_sessions SET state = 'COMMITTED', bytes_received = ?
                        WHERE session_uuid = ? AND state = 'OPEN'""".trimIndent(),
                    stage.sizeBytes,
                    stage.sessionUuid,
                )
                checkUpdateCount(committed, 1, "commit write session '${stage.sessionUuid}'")
                requireEntry(transaction, stage.filesystemUuid, stage.path).toMetadata()
            }
        } catch (failure: Throwable) {
            try {
                abortSession(stage.sessionUuid)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    internal fun commitAppend(stage: StagedGeneration, mustExist: Boolean): FileMetadataInfo {
        val assembledGenerationUuid = UUID.randomUUID()
        return try {
            transactionally { transaction ->
                claimAndVerifyStagedGeneration(transaction, stage)
                val filesystem = requireActiveFilesystem(transaction, stage.filesystemUuid, lock = true)
                if (stage.path != "/") {
                    requireDirectoryPath(
                        transaction,
                        stage.filesystemUuid,
                        parentPath(stage.path),
                        lock = true,
                    )
                }
                val current = findEntry(transaction, stage.filesystemUuid, stage.path, lock = true)
                if (mustExist && current == null) throw PathNotFoundException(stage.path)
                if (current != null && !current.isFile) {
                    throw PathTypeMismatchException(stage.path, FileEntryType.REGULAR_FILE, current.entryType)
                }
                val oldSize = current?.sizeBytes ?: 0L
                val appendedSize = try {
                    Math.addExact(oldSize, stage.sizeBytes)
                } catch (_: ArithmeticException) {
                    throw QuotaArithmeticOverflowException(stage.path, filesystem.usedBytes, oldSize, stage.sizeBytes)
                }
                val attemptedUsage = checkedAttemptedUsage(filesystem, stage.path, oldSize, appendedSize)
                val assembler = TransactionalBlockAssembler(this, transaction, assembledGenerationUuid)
                current?.generationUuid?.let { generation ->
                    forEachGenerationBlock(transaction, generation) { block -> assembler.append(block) }
                }
                forEachGenerationBlock(transaction, stage.sessionUuid) { block -> assembler.append(block) }
                val final = assembler.finish()
                check(final.first == appendedSize) {
                    "Append assembly for path '${stage.path}' produced ${final.first} bytes, but $appendedSize bytes " +
                        "were required by the committed old and staged lengths."
                }
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
                        assembledGenerationUuid,
                        final.first,
                        final.second,
                        now,
                        now,
                    )
                } else {
                    transaction.execute(
                        """UPDATE entries SET generation_uuid = ?, size_bytes = ?, content_hash = ?,
                            modified_at_millis = ? WHERE filesystem_uuid = ? AND path = ?""".trimIndent(),
                        assembledGenerationUuid,
                        final.first,
                        final.second,
                        now,
                        stage.filesystemUuid,
                        stage.path,
                    )
                }
                transaction.execute(
                    "UPDATE file_blocks SET reference_count = 1 WHERE generation_uuid = ?",
                    assembledGenerationUuid,
                )
                current?.generationUuid?.let { releaseGeneration(transaction, it) }
                deleteGenerationBlocks(transaction, stage.sessionUuid)
                transaction.execute(
                    "UPDATE filesystems SET used_bytes = ? WHERE uuid = ?",
                    attemptedUsage,
                    stage.filesystemUuid,
                )
                recordNamespaceMutation(
                    transaction,
                    filesystem,
                    attemptedUsage,
                    listOf(stage.path) + if (current == null) listOf(parentPath(stage.path)) else emptyList(),
                )
                val committed = transaction.execute(
                    """UPDATE write_sessions SET state = 'COMMITTED', bytes_received = ?
                        WHERE session_uuid = ? AND state = 'OPEN'""".trimIndent(),
                    stage.sizeBytes,
                    stage.sessionUuid,
                )
                checkUpdateCount(committed, 1, "commit append session '${stage.sessionUuid}'")
                requireEntry(transaction, stage.filesystemUuid, stage.path).toMetadata()
            }
        } catch (failure: Throwable) {
            try {
                abortSession(stage.sessionUuid)
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            throw failure
        }
    }

    private fun verifyConditionalWrite(path: String, expectedHash: String?, current: EntryRecord?) {
        val observed = current?.contentHash
        val matches = if (expectedHash == null) current == null else current != null && observed == expectedHash
        if (!matches) throw FileContentConflictException(path, expectedHash, observed)
    }

    /**
     * Global writer lock order starts with the write-session row, then the filesystem row, entry rows, and
     * finally per-hash GC-outbox rows. Holding the OPEN session row through publication makes reaping and
     * committing mutually exclusive across independently connected manager instances.
     */
    private fun claimAndVerifyStagedGeneration(database: Database, stage: StagedGeneration) {
        val session = database.getRows(
            """SELECT filesystem_uuid, path, expected_hash, bytes_received, state
                FROM write_sessions WHERE session_uuid = ? FOR UPDATE""".trimIndent(),
            stage.sessionUuid,
        ).firstOrNull() ?: throw SimpleFileSystemException(
            "Write session '${stage.sessionUuid}' no longer exists and cannot publish path '${stage.path}'.",
        )
        val state = session.stringValue("state")
        if (state != "OPEN") {
            throw SimpleFileSystemException(
                "Write session '${stage.sessionUuid}' cannot publish path '${stage.path}' because its state is " +
                    "'$state', not 'OPEN'.",
            )
        }
        val storedFilesystem = session.uuidValue("filesystem_uuid")
        val storedPath = session.stringValue("path")
        val storedExpectedHash = session.nullableStringValue("expected_hash")
        val storedBytes = session.longValue("bytes_received")
        if (storedFilesystem != stage.filesystemUuid || storedPath != stage.path ||
            storedExpectedHash != stage.expectedHash || storedBytes != stage.sizeBytes
        ) {
            throw SimpleFileSystemException(
                "Write session '${stage.sessionUuid}' publication metadata changed unexpectedly: expected " +
                    "filesystem='${stage.filesystemUuid}', path='${stage.path}', expectedHash=${stage.expectedHash}, " +
                    "bytes=${stage.sizeBytes}; stored filesystem='$storedFilesystem', path='$storedPath', " +
                    "expectedHash=$storedExpectedHash, bytes=$storedBytes.",
            )
        }

        val expectedBlockCount = if (stage.sizeBytes == 0L) 0L else
            ((stage.sizeBytes - 1L) / BLOCK_SIZE_BYTES.toLong()) + 1L
        val digest = MessageDigest.getInstance("SHA-256")
        var observedTotal = 0L
        var observedBlockCount = 0L
        forEachGenerationBlock(database, stage.sessionUuid) { block ->
            if (block.ordinal.toLong() != observedBlockCount) {
                throw SimpleFileSystemException(
                    "Write session '${stage.sessionUuid}' cannot publish path '${stage.path}': expected staged " +
                        "block ordinal $observedBlockCount, but found ordinal ${block.ordinal}.",
                )
            }
            var observedBlock = 0L
            blobstoreService.getBlob(BLOB_PIN_OWNER, block.blobHash).use { input ->
                val transfer = ByteArray(8192)
                while (true) {
                    val count = input.read(transfer)
                    if (count == -1) break
                    if (count == 0) continue
                    digest.update(transfer, 0, count)
                    observedBlock = Math.addExact(observedBlock, count.toLong())
                }
            }
            if (observedBlock != block.sizeBytes.toLong()) {
                throw SimpleFileSystemException(
                    "Write session '${stage.sessionUuid}' block ${block.ordinal} declares ${block.sizeBytes} bytes, " +
                        "but Blobstore returned $observedBlock bytes for hash '${block.blobHash}'.",
                )
            }
            observedTotal = Math.addExact(observedTotal, observedBlock)
            observedBlockCount += 1L
        }
        if (observedBlockCount != expectedBlockCount) {
            throw SimpleFileSystemException(
                "Write session '${stage.sessionUuid}' cannot publish path '${stage.path}': expected " +
                    "$expectedBlockCount contiguous staged block(s) for ${stage.sizeBytes} bytes, but found " +
                    "$observedBlockCount block(s).",
            )
        }
        val observedHash = digest.digest().toUpperHex()
        if (observedTotal != stage.sizeBytes || observedHash != stage.contentHash) {
            throw SimpleFileSystemException(
                "Write session '${stage.sessionUuid}' cannot publish path '${stage.path}': staged content was " +
                    "$observedTotal bytes with SHA-256 $observedHash, but the sink declared ${stage.sizeBytes} bytes " +
                    "with SHA-256 ${stage.contentHash}.",
            )
        }
    }

    private fun checkUpdateCount(result: sql.DatabaseRow, expected: Int, action: String) {
        val observed = affectedRowCount(result, action)
        check(observed == expected) {
            "Expected to $action by updating exactly $expected row(s), but CockroachDB reported $observed row(s)."
        }
    }

    private fun affectedRowCount(result: sql.DatabaseRow, action: String): Int {
        return (result.results["UPDATE_COUNT"] as? Number)?.toInt()
            ?: error("CockroachDB did not report an update count while attempting to $action.")
    }

    internal fun checkedAttemptedUsage(
        filesystem: FilesystemRecord,
        path: String,
        oldSize: Long,
        newSize: Long,
    ): Long {
        val attempted = try {
            Math.addExact(Math.subtractExact(filesystem.usedBytes, oldSize), newSize).also {
                if (it < 0L) throw ArithmeticException("negative usage")
            }
        } catch (_: ArithmeticException) {
            throw QuotaArithmeticOverflowException(
                path,
                filesystem.usedBytes,
                oldSize,
                newSize,
            )
        }
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
        val first = database.getRows(
            "SELECT reference_count FROM file_blocks WHERE generation_uuid = ? ORDER BY ordinal LIMIT 1",
            generationUuid,
        ).firstOrNull() ?: return
        val references = first.longValue("reference_count")
        if (references <= 0L) return
        if (references == 1L) {
            deleteGenerationBlocks(database, generationUuid)
        } else {
            database.execute(
                "UPDATE file_blocks SET reference_count = reference_count - 1 WHERE generation_uuid = ?",
                generationUuid,
            )
        }
    }

    internal fun prepareBlobReference(database: Database, hash: String) {
        // INSERT .. ON CONFLICT DO UPDATE already holds the outbox row lock until this transaction
        // completes. A following SELECT .. FOR UPDATE only repeated the same lock acquisition.
        enqueueBlobForGc(database, hash)
        if (!blobstoreService.pinBlob(BLOB_PIN_OWNER, hash)) {
            throw IllegalStateException(
                "Blob '$hash' disappeared after upload while acquiring its GC coordination lock; " +
                    "the required '$BLOB_PIN_OWNER' durability pin could not be renewed before metadata commit.",
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

    private fun resolveBlobGcIntents(limit: Int): Int = transactionally { transaction ->
        val candidates = transaction.getStrings(
            "SELECT blob_hash FROM blob_gc_outbox ORDER BY created_at_millis, blob_hash LIMIT ?",
            limit,
        )
        resolveBlobGcIntents(transaction, candidates)
    }

    private fun resolveBlobGcIntents(transaction: Database, candidates: List<String>): Int {
        if (candidates.isEmpty()) return 0
        val candidatePlaceholders = candidates.joinToString(", ") { "?" }
        val hashes = transaction.getStrings(
            """SELECT blob_hash FROM blob_gc_outbox
                WHERE blob_hash IN ($candidatePlaceholders)
                ORDER BY blob_hash FOR UPDATE""".trimIndent(),
            *candidates.toTypedArray(),
        )
        if (hashes.isEmpty()) return 0

        val placeholders = hashes.joinToString(", ") { "?" }
        val referencesBefore = transaction.getRows(
            """SELECT blob_hash, count(*) AS reference_count FROM file_blocks
                WHERE blob_hash IN ($placeholders) GROUP BY blob_hash""".trimIndent(),
            *hashes.toTypedArray(),
        ).associate { it.stringValue("blob_hash") to it.longValue("reference_count") }
        hashes.forEach { hash ->
            val references = referencesBefore[hash] ?: 0L
            if (references > 0L) {
                if (!blobstoreService.pinBlob(BLOB_PIN_OWNER, hash)) {
                    throw IllegalStateException(
                        "Blob '$hash' has $references live SQL reference(s), but Blobstore refused the " +
                            "required compensating '$BLOB_PIN_OWNER' pin while resolving its stale GC intent.",
                    )
                }
            } else {
                blobstoreService.unpinBlob(BLOB_PIN_OWNER, hash)
            }
        }

        val referencesAfter = transaction.getRows(
            """SELECT blob_hash, count(*) AS reference_count FROM file_blocks
                WHERE blob_hash IN ($placeholders) GROUP BY blob_hash""".trimIndent(),
            *hashes.toTypedArray(),
        ).associate { it.stringValue("blob_hash") to it.longValue("reference_count") }
        hashes.forEach { hash ->
            val references = referencesAfter[hash] ?: 0L
            if (references > 0L && !blobstoreService.pinBlob(BLOB_PIN_OWNER, hash)) {
                throw IllegalStateException(
                    "Blob '$hash' gained $references SQL reference(s) during GC re-verification, but " +
                        "Blobstore refused the compensating '$BLOB_PIN_OWNER' pin.",
                )
            }
        }
        transaction.execute(
            "DELETE FROM blob_gc_outbox WHERE blob_hash IN ($placeholders)",
            *hashes.toTypedArray(),
        )
        return hashes.size
    }

    private fun enqueueUnreferencedPinnedBlobs(
        limit: Int,
        inventory: List<String> = blobstoreService.listBlobs(BLOB_PIN_OWNER),
    ): Int {
        return transactionally { transaction ->
            val highWater = transaction.getRows(
                "SELECT orphan_inventory_high_water FROM simple_filesystem_maintenance_state " +
                    "WHERE singleton = true FOR UPDATE",
            ).single().nullableStringValue("orphan_inventory_high_water")
            val candidates = TreeSet<String>()
            // BlobstoreApi currently returns a materialized List. We cannot prevent that allocation in the client,
            // but deliberately neither copy nor sort it: only this bounded lexical window is retained locally.
            inventory.forEach { hash ->
                if ((highWater == null || hash > highWater) && candidates.add(hash) && candidates.size > limit) {
                    candidates.pollLast()
                }
            }
            if (candidates.isEmpty()) {
                if (highWater != null) transaction.execute(
                    "UPDATE simple_filesystem_maintenance_state SET orphan_inventory_high_water = NULL " +
                        "WHERE singleton = true",
                )
                return@transactionally 0
            }
            val candidateValues = candidates.joinToString(", ") { "(?::STRING)" }
            val candidateArguments = candidates.toTypedArray()
            val enqueued = transaction.getStrings(
                """INSERT INTO blob_gc_outbox (blob_hash, action, created_at_millis)
                    SELECT candidate.blob_hash, 'UNPIN_IF_UNREFERENCED', ?
                    FROM (VALUES $candidateValues) AS candidate(blob_hash)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM file_blocks WHERE blob_hash = candidate.blob_hash
                    )
                    ON CONFLICT (blob_hash) DO UPDATE
                    SET action = excluded.action, created_at_millis = excluded.created_at_millis
                    RETURNING blob_hash""".trimIndent(),
                clock.currentTimeMillis(),
                *candidateArguments,
            ).size
            transaction.execute(
                "UPDATE simple_filesystem_maintenance_state SET orphan_inventory_high_water = ? " +
                    "WHERE singleton = true",
                candidates.last(),
            )
            enqueued
        }
    }

    private fun reconciliationSnapshot(limit: Int, observedNow: Long): ReconciliationSnapshot {
        val rows = metadataDatabase.getRows(
            """SELECT '$WRITE_MAINTENANCE_CANDIDATE' AS candidate_kind,
                      session_uuid AS candidate_uuid,
                      NULL::STRING AS candidate_value
                FROM (
                    SELECT session_uuid FROM write_sessions
                    WHERE state = 'ABORTED' OR (state = 'OPEN' AND lease_expires_at_millis <= ?)
                    ORDER BY lease_expires_at_millis, session_uuid LIMIT ?
                ) AS abandoned_writes
                UNION ALL
                SELECT '$READER_MAINTENANCE_CANDIDATE' AS candidate_kind,
                       reader_uuid AS candidate_uuid,
                       NULL::STRING AS candidate_value
                FROM (
                    SELECT reader_uuid FROM reader_sessions
                    WHERE state = 'OPEN' AND lease_expires_at_millis <= ?
                    ORDER BY lease_expires_at_millis, reader_uuid LIMIT ?
                ) AS abandoned_readers
                UNION ALL
                SELECT '$ORPHAN_HIGH_WATER_CANDIDATE' AS candidate_kind,
                       NULL::UUID AS candidate_uuid,
                       orphan_inventory_high_water AS candidate_value
                FROM simple_filesystem_maintenance_state
                WHERE singleton = true
                UNION ALL
                SELECT '$BLOB_GC_MAINTENANCE_CANDIDATE' AS candidate_kind,
                       NULL::UUID AS candidate_uuid,
                       blob_hash AS candidate_value
                FROM (
                    SELECT blob_hash FROM blob_gc_outbox
                    ORDER BY created_at_millis, blob_hash LIMIT ?
                ) AS pending_blob_gc""".trimIndent(),
            observedNow,
            limit,
            observedNow,
            limit,
            limit,
        )
        val maintenanceCandidates = mutableListOf<Pair<String, UUID>>()
        val blobGcCandidates = mutableListOf<String>()
        var orphanInventoryHighWater: String? = null
        rows.forEach { row ->
            when (val kind = row.stringValue("candidate_kind")) {
                WRITE_MAINTENANCE_CANDIDATE, READER_MAINTENANCE_CANDIDATE ->
                    maintenanceCandidates += kind to row.uuidValue("candidate_uuid")
                ORPHAN_HIGH_WATER_CANDIDATE ->
                    orphanInventoryHighWater = row.nullableStringValue("candidate_value")
                BLOB_GC_MAINTENANCE_CANDIDATE ->
                    blobGcCandidates += row.stringValue("candidate_value")
                else -> error("Unexpected reconciliation candidate kind '$kind'.")
            }
        }
        return ReconciliationSnapshot(
            maintenanceCandidates,
            orphanInventoryHighWater,
            blobGcCandidates,
        )
    }

    private fun maintenanceCandidates(
        limit: Int,
        observedNow: Long,
        includeExpiredFilesystems: Boolean,
    ): List<Pair<String, UUID>> {
        val expirationQuery = if (includeExpiredFilesystems) {
            """UNION ALL
                SELECT '$FILESYSTEM_MAINTENANCE_CANDIDATE' AS candidate_kind, uuid AS candidate_uuid
                FROM (
                    SELECT uuid FROM filesystems
                    WHERE expires_at_millis IS NOT NULL AND expires_at_millis <= ?
                    ORDER BY expires_at_millis, uuid LIMIT ?
                ) AS expired_filesystems""".trimIndent()
        } else {
            ""
        }
        val arguments = mutableListOf<Any>(observedNow, limit, observedNow, limit)
        if (includeExpiredFilesystems) arguments.addAll(listOf(observedNow, limit))
        return metadataDatabase.getRows(
            """SELECT '$WRITE_MAINTENANCE_CANDIDATE' AS candidate_kind,
                      session_uuid AS candidate_uuid
                FROM (
                    SELECT session_uuid FROM write_sessions
                    WHERE state = 'ABORTED' OR (state = 'OPEN' AND lease_expires_at_millis <= ?)
                    ORDER BY lease_expires_at_millis, session_uuid LIMIT ?
                ) AS abandoned_writes
                UNION ALL
                SELECT '$READER_MAINTENANCE_CANDIDATE' AS candidate_kind,
                       reader_uuid AS candidate_uuid
                FROM (
                    SELECT reader_uuid FROM reader_sessions
                    WHERE state = 'OPEN' AND lease_expires_at_millis <= ?
                    ORDER BY lease_expires_at_millis, reader_uuid LIMIT ?
                ) AS abandoned_readers
                $expirationQuery""".trimIndent(),
            *arguments.toTypedArray(),
        ).map { row ->
            row.stringValue("candidate_kind") to row.uuidValue("candidate_uuid")
        }
    }

    private fun reapSessionIfEligible(sessionUuid: UUID, observedNow: Long): Boolean = transactionally { transaction ->
        val session = transaction.getRows(
            """SELECT state, lease_expires_at_millis FROM write_sessions
                WHERE session_uuid = ? FOR UPDATE""".trimIndent(),
            sessionUuid,
        ).firstOrNull() ?: return@transactionally false
        val state = session.stringValue("state")
        val leaseExpiresAt = session.longValue("lease_expires_at_millis")
        if (state != "ABORTED" && (state != "OPEN" || leaseExpiresAt > observedNow)) {
            return@transactionally false
        }
        deleteGenerationBlocks(transaction, sessionUuid)
        transaction.execute(
            "UPDATE write_sessions SET state = 'REAPED' WHERE session_uuid = ? AND state IN ('OPEN', 'ABORTED')",
            sessionUuid,
        )
        true
    }

    private fun reapReaderIfEligible(readerUuid: UUID, observedNow: Long): Boolean = transactionally { transaction ->
        val reader = transaction.getRows(
            """SELECT generation_uuid, state, lease_expires_at_millis FROM reader_sessions
                WHERE reader_uuid = ? FOR UPDATE""".trimIndent(),
            readerUuid,
        ).firstOrNull() ?: return@transactionally false
        if (reader.stringValue("state") != "OPEN" || reader.longValue("lease_expires_at_millis") > observedNow) {
            return@transactionally false
        }
        releaseGeneration(transaction, reader.uuidValue("generation_uuid"))
        transaction.execute(
            "UPDATE reader_sessions SET state = 'REAPED' WHERE reader_uuid = ? AND state = 'OPEN'",
            readerUuid,
        )
        true
    }

    private fun purgeFilesystemIfExpired(uuid: UUID, observedNow: Long): Boolean = transactionally { transaction ->
        lockWriteSessionsForFilesystem(transaction, uuid)
        val filesystem = transaction.getRows(
            "SELECT * FROM filesystems WHERE uuid = ? FOR UPDATE",
            uuid,
        ).firstOrNull()?.toFilesystemRecord() ?: return@transactionally false
        val expiration = filesystem.expiresAtMillis
        if (expiration == null || expiration > observedNow) return@transactionally false
        purgeFilesystem(transaction, filesystem, PathWatchTerminalReason.FILESYSTEM_PURGED)
        true
    }

    private fun purgeFilesystem(
        database: Database,
        filesystem: FilesystemRecord,
        terminalReason: PathWatchTerminalReason,
    ) {
        appendNamespaceEvents(database, filesystem, emptyList(), terminalReason)
        var lastPath: String? = null
        while (true) {
            val rows = if (lastPath == null) {
                database.getRows(
                    """SELECT path, generation_uuid FROM entries
                        WHERE filesystem_uuid = ? AND entry_kind = 'FILE'
                        ORDER BY path LIMIT ? FOR UPDATE""".trimIndent(),
                    filesystem.uuid,
                    INTERNAL_KEYSET_BATCH_SIZE,
                )
            } else {
                database.getRows(
                    """SELECT path, generation_uuid FROM entries
                        WHERE filesystem_uuid = ? AND entry_kind = 'FILE' AND path > ?
                        ORDER BY path LIMIT ? FOR UPDATE""".trimIndent(),
                    filesystem.uuid,
                    lastPath,
                    INTERNAL_KEYSET_BATCH_SIZE,
                )
            }
            if (rows.isEmpty()) break
            rows.forEach { row -> row.nullableUuidValue("generation_uuid")?.let { releaseGeneration(database, it) } }
            lastPath = rows.last().stringValue("path")
        }
        forEachWriteSession(database, filesystem.uuid, openOrAbortedOnly = true) { generation ->
            deleteGenerationBlocks(database, generation)
        }
        database.execute("DELETE FROM filesystems WHERE uuid = ?", filesystem.uuid)
        bumpManagerRevision(database)
    }

    private fun lockWriteSessionsForFilesystem(database: Database, filesystemUuid: UUID) {
        forEachWriteSession(database, filesystemUuid, openOrAbortedOnly = false) { }
    }

    private fun forEachWriteSession(
        database: Database,
        filesystemUuid: UUID,
        openOrAbortedOnly: Boolean,
        action: (UUID) -> Unit,
    ) {
        var last: UUID? = null
        while (true) {
            val statePredicate = if (openOrAbortedOnly) " AND state IN ('OPEN', 'ABORTED')" else ""
            val cursorPredicate = if (last == null) "" else " AND session_uuid > ?::UUID"
            val arguments = mutableListOf<Any>(filesystemUuid)
            if (last != null) arguments += last
            arguments += INTERNAL_KEYSET_BATCH_SIZE
            val rows = database.getRows(
                "SELECT session_uuid FROM write_sessions WHERE filesystem_uuid = ?" + statePredicate +
                    cursorPredicate + " ORDER BY session_uuid LIMIT ? FOR UPDATE",
                *arguments.toTypedArray(),
            )
            if (rows.isEmpty()) break
            rows.forEach { action(it.uuidValue("session_uuid")) }
            last = rows.last().uuidValue("session_uuid")
        }
    }

    private fun forEachGenerationBlock(database: Database, generationUuid: UUID, action: (BlockRecord) -> Unit) {
        var lastOrdinal = -1
        while (true) {
            val rows = database.getRows(
                """SELECT * FROM file_blocks WHERE generation_uuid = ? AND ordinal > ?
                    ORDER BY ordinal LIMIT ?""".trimIndent(),
                generationUuid,
                lastOrdinal,
                GENERATION_READ_BATCH_SIZE,
            )
            if (rows.isEmpty()) break
            rows.map { it.toBlockRecord() }.forEach(action)
            lastOrdinal = rows.last().intValue("ordinal")
            if (rows.size < GENERATION_READ_BATCH_SIZE) break
        }
    }

    private fun deleteGenerationBlocks(database: Database, generationUuid: UUID) {
        while (true) {
            val deleted = database.execute(
                """WITH block_page AS MATERIALIZED (
                        SELECT ordinal, blob_hash FROM file_blocks
                        WHERE generation_uuid = ?
                        ORDER BY ordinal
                        LIMIT ?
                    ),
                    enqueued_hashes AS (
                        INSERT INTO blob_gc_outbox (blob_hash, action, created_at_millis)
                        SELECT DISTINCT blob_hash, 'UNPIN_IF_UNREFERENCED', ?
                        FROM block_page
                        ON CONFLICT (blob_hash) DO UPDATE
                        SET action = excluded.action,
                            created_at_millis = excluded.created_at_millis
                        RETURNING blob_hash
                    )
                    DELETE FROM file_blocks
                    WHERE generation_uuid = ?
                      AND ordinal IN (SELECT ordinal FROM block_page)
                      AND (SELECT count(*) FROM enqueued_hashes) >= 0""".trimIndent(),
                generationUuid,
                GENERATION_DELETE_BATCH_SIZE,
                clock.currentTimeMillis(),
                generationUuid,
            )
            val deletedCount = affectedRowCount(deleted, "delete generation '$generationUuid' blocks")
            if (deletedCount < GENERATION_DELETE_BATCH_SIZE) break
        }
    }

    private fun requireRenewableSession(database: Database, sessionUuid: UUID) {
        val session = database.getRows(
            """SELECT state, lease_expires_at_millis FROM write_sessions
                WHERE session_uuid = ? FOR UPDATE""".trimIndent(),
            sessionUuid,
        ).firstOrNull() ?: throw IllegalStateException(
            "Write session '$sessionUuid' no longer exists and cannot renew its lease.",
        )
        val state = session.stringValue("state")
        if (state != "OPEN") {
            throw IllegalStateException(
                "Write session '$sessionUuid' cannot renew its lease because its state is '$state', not 'OPEN'.",
            )
        }
        val now = clock.currentTimeMillis()
        val leaseExpiresAt = session.longValue("lease_expires_at_millis")
        if (leaseExpiresAt <= now) {
            throw IllegalStateException(
                "Write session '$sessionUuid' lease expired at epoch millisecond $leaseExpiresAt; " +
                    "the attempted chunk was observed at epoch millisecond $now.",
            )
        }
    }

    private fun renewSessionLeaseRow(
        database: Database,
        sessionUuid: UUID,
        bytesReceived: Long?,
        observedNow: Long = clock.currentTimeMillis(),
    ) {
        val updated = if (bytesReceived == null) {
            database.getRows(
                """UPDATE write_sessions SET lease_expires_at_millis = ?
                    WHERE session_uuid = ? AND state = 'OPEN' AND lease_expires_at_millis > ?
                    RETURNING session_uuid""".trimIndent(),
                leaseDeadline(observedNow),
                sessionUuid,
                observedNow,
            )
        } else {
            database.getRows(
                """UPDATE write_sessions SET bytes_received = ?, lease_expires_at_millis = ?
                    WHERE session_uuid = ? AND state = 'OPEN' AND lease_expires_at_millis > ?
                    RETURNING session_uuid""".trimIndent(),
                bytesReceived,
                leaseDeadline(observedNow),
                sessionUuid,
                observedNow,
            )
        }
        if (updated.isEmpty()) requireRenewableSession(database, sessionUuid)
    }

    private fun leaseRenewalThreshold(now: Long): Long {
        leaseDeadline(now)
        return Math.addExact(now, sessionLeaseMillis / 2L)
    }

    private fun leaseDeadline(now: Long): Long = try {
        Math.addExact(now, sessionLeaseMillis)
    } catch (_: ArithmeticException) {
        throw SimpleFileSystemException(
            "Write-session lease deadline overflowed: current epoch millisecond $now plus " +
                "$sessionLeaseMillis lease milliseconds is outside the signed 64-bit range.",
        )
    }

    private fun validateMaintenanceLimit(limit: Int) {
        require(limit > 0) { "Maintenance batch limit must be positive, but was $limit." }
    }

    /** Clock.schedule accepts an absolute epoch-millisecond deadline, not a relative delay. */
    private fun scheduleNextMaintenance() {
        if (closed) return
        val interval = requireNotNull(maintenanceIntervalMillis)
        val deadline = try {
            Math.addExact(clock.currentTimeMillis(), interval)
        } catch (_: ArithmeticException) {
            throw SimpleFileSystemException(
                "Background maintenance deadline overflowed while adding $interval milliseconds to " +
                    "epoch millisecond ${clock.currentTimeMillis()}.",
            )
        }
        scheduledMaintenance = clock.schedule(deadline) {
            try {
                runMaintenance(maintenanceBatchSize)
            } finally {
                scheduleNextMaintenance()
            }
        }
    }

    internal fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(bytes)
        .joinToString("") { "%02X".format(it.toInt() and 0xFF) }

    internal fun recordNamespaceMutation(
        database: Database,
        filesystem: FilesystemRecord,
        newUsedBytes: Long = filesystem.usedBytes,
        affectedPaths: Collection<String>,
    ) {
        appendNamespaceEvents(database, filesystem, affectedPaths, terminalReason = null)
        if (newUsedBytes != filesystem.usedBytes) bumpManagerRevision(database)
    }

    private fun appendNamespaceEvents(
        database: Database,
        filesystem: FilesystemRecord,
        affectedPaths: Collection<String>,
        terminalReason: PathWatchTerminalReason?,
    ): Long {
        val next = try {
            Math.addExact(filesystem.namespaceRevision, 1L)
        } catch (_: ArithmeticException) {
            throw SimpleFileSystemException(
                "Namespace revision overflowed for filesystem '${filesystem.uuid}'.",
            )
        }
        database.execute(
            "UPDATE filesystems SET namespace_revision = ? WHERE uuid = ?",
            next,
            filesystem.uuid,
        )
        val now = clock.currentTimeMillis()
        if (terminalReason == null) {
            require(affectedPaths.isNotEmpty()) {
                "A non-terminal namespace mutation for filesystem '${filesystem.uuid}' must identify at least one affected path."
            }
            affectedPaths.toSortedSet().chunked(NAMESPACE_EVENT_BATCH_SIZE).forEach { paths ->
                val values = paths.joinToString(", ") { "(?)" }
                val arguments = mutableListOf<Any>(filesystem.uuid, next, now)
                arguments.addAll(paths)
                arguments.add(filesystem.uuid)
                database.execute(
                    """INSERT INTO namespace_events
                        (filesystem_uuid, revision, canonical_path, entry_type, exists, is_directory,
                         is_regular_file, size_bytes, content_hash, terminal, terminal_reason, event_at_millis)
                        SELECT ?, ?, requested.path,
                               CASE entry.entry_kind
                                   WHEN 'FILE' THEN 'REGULAR_FILE'
                                   WHEN 'DIRECTORY' THEN 'DIRECTORY'
                                   ELSE NULL
                               END,
                               entry.path IS NOT NULL,
                               COALESCE(entry.entry_kind = 'DIRECTORY', false),
                               COALESCE(entry.entry_kind = 'FILE', false),
                               entry.size_bytes,
                               entry.content_hash,
                               false,
                               NULL,
                               ?
                        FROM (VALUES $values) AS requested(path)
                        LEFT JOIN entries AS entry
                          ON entry.filesystem_uuid = ? AND entry.path = requested.path""".trimIndent(),
                    *arguments.toTypedArray(),
                )
            }
        } else {
            database.execute(
                """INSERT INTO namespace_events
                    (filesystem_uuid, revision, canonical_path, entry_type, exists, is_directory,
                     is_regular_file, size_bytes, content_hash, terminal, terminal_reason, event_at_millis)
                    VALUES (?, ?, '/', NULL, false, false, false, NULL, NULL, true, ?, ?)""".trimIndent(),
                filesystem.uuid,
                next,
                terminalReason.name,
                now,
            )
        }
        database.execute(
            """UPDATE namespace_event_streams SET latest_revision = ?, terminal_reason = ?
                WHERE filesystem_uuid = ?""".trimIndent(),
            next,
            terminalReason?.name,
            filesystem.uuid,
        )
        pruneNamespaceEvents(database, filesystem.uuid)
        return next
    }

    private fun namespaceSnapshot(
        database: Database,
        filesystemUuid: UUID,
        canonicalPath: String,
        revision: Long,
    ): NamespaceEventRecord {
        val entry = findEntry(database, filesystemUuid, canonicalPath)
        return NamespaceEventRecord(
            filesystemUuid = filesystemUuid,
            revision = revision,
            canonicalPath = canonicalPath,
            entryType = entry?.entryType,
            exists = entry != null,
            isDirectory = entry?.isDirectory == true,
            isRegularFile = entry?.isFile == true,
            sizeBytes = entry?.sizeBytes,
            contentHash = entry?.contentHash,
            terminalReason = null,
        )
    }

    private fun requireNamespaceEventStream(
        database: Database,
        filesystemUuid: UUID,
        lock: Boolean,
    ): NamespaceEventStreamRecord {
        val suffix = if (lock) " FOR UPDATE" else ""
        return database.getRows(
            "SELECT * FROM namespace_event_streams WHERE filesystem_uuid = ?$suffix",
            filesystemUuid,
        ).firstOrNull()?.toNamespaceEventStreamRecord()
            ?: throw FilesystemNotFoundException(filesystemUuid.toString())
    }

    private fun pruneNamespaceEvents(database: Database, filesystemUuid: UUID) {
        val now = clock.currentTimeMillis()
        val ageCutoff = try {
            Math.subtractExact(now, namespaceEventRetentionMillis)
        } catch (_: ArithmeticException) {
            Long.MIN_VALUE
        }
        val retention = database.getRows(
            """WITH count_cutoff AS MATERIALIZED (
                    SELECT revision FROM namespace_events
                    WHERE filesystem_uuid = ? AND event_at_millis >= ?
                    ORDER BY revision DESC, canonical_path DESC LIMIT 1 OFFSET ?
                ),
                minimum_retained AS MATERIALIZED (
                    SELECT min(revision) AS minimum_revision FROM namespace_events
                    WHERE filesystem_uuid = ? AND event_at_millis >= ?
                      AND (
                          NOT EXISTS (SELECT 1 FROM count_cutoff)
                          OR revision >= (SELECT revision FROM count_cutoff)
                      )
                ),
                deleted_events AS (
                    DELETE FROM namespace_events
                    WHERE filesystem_uuid = ?
                      AND (
                          event_at_millis < ?
                          OR (
                              EXISTS (SELECT 1 FROM count_cutoff)
                              AND revision < (SELECT revision FROM count_cutoff)
                          )
                      )
                    RETURNING revision
                )
                SELECT stream.latest_revision,
                       stream.oldest_available_since_revision,
                       (SELECT minimum_revision FROM minimum_retained) AS minimum_revision,
                       (SELECT count(*) FROM deleted_events) AS deleted_count
                FROM namespace_event_streams AS stream
                WHERE stream.filesystem_uuid = ?
                FOR UPDATE""".trimIndent(),
            filesystemUuid,
            ageCutoff,
            namespaceEventRetentionCount - 1,
            filesystemUuid,
            ageCutoff,
            filesystemUuid,
            ageCutoff,
            filesystemUuid,
        ).single()
        val minimumRetainedRevision = retention.nullableLongValue("minimum_revision")
        val latestRevision = retention.longValue("latest_revision")
        val oldestAvailableSinceRevision = retention.longValue("oldest_available_since_revision")
        val safeSinceRevision = minimumRetainedRevision?.let { it - 1L } ?: latestRevision
        if (safeSinceRevision > oldestAvailableSinceRevision) {
            database.execute(
                """UPDATE namespace_event_streams SET oldest_available_since_revision = ?
                    WHERE filesystem_uuid = ?""".trimIndent(),
                safeSinceRevision,
                filesystemUuid,
            )
        }
    }

    private fun managerRevision(database: Database, lock: Boolean = false): Long {
        val suffix = if (lock) " FOR UPDATE" else ""
        return database.getRows(
            "SELECT descriptor_revision FROM simple_filesystem_manager_state WHERE singleton = true$suffix",
        ).single().longValue("descriptor_revision")
    }

    private fun bumpManagerRevision(database: Database) {
        val updated = database.getRows(
            """UPDATE simple_filesystem_manager_state
                SET descriptor_revision = descriptor_revision + 1
                WHERE singleton = true AND descriptor_revision < ?
                RETURNING descriptor_revision""".trimIndent(),
            Long.MAX_VALUE,
        )
        if (updated.isEmpty()) {
            throw SimpleFileSystemException("SimpleFileSystem manager descriptor revision overflowed.")
        }
    }

    private fun validateDescription(description: String) {
        firstMalformedUnicodeIndex(description)?.let {
            throw InvalidFilesystemDescriptionException(
                description,
                null,
                "Filesystem description is not valid Unicode text: " +
                    "unpaired UTF-16 surrogate at character index $it.",
            )
        }
        val byteCount = strictUtf8Size(description).toLong()
        if (byteCount > FILESYSTEM_DESCRIPTION_MAX_UTF8_BYTES) {
            throw InvalidFilesystemDescriptionException(
                description,
                byteCount,
                "Filesystem description is $byteCount UTF-8 bytes, exceeding the maximum of " +
                    "$FILESYSTEM_DESCRIPTION_MAX_UTF8_BYTES UTF-8 bytes.",
            )
        }
    }

    private fun parseFilesystemCursor(after: String?): Pair<Long, UUID>? {
        if (after == null) return null
        val expected = "expected '<createdAtMillis>:<uuid>' where createdAtMillis is a non-negative decimal " +
            "and uuid is canonical lowercase RFC 4122."
        val separator = after.indexOf(':')
        if (separator <= 0 || separator != after.lastIndexOf(':')) {
            throw InvalidCursorException(
                after,
                "$expected The cursor must contain exactly one colon separating its two non-empty components.",
            )
        }
        val timestampText = after.substring(0, separator)
        if (!UNSIGNED_DECIMAL.matches(timestampText) ||
            (timestampText.length > 1 && timestampText.startsWith('0'))
        ) {
            throw InvalidCursorException(
                after,
                "$expected The createdAtMillis component must be canonical unsigned decimal text without leading zeroes.",
            )
        }
        val timestamp = timestampText.toLongOrNull()
            ?: throw InvalidCursorException(
                after,
                "$expected The createdAtMillis component exceeds the supported signed 64-bit epoch-millisecond range.",
            )
        val uuidText = after.substring(separator + 1)
        val uuid = try {
            parseUuid(uuidText)
        } catch (_: InvalidFilesystemUuidException) {
            throw InvalidCursorException(
                after,
                "$expected The UUID component is not lowercase canonical RFC 4122 text.",
            )
        }
        return timestamp to uuid
    }

    private companion object {
        val EXPECTED_HASH = Regex("[0-9A-F]{64}")
        val UNSIGNED_DECIMAL = Regex("[0-9]+")
        const val SERIALIZATION_FAILURE_SQL_STATE = "40001"
        const val MAX_TRANSACTION_RETRIES = 32
        const val DEFAULT_SESSION_LEASE_MILLIS = 5L * 60L * 1000L
        const val DEFAULT_MAINTENANCE_BATCH_SIZE = 1_000
        const val INTERNAL_KEYSET_BATCH_SIZE = 256
        const val GENERATION_READ_BATCH_SIZE = 4_096
        const val GENERATION_DELETE_BATCH_SIZE = 4_096
        const val NAMESPACE_EVENT_BATCH_SIZE = 2_048
        const val DEFAULT_NAMESPACE_EVENT_RETENTION_COUNT = 10_000
        const val DEFAULT_NAMESPACE_EVENT_RETENTION_MILLIS = 7L * 24L * 60L * 60L * 1000L
        const val WRITE_MAINTENANCE_CANDIDATE = "WRITE_SESSION"
        const val READER_MAINTENANCE_CANDIDATE = "READER_SESSION"
        const val FILESYSTEM_MAINTENANCE_CANDIDATE = "FILESYSTEM"
        const val ORPHAN_HIGH_WATER_CANDIDATE = "ORPHAN_HIGH_WATER"
        const val BLOB_GC_MAINTENANCE_CANDIDATE = "BLOB_GC"
    }
}

private data class ReconciliationSnapshot(
    val maintenanceCandidates: List<Pair<String, UUID>>,
    val orphanInventoryHighWater: String?,
    val blobGcCandidates: List<String>,
)

private const val CURRENT_SCHEMA_VERSION = 1

private val schemaVersionCache = WeakHashMap<Database, Int>()

private fun cachedSchemaVersion(database: Database): Int? = synchronized(schemaVersionCache) {
    schemaVersionCache[database]
}

private fun cacheSchemaVersion(database: Database, version: Int) = synchronized(schemaVersionCache) {
    schemaVersionCache[database] = version
}

/*
 * This is intentionally one PostgreSQL request. On a shared two-core CockroachDB node, issuing
 * every idempotent DDL statement as its own auto-committed request made concurrent test databases
 * spend 1-4.5 seconds on each index and repeatedly replay the whole sequence on manager restart.
 * PostgreSQL executes this multi-statement request as one implicit transaction, so the version
 * marker becomes visible only after the complete schema does.
 */
private val SCHEMA_INITIALIZATION_SQL = """
    CREATE TABLE IF NOT EXISTS filesystems (
        uuid UUID PRIMARY KEY,
        description STRING NOT NULL,
        owner STRING NULL,
        max_size_bytes INT8 NOT NULL CHECK (max_size_bytes > 0),
        used_bytes INT8 NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
        expires_at_millis INT8 NULL,
        created_at_millis INT8 NOT NULL,
        namespace_revision INT8 NOT NULL DEFAULT 0 CHECK (namespace_revision >= 0)
    );
    CREATE TABLE IF NOT EXISTS simple_filesystem_manager_state (
        singleton BOOL PRIMARY KEY DEFAULT true CHECK (singleton),
        descriptor_revision INT8 NOT NULL CHECK (descriptor_revision >= 0)
    );
    INSERT INTO simple_filesystem_manager_state (singleton, descriptor_revision)
        VALUES (true, 0) ON CONFLICT (singleton) DO NOTHING;
    CREATE TABLE IF NOT EXISTS namespace_event_streams (
        filesystem_uuid UUID PRIMARY KEY,
        latest_revision INT8 NOT NULL CHECK (latest_revision >= 0),
        oldest_available_since_revision INT8 NOT NULL
            CHECK (oldest_available_since_revision >= 0),
        terminal_reason STRING NULL CHECK (terminal_reason IS NULL OR terminal_reason IN
            ('FILESYSTEM_EXPIRED', 'FILESYSTEM_DELETED', 'FILESYSTEM_PURGED')),
        CHECK (oldest_available_since_revision <= latest_revision)
    );
    INSERT INTO namespace_event_streams
        (filesystem_uuid, latest_revision, oldest_available_since_revision, terminal_reason)
        SELECT uuid, namespace_revision, namespace_revision, NULL FROM filesystems
        ON CONFLICT (filesystem_uuid) DO NOTHING;
    CREATE TABLE IF NOT EXISTS namespace_events (
        filesystem_uuid UUID NOT NULL,
        revision INT8 NOT NULL CHECK (revision > 0),
        canonical_path STRING NOT NULL,
        entry_type STRING NULL CHECK (entry_type IS NULL OR entry_type IN
            ('REGULAR_FILE', 'DIRECTORY', 'SYMLINK', 'OTHER')),
        exists BOOL NOT NULL,
        is_directory BOOL NOT NULL,
        is_regular_file BOOL NOT NULL,
        size_bytes INT8 NULL CHECK (size_bytes IS NULL OR size_bytes >= 0),
        content_hash STRING NULL,
        terminal BOOL NOT NULL,
        terminal_reason STRING NULL CHECK (terminal_reason IS NULL OR terminal_reason IN
            ('FILESYSTEM_EXPIRED', 'FILESYSTEM_DELETED', 'FILESYSTEM_PURGED')),
        event_at_millis INT8 NOT NULL,
        PRIMARY KEY (filesystem_uuid, revision, canonical_path),
        INDEX namespace_events_by_path (filesystem_uuid, canonical_path, revision),
        INDEX namespace_events_by_age (filesystem_uuid, event_at_millis, revision),
        CHECK ((terminal AND terminal_reason IS NOT NULL AND canonical_path = '/' AND
                NOT exists AND entry_type IS NULL AND NOT is_directory AND
                NOT is_regular_file AND size_bytes IS NULL AND content_hash IS NULL)
            OR (NOT terminal AND terminal_reason IS NULL AND
                exists = (entry_type IS NOT NULL) AND
                is_directory = (entry_type = 'DIRECTORY') AND
                is_regular_file = (entry_type = 'REGULAR_FILE') AND
                (is_regular_file OR (size_bytes IS NULL AND content_hash IS NULL))))
    );
    CREATE TABLE IF NOT EXISTS entries (
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
        PRIMARY KEY (filesystem_uuid, path),
        INDEX entries_by_parent (filesystem_uuid, parent_path, name)
    );
    CREATE TABLE IF NOT EXISTS file_blocks (
        generation_uuid UUID NOT NULL,
        ordinal INT4 NOT NULL,
        blob_hash STRING NOT NULL,
        size_bytes INT4 NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= $BLOCK_SIZE_BYTES),
        reference_count INT8 NOT NULL DEFAULT 0 CHECK (reference_count >= 0),
        PRIMARY KEY (generation_uuid, ordinal),
        INDEX file_blocks_by_blob_hash (blob_hash, reference_count)
    );
    CREATE TABLE IF NOT EXISTS write_sessions (
        session_uuid UUID PRIMARY KEY,
        filesystem_uuid UUID NOT NULL REFERENCES filesystems(uuid) ON DELETE CASCADE,
        path STRING NOT NULL,
        expected_hash STRING NULL,
        bytes_received INT8 NOT NULL,
        created_at_millis INT8 NOT NULL,
        lease_expires_at_millis INT8 NOT NULL,
        state STRING NOT NULL CHECK (state IN ('OPEN', 'COMMITTED', 'ABORTED', 'REAPED')),
        INDEX write_sessions_by_lease (state, lease_expires_at_millis)
    );
    CREATE TABLE IF NOT EXISTS reader_sessions (
        reader_uuid UUID PRIMARY KEY,
        generation_uuid UUID NOT NULL,
        lease_expires_at_millis INT8 NOT NULL,
        state STRING NOT NULL CHECK (state IN ('OPEN', 'RELEASED', 'REAPED')),
        INDEX reader_sessions_by_lease (state, lease_expires_at_millis)
    );
    CREATE TABLE IF NOT EXISTS blob_gc_outbox (
        blob_hash STRING PRIMARY KEY,
        action STRING NOT NULL,
        created_at_millis INT8 NOT NULL
    );
    CREATE TABLE IF NOT EXISTS simple_filesystem_maintenance_state (
        singleton BOOL PRIMARY KEY DEFAULT true CHECK (singleton),
        orphan_inventory_high_water STRING NULL
    );
    INSERT INTO simple_filesystem_maintenance_state (singleton, orphan_inventory_high_water)
        VALUES (true, NULL) ON CONFLICT (singleton) DO NOTHING;
    CREATE TABLE IF NOT EXISTS simple_filesystem_schema_version (
        singleton BOOL PRIMARY KEY DEFAULT true CHECK (singleton),
        schema_version INT4 NOT NULL CHECK (schema_version > 0)
    );
    INSERT INTO simple_filesystem_schema_version (singleton, schema_version)
        VALUES (true, $CURRENT_SCHEMA_VERSION)
""".trimIndent()

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

private fun strictUtf8Size(text: String): Int = text.toByteArray(StandardCharsets.UTF_8).size
