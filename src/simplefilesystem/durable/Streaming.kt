package simplefilesystem.durable

import okio.Buffer
import okio.Source
import okio.Timeout
import simplefilesystem.FileMetadataInfo
import simplefilesystem.FileSink
import simplefilesystem.QuotaArithmeticOverflowException
import sql.Database
import java.io.InputStream
import java.security.MessageDigest
import java.util.UUID

internal class StagedBlockSink(
    private val manager: DurableSimpleFileSystemManager,
    private val filesystemUuid: UUID,
    rawPath: String,
    private val expectedHash: String?,
    private val unconditional: Boolean,
    private val append: Boolean,
    private val appendMustExist: Boolean = false,
) : FileSink {
    private val sessionAndPath = manager.beginSession(filesystemUuid, rawPath, expectedHash)
    private val sessionUuid: UUID = sessionAndPath.first
    private val path: String = sessionAndPath.second
    private val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    private val pending = ByteArray(BLOCK_SIZE_BYTES)
    private var pendingSize: Int = 0
    private var ordinal: Int = 0
    private var totalBytes: Long = 0L
    private var state: SinkState = SinkState.OPEN
    private var committedMetadata: FileMetadataInfo? = null
    private var commitFailure: Throwable? = null

    override fun write(source: Buffer, byteCount: Long) {
        ensureWritable()
        try {
            require(byteCount >= 0L && byteCount <= source.size) {
                "Cannot consume $byteCount bytes from an Okio buffer containing ${source.size} bytes."
            }
            manager.renewSessionLease(sessionUuid)
            var remaining = byteCount
            while (remaining > 0L) {
                val requested = minOf(remaining, (pending.size - pendingSize).toLong()).toInt()
                val read = source.read(pending, pendingSize, requested)
                check(read > 0) {
                    "Okio buffer for '$path' ended while $remaining staged bytes were still required."
                }
                digest.update(pending, pendingSize, read)
                pendingSize += read
                totalBytes = try {
                    Math.addExact(totalBytes, read.toLong())
                } catch (_: ArithmeticException) {
                    throw QuotaArithmeticOverflowException(path, totalBytes, 0L, read.toLong())
                }
                remaining -= read.toLong()
                if (pendingSize == pending.size) flushBlock()
            }
        } catch (failure: Throwable) {
            poison(failure)
            throw failure
        }
    }

    override fun flush() {
        ensureWritable()
    }

    override fun timeout(): Timeout = Timeout.NONE

    override fun commit(): FileMetadataInfo {
        committedMetadata?.let { return it }
        commitFailure?.let { throw it }
        checkCommitAllowed()
        return try {
            manager.requireActiveFilesystem(filesystemUuid)
            if (pendingSize > 0) flushBlock()
            val contentHash = digest.digest().toUpperHex()
            val stage = StagedGeneration(
                sessionUuid = sessionUuid,
                filesystemUuid = filesystemUuid,
                path = path,
                expectedHash = expectedHash,
                sizeBytes = totalBytes,
                contentHash = contentHash,
            )
            val metadata = if (append) {
                manager.commitAppend(stage, appendMustExist)
            } else {
                manager.commitGeneration(stage, unconditional)
            }
            committedMetadata = metadata
            state = SinkState.COMMITTED
            metadata
        } catch (failure: Throwable) {
            commitFailure = failure
            state = SinkState.FAILED
            abortAfterFailure(failure)
            throw failure
        }
    }

    override fun abort() {
        if (state != SinkState.OPEN) return
        state = SinkState.ABORTED
        manager.abortSession(sessionUuid)
    }

    override fun close() {
        if (state != SinkState.OPEN) return
        state = SinkState.CLOSED
        manager.abortSession(sessionUuid)
    }

    private fun flushBlock() {
        if (pendingSize == 0) return
        val bytes = pending.copyOf(pendingSize)
        manager.stageSessionBlock(sessionUuid, ordinal, bytes, totalBytes)
        ordinal += 1
        pendingSize = 0
    }

    private fun ensureWritable() {
        val reason = when (state) {
            SinkState.ABORTED, SinkState.CLOSED -> "the sink was aborted."
            SinkState.COMMITTED -> "the sink was already committed."
            SinkState.FAILED -> "the sink failed earlier and is poisoned."
            SinkState.OPEN -> return
        }
        throw IllegalStateException("FileSink for path '$path' cannot accept data: $reason")
    }

    private fun checkCommitAllowed() {
        val reason = when (state) {
            SinkState.ABORTED -> "the sink was aborted."
            SinkState.CLOSED -> "the sink was closed without commit, which aborted it."
            SinkState.OPEN, SinkState.COMMITTED, SinkState.FAILED -> return
        }
        throw IllegalStateException("FileSink for path '$path' cannot commit: $reason")
    }

    private fun poison(failure: Throwable) {
        if (commitFailure == null) commitFailure = failure
        state = SinkState.FAILED
        abortAfterFailure(failure)
    }

    private fun abortAfterFailure(failure: Throwable) {
        try {
            manager.abortSession(sessionUuid)
        } catch (cleanupFailure: Throwable) {
            failure.addSuppressed(cleanupFailure)
        }
    }

    private enum class SinkState { OPEN, COMMITTED, FAILED, ABORTED, CLOSED }
}

internal class TransactionalBlockAssembler(
    private val manager: DurableSimpleFileSystemManager,
    private val transaction: Database,
    private val generationUuid: UUID,
    private val onStagedHash: (String) -> Unit = {},
) {
    private val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    private val pending = ByteArray(BLOCK_SIZE_BYTES)
    private var pendingSize: Int = 0
    private var ordinal: Int = 0
    private var totalBytes: Long = 0L

    val hasPendingBytes: Boolean get() = pendingSize > 0

    fun reuseCompleteBlock(block: BlockRecord) {
        check(!hasPendingBytes) {
            "Cannot reuse complete block ${block.ordinal} after a partial block has started a new extent."
        }
        check(block.sizeBytes == BLOCK_SIZE_BYTES) {
            "Cannot reuse block ${block.ordinal} as a complete extent because it contains ${block.sizeBytes} bytes, " +
                "not $BLOCK_SIZE_BYTES bytes."
        }
        var observedBytes = 0L
        manager.blobstoreService.getBlob(BLOB_PIN_OWNER, block.blobHash).use { input ->
            val transfer = ByteArray(8192)
            while (true) {
                val count = input.read(transfer)
                if (count == -1) break
                if (count == 0) continue
                digest.update(transfer, 0, count)
                observedBytes += count.toLong()
            }
        }
        check(observedBytes == block.sizeBytes.toLong()) {
            "Blob '${block.blobHash}' for complete block ${block.ordinal} contains $observedBytes bytes, but metadata " +
                "declares ${block.sizeBytes} bytes."
        }
        manager.prepareBlobReference(transaction, block.blobHash)
        transaction.execute(
            """INSERT INTO file_blocks
                (generation_uuid, ordinal, blob_hash, size_bytes, reference_count)
                VALUES (?, ?, ?, ?, 0)""".trimIndent(),
            generationUuid,
            ordinal,
            block.blobHash,
            block.sizeBytes,
        )
        transaction.execute("DELETE FROM blob_gc_outbox WHERE blob_hash = ?", block.blobHash)
        ordinal += 1
        totalBytes += observedBytes
    }

    fun writeFrom(input: InputStream) {
        val transfer = ByteArray(8192)
        while (true) {
            val count = input.read(transfer)
            if (count == -1) break
            if (count == 0) continue
            write(transfer, 0, count)
        }
    }

    fun finish(): Pair<Long, String> {
        flushBlock()
        return totalBytes to digest.digest().toUpperHex()
    }

    private fun write(bytes: ByteArray, offset: Int, length: Int) {
        var cursor = offset
        var remaining = length
        while (remaining > 0) {
            val count = minOf(remaining, pending.size - pendingSize)
            bytes.copyInto(pending, pendingSize, cursor, cursor + count)
            digest.update(bytes, cursor, count)
            cursor += count
            remaining -= count
            pendingSize += count
            totalBytes += count.toLong()
            if (pendingSize == pending.size) flushBlock()
        }
    }

    private fun flushBlock() {
        if (pendingSize == 0) return
        val staged = manager.stageBlock(transaction, generationUuid, ordinal, pending.copyOf(pendingSize))
        onStagedHash(staged.blobHash)
        ordinal += 1
        pendingSize = 0
    }
}

internal class GenerationSource(
    private val manager: DurableSimpleFileSystemManager,
    private val generationUuid: UUID,
    private val readerUuid: UUID,
    firstOrdinal: Int,
    private val lastOrdinal: Int,
    private val initialSkip: Long,
    byteCount: Long,
) : Source {
    private var remaining: Long = byteCount
    private var nextOrdinal: Int = firstOrdinal
    private var current: InputStream? = null
    private var closed: Boolean = false
    private var readerReleased: Boolean = false
    private var skipForNextBlock: Long = initialSkip

    override fun read(sink: Buffer, byteCount: Long): Long {
        check(!closed) { "Cannot read from a closed durable filesystem source." }
        require(byteCount >= 0L) { "Source read byteCount must be non-negative, but was $byteCount." }
        if (byteCount == 0L) return 0L
        if (remaining == 0L) {
            releaseReader()
            return -1L
        }
        while (true) {
            val stream = current ?: openNextBlock() ?: error(
                "Generation ended with $remaining bytes still required by the requested file range.",
            )
            val requested = minOf(byteCount, remaining, 8192L).toInt()
            val bytes = ByteArray(requested)
            val read = stream.read(bytes)
            if (read == -1) {
                stream.close()
                current = null
                continue
            }
            if (read == 0) continue
            sink.write(bytes, 0, read)
            remaining -= read.toLong()
            if (remaining == 0L) releaseReader()
            return read.toLong()
        }
    }

    override fun timeout(): Timeout = Timeout.NONE

    override fun close() {
        if (closed) return
        closed = true
        current?.close()
        current = null
        releaseReader()
    }

    private fun openNextBlock(): InputStream? {
        if (nextOrdinal > lastOrdinal) return null
        manager.renewReaderSession(readerUuid)
        val block = manager.block(generationUuid, nextOrdinal++) ?: return null
        val stream = manager.blobstoreService.getBlob(BLOB_PIN_OWNER, block.blobHash)
        var toSkip = skipForNextBlock
        skipForNextBlock = 0L
        while (toSkip > 0L) {
            val skipped = stream.skip(toSkip)
            if (skipped > 0L) {
                toSkip -= skipped
            } else if (stream.read() == -1) {
                stream.close()
                error(
                    "Blob '${block.blobHash}' ended before the requested offset inside block ${block.ordinal}.",
                )
            } else {
                toSkip -= 1L
            }
        }
        current = stream
        return stream
    }

    private fun releaseReader() {
        if (readerReleased) return
        manager.releaseReaderSession(readerUuid)
        readerReleased = true
    }
}

internal fun ByteArray.toUpperHex(): String = joinToString("") { byte ->
    "%02X".format(byte.toInt() and 0xFF)
}
