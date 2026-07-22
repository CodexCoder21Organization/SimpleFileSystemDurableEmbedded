package simplefilesystem.durable

import okio.Buffer
import okio.Sink
import okio.Source
import okio.Timeout
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
) : Sink {
    private val sessionAndPath = manager.beginSession(filesystemUuid, rawPath, expectedHash)
    private val sessionUuid: UUID = sessionAndPath.first
    private val path: String = sessionAndPath.second
    private val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    private val pending = ByteArray(BLOCK_SIZE_BYTES)
    private var pendingSize: Int = 0
    private var ordinal: Int = 0
    private var totalBytes: Long = 0L
    private var closed: Boolean = false
    private var failed: Throwable? = null

    override fun write(source: Buffer, byteCount: Long) {
        check(!closed) { "Cannot write to the staged sink for '$path' after it has been closed." }
        require(byteCount >= 0L && byteCount <= source.size) {
            "Cannot consume $byteCount bytes from an Okio buffer containing ${source.size} bytes."
        }
        failed?.let { throw it }
        var remaining = byteCount
        try {
            while (remaining > 0L) {
                val requested = minOf(remaining, (pending.size - pendingSize).toLong()).toInt()
                val read = source.read(pending, pendingSize, requested)
                check(read > 0) {
                    "Okio buffer for '$path' ended while $remaining staged bytes were still required."
                }
                digest.update(pending, pendingSize, read)
                pendingSize += read
                totalBytes += read.toLong()
                remaining -= read.toLong()
                if (pendingSize == pending.size) flushBlock()
            }
        } catch (failure: Throwable) {
            failed = failure
            manager.abortSession(sessionUuid)
            throw failure
        }
    }

    override fun flush() {
        check(!closed) { "Cannot flush the staged sink for '$path' after it has been closed." }
    }

    override fun timeout(): Timeout = Timeout.NONE

    override fun close() {
        if (closed) return
        closed = true
        failed?.let { throw it }
        try {
            if (pendingSize > 0) flushBlock()
            manager.updateSessionBytes(sessionUuid, totalBytes)
            val contentHash = digest.digest().toUpperHex()
            val stage = StagedGeneration(
                sessionUuid = sessionUuid,
                filesystemUuid = filesystemUuid,
                path = path,
                expectedHash = expectedHash,
                sizeBytes = totalBytes,
                contentHash = contentHash,
            )
            if (append) manager.commitAppend(stage) else manager.commitGeneration(stage, unconditional)
        } catch (failure: Throwable) {
            manager.abortSession(sessionUuid)
            throw failure
        }
    }

    private fun flushBlock() {
        if (pendingSize == 0) return
        val bytes = pending.copyOf(pendingSize)
        manager.stageBlock(manager.metadataDatabase, sessionUuid, ordinal, bytes)
        ordinal += 1
        pendingSize = 0
        manager.updateSessionBytes(sessionUuid, totalBytes)
    }
}

internal class TransactionalBlockAssembler(
    private val manager: DurableSimpleFileSystemManager,
    private val transaction: Database,
    private val generationUuid: UUID,
) {
    private val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    private val pending = ByteArray(BLOCK_SIZE_BYTES)
    private var pendingSize: Int = 0
    private var ordinal: Int = 0
    private var totalBytes: Long = 0L

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
        manager.stageBlock(transaction, generationUuid, ordinal, pending.copyOf(pendingSize))
        ordinal += 1
        pendingSize = 0
    }
}

internal class GenerationSource(
    private val manager: DurableSimpleFileSystemManager,
    private val blocks: List<BlockRecord>,
    private val initialSkip: Long,
    byteCount: Long,
) : Source {
    private var remaining: Long = byteCount
    private var blockIndex: Int = 0
    private var current: InputStream? = null
    private var closed: Boolean = false
    private var skipForNextBlock: Long = initialSkip

    override fun read(sink: Buffer, byteCount: Long): Long {
        check(!closed) { "Cannot read from a closed durable filesystem source." }
        require(byteCount >= 0L) { "Source read byteCount must be non-negative, but was $byteCount." }
        if (byteCount == 0L) return 0L
        if (remaining == 0L) return -1L
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
            return read.toLong()
        }
    }

    override fun timeout(): Timeout = Timeout.NONE

    override fun close() {
        if (closed) return
        closed = true
        current?.close()
        current = null
    }

    private fun openNextBlock(): InputStream? {
        if (blockIndex >= blocks.size) return null
        val block = blocks[blockIndex++]
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
}

internal fun ByteArray.toUpperHex(): String = joinToString("") { byte ->
    "%02X".format(byte.toInt() and 0xFF)
}
