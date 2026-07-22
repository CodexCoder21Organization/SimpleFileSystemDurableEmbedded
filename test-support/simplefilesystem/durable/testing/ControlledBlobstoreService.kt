package simplefilesystem.durable.testing

import blobstore.api.BlobstoreService
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Real in-memory Blobstore with reusable deterministic observation/fault/latch controls for integration tests. */
class ControlledBlobstoreService(
    val delegate: InMemoryBlobstoreService = InMemoryBlobstoreService(),
) : BlobstoreService by delegate {
    val fetchedHashes = CopyOnWriteArrayList<String>()
    val pinCounts = ConcurrentHashMap<String, AtomicInteger>()
    val unpinCounts = ConcurrentHashMap<String, AtomicInteger>()

    @Volatile
    var beforePin: ((String) -> Unit)? = null
    @Volatile
    var beforeUnpin: ((String) -> Unit)? = null
    @Volatile
    var beforePut: ((String) -> Unit)? = null
    @Volatile
    var beforeGet: ((String) -> Unit)? = null
    @Volatile
    var requirePinnedOnGet: Boolean = false

    override fun pinBlob(publicKeyHash: String, sha256hex: String): Boolean {
        pinCounts.computeIfAbsent(sha256hex) { AtomicInteger() }.incrementAndGet()
        beforePin?.invoke(sha256hex)
        return delegate.pinBlob(publicKeyHash, sha256hex)
    }

    override fun unpinBlob(publicKeyHash: String, sha256hex: String) {
        unpinCounts.computeIfAbsent(sha256hex) { AtomicInteger() }.incrementAndGet()
        beforeUnpin?.invoke(sha256hex)
        delegate.unpinBlob(publicKeyHash, sha256hex)
    }

    override fun putBlob(publicKeyHash: String, sha256hex: String, size: Long, data: InputStream) {
        beforePut?.invoke(sha256hex)
        delegate.putBlob(publicKeyHash, sha256hex, size, data)
    }

    override fun getBlob(publicKeyHash: String, sha256hex: String): InputStream {
        fetchedHashes += sha256hex
        beforeGet?.invoke(sha256hex)
        if (requirePinnedOnGet && !delegate.isPinned(publicKeyHash, sha256hex)) {
            throw IllegalStateException("Blob '$sha256hex' was collected after its last pin was released.")
        }
        return delegate.getBlob(publicKeyHash, sha256hex)
    }
}
