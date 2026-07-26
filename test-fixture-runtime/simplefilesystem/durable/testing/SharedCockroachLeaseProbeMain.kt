package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

fun main(args: Array<String>) {
    require(args.size == 2) {
        "SharedCockroachLeaseProbeMain requires <ready-file> <release-file>, but received ${args.size} argument(s)."
    }
    val readyFile = File(args[0])
    val releaseFile = File(args[1])
    SharedCockroachCluster().start().use { cluster ->
        Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password).use { database ->
            DurableSimpleFileSystemManager(
                blobstoreService = InMemoryBlobstoreService(),
                metadataDatabase = database,
            ).use { manager ->
                manager.listFilesystems(null, 1)
            }
        }
        writeAtomically(readyFile, cluster.jdbcUrl())
        waitForFileCreation(releaseFile)
    }
}

private fun waitForFileCreation(file: File) {
    file.parentFile?.let { parent ->
        check(parent.isDirectory || parent.mkdirs()) {
            "Could not create the release-file directory ${parent.absolutePath}."
        }
    }
    FileSystems.getDefault().newWatchService().use { watcher ->
        file.parentFile.toPath().register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
        while (!file.isFile) {
            val key = watcher.take()
            key.pollEvents()
            check(key.reset()) {
                "Could not continue watching ${file.parentFile.absolutePath} for ${file.name}."
            }
        }
    }
}
