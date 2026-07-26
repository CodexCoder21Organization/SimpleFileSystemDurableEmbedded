package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

fun main(args: Array<String>) {
    require(args.size == 2 || args.size == 5 || args.size == 6) {
        "SharedCockroachLeaseProbeMain requires <ready-file> <release-file> or <ready-file> " +
            "<release-file> <armed-file> <start-gate> <control-directory-or-dash> " +
            "[lease-only], but received ${args.size} argument(s)."
    }
    require(args.size != 6 || args[5] == "lease-only") {
        "SharedCockroachLeaseProbeMain optional mode must be 'lease-only', but was '${args[5]}'."
    }
    val initializeDurableSchema = args.size != 6
    val readyFile = File(args[0])
    val releaseFile = File(args[1])
    val control = if (args.size >= 5 && args[4] != "-") {
        SharedCockroachFixtureControl(File(args[4]).canonicalFile)
    } else {
        null
    }
    if (args.size >= 5) {
        val armedFile = File(args[2])
        writePropertiesAtomically(
            armedFile,
            versionedProperties().apply {
                setProperty("pid", ProcessHandle.current().pid().toString())
            },
        )
        waitForFileCreation(File(args[3]))
    }
    SharedCockroachCluster(fixtureControl = control).start().use { cluster ->
        if (initializeDurableSchema) {
            Database(
                "org.postgresql.Driver",
                cluster.jdbcUrl(),
                cluster.username,
                cluster.password,
            ).use { database ->
                DurableSimpleFileSystemManager(
                    blobstoreService = InMemoryBlobstoreService(),
                    metadataDatabase = database,
                ).use { manager ->
                    manager.listFilesystems(null, 1)
                }
            }
        }
        val diagnostics = cluster.diagnostics()
        writePropertiesAtomically(
            readyFile,
            versionedProperties().apply {
                setProperty("jdbcUrl", cluster.jdbcUrl())
                setProperty("stateDirectory", diagnostics.stateDirectory.absolutePath)
                setProperty("lockFile", diagnostics.lockFile.absolutePath)
                setProperty("token", diagnostics.electionToken)
                setProperty("daemonPid", diagnostics.daemonPid.toString())
                setProperty(
                    "daemonStartedAtMillis",
                    diagnostics.daemonStartedAtMillis.toString(),
                )
                setProperty("cockroachPid", diagnostics.cockroachPid.toString())
                setProperty(
                    "cockroachStartedAtMillis",
                    diagnostics.cockroachStartedAtMillis.toString(),
                )
                setProperty(
                    "cockroachProcessGroupId",
                    diagnostics.cockroachProcessGroupId.toString(),
                )
            },
        )
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
