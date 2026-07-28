package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

fun main(args: Array<String>) {
    require(args.size == 2 || args.size == 3 || args.size == 5 || args.size == 6) {
        "SharedCockroachLeaseProbeMain requires <ready-file> <release-file> or <ready-file> " +
            "<release-file> <initial-schema-ready-databases> or <ready-file> " +
            "<release-file> <armed-file> <start-gate> <control-directory-or-dash> " +
            "[lease-only|node-only|schema-ready], but received ${args.size} argument(s)."
    }
    require(
        args.size != 6 ||
            args[5] == "lease-only" ||
            args[5] == "node-only" ||
            args[5] == "schema-ready",
    ) {
        "SharedCockroachLeaseProbeMain optional mode must be 'lease-only', 'node-only', or " +
            "'schema-ready', but was '${args[5]}'."
    }
    val initializeDurableSchema = args.size != 6 && args.size != 3
    val initialSchemaReadyDatabases = if (args.size == 3) {
        args[2].toIntOrNull()
            ?.takeIf { it in 1..FIXTURE_DATABASE_POOL_SIZE }
            ?: throw IllegalArgumentException(
                "Shared CockroachDB probe initial schema-ready database count must be between 1 " +
                    "and $FIXTURE_DATABASE_POOL_SIZE, but was '${args[2]}'.",
            )
    } else {
        FIXTURE_DATABASE_READY_POOL_SIZE
    }
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
    val cluster = when {
        args.size == 6 && args[5] == "node-only" ->
            sharedCockroachNodeLease(control)
        args.size == 6 && args[5] == "schema-ready" ->
            preinitializedSharedCockroachDatabaseLease(
                control,
                FIXTURE_DATABASE_READY_POOL_SIZE,
            )
        args.size == 6 ->
            uninitializedSharedCockroachDatabaseLease(control)
        initialSchemaReadyDatabases > FIXTURE_DATABASE_READY_POOL_SIZE ->
            preinitializedSharedCockroachDatabaseLease(
                control,
                initialSchemaReadyDatabases,
            )
        else -> SharedCockroachCluster(fixtureControl = control)
    }
    cluster.start().use {
        if (initializeDurableSchema) {
            Database(
                "org.postgresql.Driver",
                it.jdbcUrl(),
                it.username,
                it.password,
            ).use { database ->
                DurableSimpleFileSystemManager(
                    blobstoreService = InMemoryBlobstoreService(),
                    metadataDatabase = database,
                ).use { manager ->
                    manager.listFilesystems(null, 1)
                }
            }
        }
        val diagnostics = it.diagnostics()
        writePropertiesAtomically(
            readyFile,
            versionedProperties().apply {
                setProperty("jdbcUrl", it.jdbcUrl())
                setProperty("stateDirectory", diagnostics.stateDirectory.absolutePath)
                setProperty(
                    "userDirectory",
                    File(System.getProperty("user.dir")).canonicalPath,
                )
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
