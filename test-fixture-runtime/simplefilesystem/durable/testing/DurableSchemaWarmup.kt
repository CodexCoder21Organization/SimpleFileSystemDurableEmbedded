package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.util.UUID
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

internal fun warmUpDurableSchema(
    adminJdbcUrl: String,
    controlDirectory: File? = null,
    token: String = "suite-fixture",
    ownershipDirectory: File? = null,
    verifyOwnership: () -> Unit = {},
) {
    waitAtWarmupBarrier(controlDirectory, token, ownershipDirectory, verifyOwnership)
    if (controlDirectory != null && File(controlDirectory, "fail-warmup").isFile) {
        throw IllegalStateException(
            "Shared CockroachDB warmup was forced to fail by " +
                File(controlDirectory, "fail-warmup").absolutePath +
                " for election token '$token'.",
        )
    }
    verifyOwnership()
    val databaseName = "durable_fixture_warmup_${UUID.randomUUID().toString().replace("-", "")}"
    val databaseJdbcUrl = fixtureJdbcUrlForDatabase(adminJdbcUrl, databaseName)
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(adminJdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("CREATE DATABASE ${fixtureQuoteIdentifier(databaseName)}")
        }
    }
    Database("org.postgresql.Driver", databaseJdbcUrl, "root", "").use { database ->
        DurableSimpleFileSystemManager(
            blobstoreService = InMemoryBlobstoreService(),
            metadataDatabase = database,
        ).use { manager ->
            manager.listFilesystems(null, 1)
        }
    }
    // The fixture owns an in-memory node, so retaining this database keeps the schema warm without
    // paying for DROP DATABASE CASCADE while concurrent test JVMs share a two-CPU worker.
    verifyOwnership()
}

private fun waitAtWarmupBarrier(
    controlDirectory: File?,
    token: String,
    ownershipDirectory: File?,
    verifyOwnership: () -> Unit,
) {
    if (controlDirectory == null || !File(controlDirectory, "pause-warmup").isFile) return
    check(controlDirectory.isDirectory || controlDirectory.mkdirs()) {
        "Could not create shared CockroachDB fixture-control directory " +
            controlDirectory.absolutePath
    }
    writePropertiesAtomically(
        File(controlDirectory, "warmup-arrived-$token.properties"),
        versionedProperties().apply {
            setProperty("token", token)
            setProperty("pid", ProcessHandle.current().pid().toString())
        },
    )
    val releaseFile = File(controlDirectory, "warmup-release-$token")
    FileSystems.getDefault().newWatchService().use { watcher ->
        controlDirectory.toPath().register(
            watcher,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
        )
        ownershipDirectory?.toPath()?.register(
            watcher,
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE,
        )
        while (!releaseFile.isFile) {
            verifyOwnership()
            val key = try {
                watcher.take()
            } catch (failure: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IllegalStateException(
                    "Interrupted while shared CockroachDB election token '$token' waited at the " +
                        "production-schema warmup barrier.",
                    failure,
                )
            }
            key.pollEvents()
            check(key.reset()) {
                "Could not continue watching ${controlDirectory.absolutePath} for " +
                    "${releaseFile.name}."
            }
        }
    }
    verifyOwnership()
}

internal fun fixtureJdbcUrlForDatabase(adminJdbcUrl: String, databaseName: String): String {
    val match = Regex("""^(jdbc:postgresql://[^/]+)/[^?]+(\?.*)?$""").matchEntire(adminJdbcUrl)
        ?: throw IllegalArgumentException(
            "The fixture CockroachDB JDBC URL must have the form " +
                "jdbc:postgresql://host:port/database?parameters, but was $adminJdbcUrl",
        )
    return match.groupValues[1] + "/" + databaseName + match.groupValues[2]
}

internal fun fixtureQuoteIdentifier(identifier: String): String =
    "\"" + identifier.replace("\"", "\"\"") + "\""
