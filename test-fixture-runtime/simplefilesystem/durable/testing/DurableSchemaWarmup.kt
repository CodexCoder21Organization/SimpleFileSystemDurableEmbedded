package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import java.sql.DriverManager
import java.util.UUID
import simplefilesystem.durable.DurableSimpleFileSystemManager

internal fun warmUpDurableSchema(
    adminJdbcUrl: String,
    controlDirectory: File? = null,
    token: String = "suite-fixture",
    ownershipDirectory: File? = null,
    targetPoolSize: Int,
    observeControlBarrier: Boolean = true,
    prepareCluster: () -> Unit = {},
    verifyOwnership: () -> Unit = {},
) {
    require(targetPoolSize in 1..FIXTURE_DATABASE_POOL_SIZE) {
        "Fixture database warmup target must be between 1 and $FIXTURE_DATABASE_POOL_SIZE, " +
            "but was $targetPoolSize."
    }
    observeDurableSchemaWarmupControl(
        controlDirectory,
        token,
        ownershipDirectory,
        observeControlBarrier,
        verifyOwnership,
    )
    verifyOwnership()
    prepareCluster()
    verifyOwnership()
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(adminJdbcUrl, "root", "").use { adminConnection ->
        adminConnection.createStatement().use { statement ->
            statement.execute(
                """CREATE TABLE IF NOT EXISTS $FIXTURE_DATABASE_POOL_TABLE (
                    database_name STRING PRIMARY KEY,
                    lease_token STRING NULL,
                    owner_pid INT8 NULL,
                    owner_started_at_millis INT8 NULL,
                    needs_reset BOOL NOT NULL DEFAULT false
                );
                ALTER TABLE $FIXTURE_DATABASE_POOL_TABLE
                    ADD COLUMN IF NOT EXISTS needs_reset BOOL NOT NULL DEFAULT false""".trimIndent(),
            )
        }
        val existingPoolSize = adminConnection.createStatement().use { statement ->
            statement.executeQuery("SELECT count(*) FROM $FIXTURE_DATABASE_POOL_TABLE").use { rows ->
                check(rows.next())
                rows.getInt(1)
            }
        }
        repeat((targetPoolSize - existingPoolSize).coerceAtLeast(0)) {
            verifyOwnership()
            val databaseName =
                "durable_fixture_pool_${UUID.randomUUID().toString().replace("-", "")}"
            val databaseJdbcUrl = fixtureJdbcUrlForDatabase(adminJdbcUrl, databaseName)
            adminConnection.createStatement().use { statement ->
                statement.execute("CREATE DATABASE ${fixtureQuoteIdentifier(databaseName)}")
            }
            try {
                openSharedCockroachTestDatabase(databaseJdbcUrl, "root", "").use { database ->
                    DurableSimpleFileSystemManager(
                        blobstoreService = InMemoryBlobstoreService(),
                        metadataDatabase = database,
                    ).use { manager ->
                        manager.listFilesystems(null, 1)
                    }
                }
                adminConnection.prepareStatement(
                    """INSERT INTO $FIXTURE_DATABASE_POOL_TABLE
                        (database_name, lease_token, owner_pid, owner_started_at_millis, needs_reset)
                        VALUES (?, NULL, NULL, NULL, false)""".trimIndent(),
                ).use { statement ->
                    statement.setString(1, databaseName)
                    statement.executeUpdate()
                }
            } catch (failure: Throwable) {
                try {
                    adminConnection.createStatement().use { statement ->
                        statement.execute(
                            "DROP DATABASE IF EXISTS ${fixtureQuoteIdentifier(databaseName)} CASCADE",
                        )
                    }
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressed(cleanupFailure)
                }
                throw failure
            }
        }
    }
    verifyOwnership()
}

internal fun observeDurableSchemaWarmupControl(
    controlDirectory: File?,
    token: String,
    ownershipDirectory: File?,
    observeControlBarrier: Boolean = true,
    verifyOwnership: () -> Unit = {},
) {
    if (observeControlBarrier) {
        waitAtWarmupBarrier(controlDirectory, token, ownershipDirectory, verifyOwnership)
    }
    val failureControl = controlDirectory?.let { File(it, "fail-warmup") }
    if (observeControlBarrier && failureControl?.isFile == true) {
        throw IllegalStateException(
            "Shared CockroachDB warmup was forced to fail by ${failureControl.absolutePath} " +
                "for election token '$token'.",
        )
    }
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
