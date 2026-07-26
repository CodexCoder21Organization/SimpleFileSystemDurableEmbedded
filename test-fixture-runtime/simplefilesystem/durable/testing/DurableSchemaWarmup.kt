package simplefilesystem.durable.testing

import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import java.sql.DriverManager
import java.util.UUID
import simplefilesystem.durable.DurableSimpleFileSystemManager
import sql.Database

internal fun warmUpDurableSchema(adminJdbcUrl: String) {
    val databaseName = "durable_fixture_warmup_${UUID.randomUUID().toString().replace("-", "")}"
    val databaseJdbcUrl = fixtureJdbcUrlForDatabase(adminJdbcUrl, databaseName)
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(adminJdbcUrl, "root", "").use { connection ->
        connection.createStatement().use { statement ->
            statement.execute("CREATE DATABASE ${fixtureQuoteIdentifier(databaseName)}")
        }
    }
    try {
        Database("org.postgresql.Driver", databaseJdbcUrl, "root", "").use { database ->
            DurableSimpleFileSystemManager(
                blobstoreService = InMemoryBlobstoreService(),
                metadataDatabase = database,
            ).use { manager ->
                manager.listFilesystems(null, 1)
            }
        }
    } finally {
        DriverManager.getConnection(adminJdbcUrl, "root", "").use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    "DROP DATABASE IF EXISTS ${fixtureQuoteIdentifier(databaseName)} CASCADE",
                )
            }
        }
    }
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
