package simplefilesystem.durable.testing

import java.io.Closeable
import java.sql.DriverManager
import java.util.UUID

private const val SHARED_COCKROACH_JDBC_URL_ENV =
    "SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL"

/**
 * Attaches one isolated logical database to the CockroachDB node owned by the suite fixture.
 *
 * Each Kompile test runs in its own JVM, so the fixture endpoint is passed across that process
 * boundary while database creation and cleanup remain owned by this test instance.
 */
class SharedCockroachCluster : Closeable {
    private val databaseName = "durable_test_${UUID.randomUUID().toString().replace("-", "")}"
    private var adminJdbcUrl: String? = null
    private var testJdbcUrl: String? = null

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started"
        }
        val configuredJdbcUrl = checkNotNull(System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)) {
            "The shared CockroachDB suite fixture URL is unavailable because environment " +
                "variable $SHARED_COCKROACH_JDBC_URL_ENV is not set; run tests through " +
                "scripts/test.bash so the suite owns exactly one fixture node"
        }
        val databaseJdbcUrl = jdbcUrlForDatabase(configuredJdbcUrl, databaseName)

        Class.forName("org.postgresql.Driver")
        DriverManager.getConnection(configuredJdbcUrl, username, password).use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE DATABASE ${quoteIdentifier(databaseName)}")
            }
        }

        adminJdbcUrl = configuredJdbcUrl
        testJdbcUrl = databaseJdbcUrl
        return this
    }

    fun jdbcUrl(): String = testJdbcUrl
        ?: throw IllegalStateException(
            "Cannot create a JDBC URL because this SharedCockroachCluster has not been started; " +
                "call start() first",
        )

    @Synchronized
    override fun close() {
        val configuredJdbcUrl = adminJdbcUrl ?: return
        try {
            DriverManager.getConnection(configuredJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE")
                }
            }
        } finally {
            adminJdbcUrl = null
            testJdbcUrl = null
        }
    }
}

private fun jdbcUrlForDatabase(adminJdbcUrl: String, databaseName: String): String {
    val match = Regex("""^(jdbc:postgresql://[^/]+)/[^?]+(\?.*)?$""").matchEntire(adminJdbcUrl)
        ?: throw IllegalArgumentException(
            "The shared CockroachDB JDBC URL must have the form " +
                "jdbc:postgresql://host:port/database?parameters, but was $adminJdbcUrl",
        )
    return match.groupValues[1] + "/" + databaseName + match.groupValues[2]
}

private fun quoteIdentifier(identifier: String): String =
    "\"" + identifier.replace("\"", "\"\"") + "\""
