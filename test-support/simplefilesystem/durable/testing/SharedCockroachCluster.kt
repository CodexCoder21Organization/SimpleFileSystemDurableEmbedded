package simplefilesystem.durable.testing

import java.io.Closeable
import java.sql.DriverManager
import java.util.UUID
import org.apache.commons.dbcp2.BasicDataSource
import sql.Database

private const val SHARED_COCKROACH_JDBC_URL_ENV =
    "SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL"

/**
 * Attaches one isolated logical database to the real CockroachDB node started during the build
 * phase. Runtime acquisition never starts, elects, renews, or stops the shared node.
 */
class SharedCockroachCluster : Closeable {
    private val databaseName = "durable_test_${UUID.randomUUID().toString().replace("-", "")}"
    private var adminJdbcUrl: String? = null
    private var testJdbcUrl: String? = null
    private var fixtureDiagnostics: SharedCockroachFixtureDiagnostics? = null
    private var readyBeforeStart: Boolean? = null

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started."
        }
        val record = readLiveSharedCockroachFixture()
        val configuredJdbcUrl = System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)
            ?.takeIf(String::isNotBlank)
        if (configuredJdbcUrl != null) {
            check(configuredJdbcUrl == record.jdbcUrl) {
                "The configured shared CockroachDB JDBC URL '$configuredJdbcUrl' does not match " +
                    "the build-phase fixture record '${record.jdbcUrl}' at " +
                    "${sharedCockroachReadyFile().absolutePath}."
            }
        }
        val sharedJdbcUrl = configuredJdbcUrl ?: record.jdbcUrl
        try {
            Class.forName("org.postgresql.Driver")
            DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE ${quoteIdentifier(databaseName)}")
                }
            }
            adminJdbcUrl = sharedJdbcUrl
            testJdbcUrl = jdbcUrlForDatabase(sharedJdbcUrl, databaseName)
            fixtureDiagnostics = record.toDiagnostics()
            readyBeforeStart = true
            return this
        } catch (failure: Throwable) {
            runCatching {
                DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute("DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE")
                    }
                }
            }.exceptionOrNull()?.let(failure::addSuppressed)
            throw failure
        }
    }

    fun jdbcUrl(): String = testJdbcUrl
        ?: throw IllegalStateException(
            "Cannot create a JDBC URL because this SharedCockroachCluster has not been started; " +
                "call start() first.",
        )

    fun diagnostics(): SharedCockroachFixtureDiagnostics = fixtureDiagnostics
        ?: throw IllegalStateException(
            "Shared fixture diagnostics are unavailable because this SharedCockroachCluster has " +
                "not been started.",
        )

    fun fixtureWasReadyBeforeStart(): Boolean = readyBeforeStart
        ?: throw IllegalStateException(
            "Shared fixture readiness is unavailable because this SharedCockroachCluster has not " +
                "been started.",
        )

    /** Opens a bounded, lazy connection pool for this process-local test database. */
    fun openDatabase(): Database = openSharedCockroachTestDatabase(jdbcUrl(), username, password)

    @Synchronized
    override fun close() {
        val sharedJdbcUrl = adminJdbcUrl ?: return
        var failure: Throwable? = null
        try {
            DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE")
                }
            }
        } catch (caught: Throwable) {
            failure = caught
        } finally {
            adminJdbcUrl = null
            testJdbcUrl = null
            fixtureDiagnostics = null
            readyBeforeStart = null
        }
        failure?.let { throw it }
    }
}

internal fun openSharedCockroachTestDatabase(
    jdbcUrl: String,
    username: String,
    password: String,
): Database {
    val applicationName =
        "simplefilesystem-durable-test-${UUID.randomUUID().toString().replace("-", "")}"
    val connectionProperties =
        "ApplicationName=$applicationName&preferQueryMode=extended&prepareThreshold=1" +
            "&preparedStatementCacheQueries=256"
    val configuredJdbcUrl = jdbcUrl + if (jdbcUrl.contains('?')) {
        "&$connectionProperties"
    } else {
        "?$connectionProperties"
    }
    val dataSource = BasicDataSource().apply {
        driverClassName = "org.postgresql.Driver"
        url = configuredJdbcUrl
        this.username = username
        this.password = password
        defaultAutoCommit = true
        initialSize = 0
        maxTotal = 20
        maxIdle = 4
        minIdle = 0
        testOnBorrow = false
        testOnReturn = false
        testWhileIdle = false
    }
    return try {
        SharedCockroachTestDatabase(Database(dataSource), dataSource)
    } catch (failure: Throwable) {
        dataSource.close()
        throw failure
    }
}

private class SharedCockroachTestDatabase(
    private val delegate: Database,
    private val dataSource: BasicDataSource,
) : Database by delegate {
    private var closed = false

    @Synchronized
    override fun close() {
        if (closed) return
        closed = true
        var failure: Throwable? = null
        try {
            delegate.close()
        } catch (caught: Throwable) {
            failure = caught
        }
        try {
            dataSource.close()
        } catch (caught: Throwable) {
            failure?.addSuppressed(caught) ?: run { failure = caught }
        }
        failure?.let { throw it }
    }
}

internal fun jdbcUrlForDatabase(adminJdbcUrl: String, databaseName: String): String {
    val queryIndex = adminJdbcUrl.indexOf('?')
    val base = if (queryIndex >= 0) adminJdbcUrl.substring(0, queryIndex) else adminJdbcUrl
    val query = if (queryIndex >= 0) adminJdbcUrl.substring(queryIndex) else ""
    val pathStart = base.indexOf('/', base.indexOf("//") + 2)
    require(pathStart >= 0) {
        "The shared CockroachDB JDBC URL must include a database path, but was '$adminJdbcUrl'."
    }
    return base.substring(0, pathStart) + "/$databaseName" + query
}

internal fun quoteIdentifier(identifier: String): String =
    "\"${identifier.replace("\"", "\"\"")}\""
