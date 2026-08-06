package simplefilesystem.durable.testing

import community.kotlin.clocks.simple.Clock
import community.kotlin.clocks.simple.SystemClock
import java.io.Closeable
import java.sql.DriverManager
import java.util.UUID
import org.apache.commons.dbcp2.BasicDataSource
import sql.Database

private const val SHARED_COCKROACH_JDBC_URL_ENV =
    "SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL"

/**
 * Attaches one isolated logical database to a real CockroachDB node. A live build-phase readiness
 * record is the fast path; direct test dispatch without that record uses main's per-test-JVM
 * lock/state/lease fallback.
 */
class SharedCockroachCluster(
    private val clock: Clock = SystemClock(),
) : Closeable {
    private val databaseName = "durable_test_${UUID.randomUUID().toString().replace("-", "")}"
    private var adminJdbcUrl: String? = null
    private var testJdbcUrl: String? = null
    private var fixtureDiagnostics: SharedCockroachFixtureDiagnostics? = null
    private var readyBeforeStart: Boolean? = null
    private var fallbackNodeLease = false

    val username: String = "root"
    val password: String = ""

    @Synchronized
    fun start(): SharedCockroachCluster {
        check(adminJdbcUrl == null) {
            "This SharedCockroachCluster was already started."
        }
        val record = readLiveSharedCockroachFixtureOrNull()
        val configuredJdbcUrl = System.getenv(SHARED_COCKROACH_JDBC_URL_ENV)
            ?.takeIf(String::isNotBlank)
        val sharedJdbcUrl = if (record != null) {
            if (configuredJdbcUrl != null) {
                check(configuredJdbcUrl == record.jdbcUrl) {
                    "The configured shared CockroachDB JDBC URL '$configuredJdbcUrl' does not match " +
                        "the build-phase fixture record '${record.jdbcUrl}' at " +
                        "${sharedCockroachReadyFile().absolutePath}."
                }
            }
            configuredJdbcUrl ?: record.jdbcUrl
        } else {
            FallbackCockroachNode.acquire(databaseName, clock).also {
                fallbackNodeLease = true
            }
        }
        try {
            Class.forName("org.postgresql.Driver")
            DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE DATABASE ${quoteIdentifier(databaseName)}")
                }
            }
            adminJdbcUrl = sharedJdbcUrl
            testJdbcUrl = jdbcUrlForDatabase(sharedJdbcUrl, databaseName)
            fixtureDiagnostics = record?.toDiagnostics()
            readyBeforeStart = record != null
            return this
        } catch (failure: Throwable) {
            if (fallbackNodeLease) {
                runCatching { FallbackCockroachNode.release(databaseName) }
                    .exceptionOrNull()
                    ?.let(failure::addSuppressed)
                fallbackNodeLease = false
            } else {
                runCatching {
                    DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                        connection.createStatement().use { statement ->
                            statement.execute(
                                "DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE",
                            )
                        }
                    }
                }.exceptionOrNull()?.let(failure::addSuppressed)
            }
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
            "Build-phase fixture diagnostics are unavailable because this SharedCockroachCluster " +
                "has not been started through the build-phase readiness fast path.",
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
            if (!fallbackNodeLease) {
                DriverManager.getConnection(sharedJdbcUrl, username, password).use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(
                            "DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} CASCADE",
                        )
                    }
                }
            }
        } catch (caught: Throwable) {
            failure = caught
        } finally {
            adminJdbcUrl = null
            testJdbcUrl = null
            fixtureDiagnostics = null
            readyBeforeStart = null
            if (fallbackNodeLease) {
                try {
                    FallbackCockroachNode.release(databaseName)
                } catch (cleanupFailure: Throwable) {
                    failure?.addSuppressed(cleanupFailure) ?: run { failure = cleanupFailure }
                } finally {
                    fallbackNodeLease = false
                }
            }
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
