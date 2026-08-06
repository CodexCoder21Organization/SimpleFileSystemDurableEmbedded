package simplefilesystem.durable.testing

import java.io.File
import java.io.RandomAccessFile
import java.sql.DriverManager
import java.util.Properties

fun main(args: Array<String>) {
    require(args.size == 6) {
        "SharedCockroachFixtureProbeMain requires <armed-file> <start-gate> <ready-file> " +
            "<release-gate> <session-owner-pid> <session-owner-started-at-millis>, but received " +
            "${args.size} argument(s)."
    }
    val armedFile = File(args[0])
    val startGate = File(args[1])
    val readyFile = File(args[2])
    val releaseGate = File(args[3])
    val sessionOwner = SharedProcessIdentity(
        pid = args[4].toLong(),
        startedAtMillis = args[5].toLong(),
    )
    armedFile.writeText("armed")
    waitForFile(startGate)
    val before = checkNotNull(readLiveSharedCockroachFixtureOrNull()) {
        "The build-phase fixture probe requires an existing readiness record at " +
            "${sharedCockroachReadyFile().absolutePath}."
    }
    val after = ensureSharedCockroachFixtureForSession(sessionOwner, emptyList())
    SharedCockroachCluster().start().use { cluster ->
        val diagnostics = cluster.diagnostics()
        val contenderAcquiredOwnerLock = RandomAccessFile(diagnostics.lockFile, "rw").use { access ->
            access.channel.tryLock()?.use { true } ?: false
        }
        val identity = DriverManager.getConnection(
            cluster.jdbcUrl(),
            cluster.username,
            cluster.password,
        ).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT crdb_internal.cluster_id(), current_database()").use { rows ->
                    check(rows.next()) { "The fixture contender identity query returned no row." }
                    rows.getString(1) to rows.getString(2)
                }
            }
        }
        writePropertiesAtomically(
            readyFile,
            Properties().apply {
                setProperty("fixtureOwnerPid", diagnostics.fixtureOwnerPid.toString())
                setProperty("cockroachPid", diagnostics.cockroachPid.toString())
                setProperty("clusterId", identity.first)
                setProperty("databaseName", identity.second)
                setProperty("contenderAcquiredOwnerLock", contenderAcquiredOwnerLock.toString())
                setProperty(
                    "fixtureOwnerChanged",
                    (before.fixtureOwner != after.fixtureOwner).toString(),
                )
            },
        )
        waitForFile(releaseGate)
    }
}

private fun waitForFile(file: File) {
    while (!file.isFile) Thread.sleep(10L)
}
