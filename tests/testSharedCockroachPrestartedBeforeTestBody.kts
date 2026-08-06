@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import simplefilesystem.durable.testing.SharedCockroachCluster

fun testSharedCockroachPrestartedBeforeTestBody() {
    val testProcessStartedAtMillis = ProcessHandle.current()
        .info()
        .startInstant()
        .orElseThrow()
        .toEpochMilli()
    SharedCockroachCluster().start().use { cluster ->
        val diagnostics = cluster.diagnostics()
        assertTrue(
            cluster.fixtureWasReadyBeforeStart(),
            "The shared CockroachDB fixture must already be ready when acquisition starts; " +
                "test bodies must never start the database process.",
        )
        assertTrue(
            diagnostics.fixtureOwnerStartedAtMillis < testProcessStartedAtMillis,
            "The fixture owner must start in the build phase before test JVM " +
                "${ProcessHandle.current().pid()}, but owner ${diagnostics.fixtureOwnerPid} " +
                "started at ${diagnostics.fixtureOwnerStartedAtMillis} and the test JVM started " +
                "at $testProcessStartedAtMillis.",
        )
        assertTrue(
            diagnostics.cockroachStartedAtMillis < testProcessStartedAtMillis,
            "CockroachDB must start in the build phase before test JVM " +
                "${ProcessHandle.current().pid()}, but CockroachDB ${diagnostics.cockroachPid} " +
                "started at ${diagnostics.cockroachStartedAtMillis} and the test JVM started at " +
                "$testProcessStartedAtMillis.",
        )
        DriverManager.getConnection(cluster.jdbcUrl(), cluster.username, cluster.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT 1").use { rows ->
                    assertTrue(rows.next(), "The build-phase CockroachDB fixture returned no row.")
                    assertEquals(1, rows.getInt(1), "The build-phase CockroachDB fixture returned the wrong value.")
                    assertFalse(rows.next(), "The build-phase CockroachDB fixture returned an extra row.")
                }
            }
        }
        cluster.openDatabase().use { database ->
            assertEquals(
                1L,
                database.getLong(
                    """SELECT count(*) FROM crdb_internal.cluster_sessions
                        WHERE application_name = current_setting('application_name')""".trimIndent(),
                ),
                "The test fixture must open only the connection needed by its first query.",
            )
        }
    }
}
