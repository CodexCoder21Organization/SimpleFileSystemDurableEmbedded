@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import simplefilesystem.durable.testing.startSelfHostedSharedCockroachBuildPhaseFixture

fun testSharedCockroachPrestartedBeforeTestBody() {
    startSelfHostedSharedCockroachBuildPhaseFixture().use { fixture ->
        val probeSource = File(fixture.workspace, "PrestartedFixtureProbe.java").apply {
            writeText(
                """
                import java.sql.Connection;
                import java.sql.ResultSet;
                import java.sql.Statement;
                import simplefilesystem.durable.testing.SharedCockroachCluster;
                import simplefilesystem.durable.testing.SharedCockroachFixtureDiagnostics;
                import sql.Database;

                public class PrestartedFixtureProbe {
                    public static void main(String[] args) throws Exception {
                        long testProcessStartedAtMillis = ProcessHandle.current()
                            .info()
                            .startInstant()
                            .orElseThrow()
                            .toEpochMilli();
                        try (SharedCockroachCluster cluster = new SharedCockroachCluster().start()) {
                            SharedCockroachFixtureDiagnostics diagnostics = cluster.diagnostics();
                            if (!cluster.fixtureWasReadyBeforeStart()) throw new AssertionError(
                                "The shared CockroachDB fixture must already be ready when acquisition starts; " +
                                "test bodies must never start the database process."
                            );
                            if (diagnostics.getFixtureOwnerStartedAtMillis() >= testProcessStartedAtMillis) {
                                throw new AssertionError(
                                    "The fixture owner must start in the build phase before test JVM " +
                                    ProcessHandle.current().pid() + ", but owner " +
                                    diagnostics.getFixtureOwnerPid() + " started at " +
                                    diagnostics.getFixtureOwnerStartedAtMillis() +
                                    " and the test JVM started at " + testProcessStartedAtMillis + "."
                                );
                            }
                            if (diagnostics.getCockroachStartedAtMillis() >= testProcessStartedAtMillis) {
                                throw new AssertionError(
                                    "CockroachDB must start in the build phase before test JVM " +
                                    ProcessHandle.current().pid() + ", but CockroachDB " +
                                    diagnostics.getCockroachPid() + " started at " +
                                    diagnostics.getCockroachStartedAtMillis() +
                                    " and the test JVM started at " + testProcessStartedAtMillis + "."
                                );
                            }
                            try (
                                Connection connection = java.sql.DriverManager.getConnection(
                                    cluster.jdbcUrl(), cluster.getUsername(), cluster.getPassword()
                                );
                                Statement statement = connection.createStatement();
                                ResultSet rows = statement.executeQuery("SELECT 1")
                            ) {
                                if (!rows.next()) throw new AssertionError(
                                    "The build-phase CockroachDB fixture returned no row."
                                );
                                if (rows.getInt(1) != 1) throw new AssertionError(
                                    "The build-phase CockroachDB fixture returned the wrong value."
                                );
                                if (rows.next()) throw new AssertionError(
                                    "The build-phase CockroachDB fixture returned an extra row."
                                );
                            }
                            try (Database database = cluster.openDatabase()) {
                                long connections = database.getLong(
                                    "SELECT count(*) FROM crdb_internal.cluster_sessions " +
                                    "WHERE application_name = current_setting('application_name')"
                                );
                                if (connections != 1L) throw new AssertionError(
                                    "The test fixture must open only the connection needed by its first query."
                                );
                            }
                        }
                    }
                }
                """.trimIndent(),
            )
        }
        val outputFile = File(fixture.workspace, "prestarted-probe.log")
        val process = fixture.clientProcessBuilder(probeSource.absolutePath)
            .redirectErrorStream(true)
            .redirectOutput(outputFile)
            .start()
        try {
            process.waitFor()
            assertEquals(
                0,
                process.exitValue(),
                "The client JVM must observe a fixture owner and CockroachDB process that were " +
                    "already live before that client JVM started, but it failed:\n" +
                    outputFile.takeIf(File::isFile)?.readText().orEmpty(),
            )
        } finally {
            if (process.isAlive) process.destroyForcibly()
            process.waitFor(5L, TimeUnit.SECONDS)
        }
    }
}
