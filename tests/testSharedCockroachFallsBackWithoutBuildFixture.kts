@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

fun testSharedCockroachFallsBackWithoutBuildFixture() {
    val privateTemp = Files.createTempDirectory("shared-cockroach-fallback-").toFile()
    val resultFile = File(privateTemp, "fallback-result.properties")
    val outputFile = File(privateTemp, "fallback-probe.log")
    val probeSource = File(privateTemp, "FallbackProbe.java").apply {
        writeText(
            """
            import java.io.FileOutputStream;
            import java.sql.Connection;
            import java.sql.ResultSet;
            import java.sql.Statement;
            import java.util.Properties;
            import simplefilesystem.durable.testing.SharedCockroachCluster;

            public class FallbackProbe {
                public static void main(String[] args) throws Exception {
                    try (SharedCockroachCluster cluster = new SharedCockroachCluster().start()) {
                        Properties result = new Properties();
                        result.setProperty(
                            "fallbackEngaged",
                            String.valueOf(!cluster.fixtureWasReadyBeforeStart())
                        );
                        try (
                            Connection connection = java.sql.DriverManager.getConnection(
                                cluster.jdbcUrl(), cluster.getUsername(), cluster.getPassword()
                            );
                            Statement statement = connection.createStatement();
                            ResultSet rows = statement.executeQuery(
                                "SELECT crdb_internal.cluster_id(), current_database()"
                            )
                        ) {
                            if (!rows.next()) throw new IllegalStateException(
                                "The fallback fixture identity query returned no row."
                            );
                            result.setProperty("clusterId", rows.getString(1));
                            result.setProperty("databaseName", rows.getString(2));
                        }
                        try (FileOutputStream output = new FileOutputStream(args[0])) {
                            result.store(output, null);
                        }
                    }
                }
            }
            """.trimIndent(),
        )
    }
    val process = ProcessBuilder(
        File(System.getProperty("java.home"), "bin/java").absolutePath,
        "-Djava.io.tmpdir=${privateTemp.absolutePath}",
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=1",
        "-XX:TieredStopAtLevel=1",
        "-cp",
        System.getProperty("java.class.path"),
        probeSource.absolutePath,
        resultFile.absolutePath,
    )
        .directory(privateTemp)
        .redirectErrorStream(true)
        .redirectOutput(outputFile)
        .also {
            it.environment().remove("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL")
        }
        .start()
    try {
        process.waitFor()
        assertEquals(
            0,
            process.exitValue(),
            "The fixture client without a build-phase readiness record must use the real " +
                "CockroachDB fallback, but its child JVM failed:\n" +
                outputFile.takeIf(File::isFile)?.readText().orEmpty(),
        )
        val result = Properties().apply { resultFile.inputStream().use(::load) }
        assertEquals(
            "true",
            result.getProperty("fallbackEngaged"),
            "A fixture client without a build-phase readiness record must report that it used " +
                "the per-test-JVM fallback.",
        )
        assertFalse(
            result.getProperty("clusterId").isNullOrBlank(),
            "The fallback must connect to a real CockroachDB cluster with an identity.",
        )
        assertTrue(
            result.getProperty("databaseName")?.startsWith("durable_test_") == true,
            "The fallback must create an isolated logical database, but reported " +
                "'${result.getProperty("databaseName")}'.",
        )
        assertFalse(
            File(privateTemp, "simplefilesystem-durable-shared-cockroach-v1/node.properties").exists(),
            "The fallback must remove its node record after the final test-JVM lease closes.",
        )
    } finally {
        if (process.isAlive) process.destroyForcibly()
        process.waitFor(5L, TimeUnit.SECONDS)
        privateTemp.deleteRecursively()
    }
}
