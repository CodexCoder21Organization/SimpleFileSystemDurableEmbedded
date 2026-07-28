@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import simplefilesystem.durable.testing.SharedCockroachCluster

fun testSharedCockroachPrestartedBeforeTestBody() {
    SharedCockroachCluster().start().use { cluster ->
        assertTrue(
            cluster.fixtureWasReadyBeforeStart(),
            "The shared CockroachDB fixture must be ready before this test body's per-test clock " +
                "starts, but this test had to wait for managed-node startup.",
        )
        cluster.openDatabase().use { database ->
            assertEquals(
                16L,
                database.getLong(
                    """SELECT count(*) FROM [SHOW DATABASES]
                        WHERE database_name LIKE 'durable_fixture_pool_%'""".trimIndent(),
                ),
                "The build-phase prestart must finish the complete fixture database pool before " +
                    "Kompile dispatches test bodies.",
            )
            assertEquals(
                1L,
                database.getLong(
                    """SELECT count(*) FROM crdb_internal.cluster_sessions
                        WHERE application_name = current_setting('application_name')""".trimIndent(),
                ),
                "The test fixture must open only the connection needed by its first query.",
            )
            listOf(
                "sql.stats.automatic_collection.enabled",
                "sql.metrics.statement_details.enabled",
                "sql.metrics.transaction_details.enabled",
                "sql.metrics.index_usage_stats.enabled",
                "sql.stats.flush.enabled",
                "admission.kv.enabled",
                "admission.sql_kv_response.enabled",
                "admission.sql_sql_response.enabled",
                "admission.elastic_cpu.enabled",
                "admission.disk_bandwidth_tokens.elastic.enabled",
            ).forEach { setting ->
                assertFalse(
                    database
                        .getRow("SHOW CLUSTER SETTING $setting")
                        .results
                        .values
                        .single() as Boolean,
                    "The disposable test node must disable $setting so observability bookkeeping " +
                        "does not compete with foreground schema and conformance work.",
                )
            }
        }
    }
}
