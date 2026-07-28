@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.net.URI
import java.sql.DriverManager
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
        val fixtureDatabaseName = DriverManager.getConnection(
            cluster.jdbcUrl(),
            cluster.username,
            cluster.password,
        ).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT current_database()").use { rows ->
                    assertTrue(rows.next(), "The fixture database-name query returned no row.")
                    rows.getString(1)
                }
            }.also { databaseName ->
                connection.createStatement().use { statement ->
                    statement.executeQuery(
                        """SELECT schema_version
                            FROM simple_filesystem_schema_version
                            WHERE singleton = true""".trimIndent(),
                    ).use { rows ->
                        assertTrue(
                            rows.next(),
                            "Fixture database '$databaseName' was acquired before its durable " +
                                "schema was initialized.",
                        )
                        assertEquals(
                            2,
                            rows.getInt("schema_version"),
                            "Fixture database '$databaseName' did not contain the complete current " +
                                "durable schema before acquisition.",
                        )
                        assertFalse(
                            rows.next(),
                            "Fixture database '$databaseName' contained more than one singleton " +
                                "schema-version row.",
                        )
                    }
                }
            }
        }
        assertTrue(
            fixtureDatabaseName.startsWith("durable_fixture_pool_"),
            "The acquired database '$fixtureDatabaseName' was created cold by this test instead " +
                "of being claimed from the schema-ready fixture pool.",
        )

        val fixtureUri = URI(cluster.jdbcUrl().removePrefix("jdbc:"))
        check(
            cluster.jdbcUrl().startsWith("jdbc:") &&
                fixtureUri.scheme == "postgresql" &&
                fixtureUri.host != null &&
                fixtureUri.port >= 0,
        ) {
            "The fixture JDBC URL must have the form " +
                "jdbc:postgresql://host:port/database?parameters, but was ${cluster.jdbcUrl()}."
        }
        val adminJdbcUrl = "jdbc:" + URI(
            fixtureUri.scheme,
            fixtureUri.userInfo,
            fixtureUri.host,
            fixtureUri.port,
            "/defaultdb",
            fixtureUri.query,
            fixtureUri.fragment,
        )
        DriverManager.getConnection(adminJdbcUrl, cluster.username, cluster.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(
                    "SELECT count(*) FROM simple_filesystem_fixture_database_pool",
                ).use { rows ->
                    assertTrue(rows.next(), "The fixture database-pool count query returned no row.")
                    assertTrue(
                        rows.getInt(1) >= 5,
                        "Build-phase prestart must publish at least five individually schema-ready " +
                            "databases for four-way dispatch plus the fixture-isolation test's " +
                            "nested acquisition, but published only ${rows.getInt(1)}.",
                    )
                }
            }
            connection.prepareStatement(
                """SELECT lease_token, owner_pid, owner_started_at_millis, needs_reset
                    FROM simple_filesystem_fixture_database_pool
                    WHERE database_name = ?""".trimIndent(),
            ).use { statement ->
                statement.setString(1, fixtureDatabaseName)
                statement.executeQuery().use { rows ->
                    assertTrue(
                        rows.next(),
                        "Acquired database '$fixtureDatabaseName' had no per-database readiness " +
                            "record in the fixture pool.",
                    )
                    assertTrue(
                        !rows.getString("lease_token").isNullOrBlank(),
                        "Acquired fixture database '$fixtureDatabaseName' did not have a live " +
                            "lease token.",
                    )
                    assertEquals(
                        ProcessHandle.current().pid(),
                        rows.getLong("owner_pid"),
                        "Fixture database '$fixtureDatabaseName' was not leased to this test JVM.",
                    )
                    assertEquals(
                        ProcessHandle.current().info().startInstant().orElseThrow().toEpochMilli(),
                        rows.getLong("owner_started_at_millis"),
                        "Fixture database '$fixtureDatabaseName' lease did not identify this test " +
                            "JVM's process generation.",
                    )
                    assertFalse(
                        rows.getBoolean("needs_reset"),
                        "Fixture database '$fixtureDatabaseName' remained dirty when acquisition " +
                            "returned.",
                    )
                    assertFalse(
                        rows.next(),
                        "Fixture pool contained duplicate readiness records for " +
                            "'$fixtureDatabaseName'.",
                    )
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
