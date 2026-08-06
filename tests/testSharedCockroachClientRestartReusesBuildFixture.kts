@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import simplefilesystem.durable.testing.startSelfHostedSharedCockroachBuildPhaseFixture

fun testSharedCockroachClientRestartReusesBuildFixture() {
    startSelfHostedSharedCockroachBuildPhaseFixture().use { fixture ->
        val first = fixture.newClient().start().use { cluster ->
            val diagnostics = cluster.diagnostics()
            val identity = DriverManager.getConnection(
                cluster.jdbcUrl(),
                cluster.username,
                cluster.password,
            ).use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT crdb_internal.cluster_id(), current_database()").use { rows ->
                        check(rows.next()) { "The first fixture identity query returned no row." }
                        rows.getString(1) to rows.getString(2)
                    }
                }
            }
            Triple(diagnostics, identity.first, identity.second)
        }
        val second = fixture.newClient().start().use { cluster ->
            val diagnostics = cluster.diagnostics()
            val identity = DriverManager.getConnection(
                cluster.jdbcUrl(),
                cluster.username,
                cluster.password,
            ).use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT crdb_internal.cluster_id(), current_database()").use { rows ->
                        check(rows.next()) { "The restarted fixture client identity query returned no row." }
                        rows.getString(1) to rows.getString(2)
                    }
                }
            }
            Triple(diagnostics, identity.first, identity.second)
        }
        assertEquals(
            first.first.fixtureOwnerPid,
            second.first.fixtureOwnerPid,
            "Restarting a fixture client must reuse the build-phase owner process.",
        )
        assertEquals(
            first.first.cockroachPid,
            second.first.cockroachPid,
            "Restarting a fixture client must reuse the build-phase CockroachDB process.",
        )
        assertEquals(
            first.second,
            second.second,
            "Restarting a fixture client must reuse the same real CockroachDB cluster.",
        )
        assertNotEquals(
            first.third,
            second.third,
            "Restarting a fixture client must receive a fresh logical database after cleanup.",
        )
    }
}
