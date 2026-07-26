@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import simplefilesystem.durable.testing.SharedCockroachCluster

fun testSharedCockroachFixtureIsolation() {
    SharedCockroachCluster().start().use { first ->
        SharedCockroachCluster().start().use { second ->
            val firstUrl = first.jdbcUrl()
            val secondUrl = second.jdbcUrl()
            assertNotEquals(firstUrl, secondUrl, "Each test fixture must receive a distinct database")
            assertEquals(
                firstUrl.substringBeforeLast('/'),
                secondUrl.substringBeforeLast('/'),
                "All test databases must use the same CockroachDB node",
            )

            DriverManager.getConnection(firstUrl, first.username, first.password).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("CREATE TABLE fixture_isolation (id INT PRIMARY KEY)")
                }
            }
            DriverManager.getConnection(secondUrl, second.username, second.password).use { connection ->
                connection.prepareStatement(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                ).use { statement ->
                    statement.setString(1, "fixture_isolation")
                    statement.executeQuery().use { rows ->
                        rows.next()
                        assertEquals(
                            0,
                            rows.getInt(1),
                            "A table created in one logical database must not be visible in another",
                        )
                    }
                }
            }
        }
    }
}
