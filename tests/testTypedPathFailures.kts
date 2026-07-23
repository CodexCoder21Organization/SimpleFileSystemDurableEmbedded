@file:WithArtifact("simplefilesystem.durable:simplefilesystem-durable-embedded:")
@file:WithArtifact("simplefilesystem.durable.buildCockroachTestFixtureFatJar()")
@file:WithArtifact("build.kotlin.annotations:build-kotlin-annotations:0.0.2")
@file:WithArtifact("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3")
@file:WithArtifact("sql:sql-api:0.0.1")
@file:WithArtifact("sql:sql:0.0.2")
@file:WithArtifact("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3")
@file:WithArtifact("org.postgresql:postgresql:42.6.0")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")
@file:WithArtifact("org.jetbrains.kotlin:kotlin-test:1.9.22")
package simplefilesystem.durable

import build.kotlin.withartifact.WithArtifact
import simplefilesystem.durable.testing.SharedCockroachCluster
import community.kotlin.blobstore.inmemory.InMemoryBlobstoreService
import community.kotlin.clocks.simple.ManualClock
import kotlin.test.assertEquals
import simplefilesystem.InvalidByteRangeException
import simplefilesystem.InvalidPathException
import simplefilesystem.InvalidPathReason
import simplefilesystem.PathAlreadyExistsException
import simplefilesystem.PathNotFoundException
import simplefilesystem.PathTypeMismatchException
import sql.Database

fun testTypedPathFailures() {
    val cluster = SharedCockroachCluster().start()
    try {
        val database = Database("org.postgresql.Driver", cluster.jdbcUrl(), cluster.username, cluster.password)
        try {
            val manager = DurableSimpleFileSystemManager(InMemoryBlobstoreService(), database, ManualClock(1L))
            val filesystem = manager.openFilesystem(manager.createFilesystem("errors", 100L).uuid)
            filesystem.createDirectory("/dir", true)
            filesystem.writeUtf8("/file", "abc", null)

            val missing = runCatching { filesystem.readUtf8("/missing") }.exceptionOrNull()
            assertEquals("simplefilesystem.PathNotFoundException", missing?.javaClass?.name)
            assertEquals("Path '/missing' does not exist in this filesystem.", missing?.message)
            val exists = runCatching { filesystem.createDirectory("/dir", true) }.exceptionOrNull()
            assertEquals("simplefilesystem.PathAlreadyExistsException", exists?.javaClass?.name)
            val wrongType = runCatching { filesystem.list("/file", null, 100) }.exceptionOrNull()
            assertEquals("simplefilesystem.PathTypeMismatchException", wrongType?.javaClass?.name)
            assertEquals(
                "Path 'relative' is invalid for reason NOT_ABSOLUTE: every non-root path must begin with '/'.",
                runCatching { filesystem.exists("relative") }.exceptionOrNull()?.message,
            )
            val grammar = listOf(
                "" to InvalidPathReason.EMPTY,
                "/trailing/" to InvalidPathReason.TRAILING_SEPARATOR,
                "/repeated//separator" to InvalidPathReason.REPEATED_SEPARATOR,
                "/./dot" to InvalidPathReason.DOT_SEGMENT,
                "/../parent" to InvalidPathReason.PARENT_SEGMENT,
                "/nul\u0000value" to InvalidPathReason.NUL_CHARACTER,
                "/malformed-\uD800" to InvalidPathReason.MALFORMED_UNICODE,
                ("/" + "a".repeat(256)) to InvalidPathReason.SEGMENT_TOO_LONG,
                ("/" + List(17) { "a".repeat(240) }.joinToString("/")) to InvalidPathReason.PATH_TOO_LONG,
                ("/" + List(129) { "a" }.joinToString("/")) to InvalidPathReason.TOO_DEEP,
            )
            grammar.forEach { (invalidPath, reason) ->
                val failure = runCatching { filesystem.exists(invalidPath) }.exceptionOrNull() as InvalidPathException
                assertEquals(reason, failure.reason)
            }
            val rootDelete = runCatching { filesystem.delete("/", false) }.exceptionOrNull() as InvalidPathException
            assertEquals(InvalidPathReason.ROOT_NOT_ALLOWED_FOR_OPERATION, rootDelete.reason)

            val intermediateType = runCatching { filesystem.metadata("/file/child") }.exceptionOrNull()
            assertEquals("simplefilesystem.PathTypeMismatchException", intermediateType?.javaClass?.name)
            val range = runCatching { filesystem.source("/file", 2L, 2L) }.exceptionOrNull()
            assertEquals("simplefilesystem.InvalidByteRangeException", range?.javaClass?.name)
            assertEquals(
                "Byte range offset=2, byteCount=2 is invalid for path '/file' with size 3 bytes; offset and " +
                    "byteCount must be non-negative, addition must not overflow, and the range must end at or before byte 3.",
                range?.message,
            )
        } finally {
            database.close()
        }
    } finally {
        cluster.close()
    }
}
