package simplefilesystem.durable.testing

import cockroachdb.testharness.LocalCockroachCluster
import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.CountDownLatch

fun main(args: Array<String>) {
    require(args.size == 1) {
        "Expected exactly one argument containing the fixture readiness-file path, but received ${args.size}"
    }
    val readyFile = File(args.single()).absoluteFile
    val cluster = LocalCockroachCluster().start()
    val shutdownHook = Thread({ cluster.close() }, "shared-cockroach-suite-fixture-shutdown")
    Runtime.getRuntime().addShutdownHook(shutdownHook)

    try {
        configureSingleNodeTestCluster(cluster.jdbcUrl())
        warmUpDurableSchema(cluster.jdbcUrl())
        readyFile.parentFile?.let { parent ->
            check(parent.isDirectory || parent.mkdirs()) {
                "Could not create the shared CockroachDB fixture directory ${parent.absolutePath}"
            }
        }
        writeAtomically(readyFile, cluster.jdbcUrl())
        CountDownLatch(1).await()
    } finally {
        readyFile.delete()
        cluster.close()
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook)
        } catch (_: IllegalStateException) {
            // The JVM is already shutting down, so the registered hook owns final cleanup.
        }
    }
}

internal fun writeAtomically(target: File, content: String) {
    val stagingFile = File(target.parentFile, "${target.name}.part")
    stagingFile.writeText(content)
    try {
        Files.move(
            stagingFile.toPath(),
            target.toPath(),
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING,
        )
    } catch (_: AtomicMoveNotSupportedException) {
        Files.move(
            stagingFile.toPath(),
            target.toPath(),
            StandardCopyOption.REPLACE_EXISTING,
        )
    }
}
