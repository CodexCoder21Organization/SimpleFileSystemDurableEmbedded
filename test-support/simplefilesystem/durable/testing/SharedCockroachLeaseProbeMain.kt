package simplefilesystem.durable.testing

import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption

/**
 * Child-process probe used by the cross-process fixture test.
 *
 * The process holds one managed-node lease until its caller creates the requested release file, allowing
 * independently forked JVMs to overlap without exposing fixture internals as production API.
 */
object SharedCockroachLeaseProbe

fun main(args: Array<String>) {
    require(args.size == 2) {
        "SharedCockroachLeaseProbeMain requires <ready-file> <release-file>, but received ${args.size} argument(s)."
    }
    val readyFile = File(args[0])
    val releaseFile = File(args[1])
    SharedCockroachCluster().start().use { cluster ->
        val stagingFile = File(readyFile.parentFile, "${readyFile.name}.part")
        stagingFile.writeText(cluster.jdbcUrl())
        try {
            Files.move(
                stagingFile.toPath(),
                readyFile.toPath(),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING,
            )
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(
                stagingFile.toPath(),
                readyFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
            )
        }
        while (!releaseFile.isFile) {
            Thread.sleep(10L)
        }
    }
}
