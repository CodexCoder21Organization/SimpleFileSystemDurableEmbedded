package simplefilesystem.durable.testing

/**
 * Child-process probe used by the cross-process fixture test.
 *
 * The process holds one managed-node lease until its caller creates the requested release file, allowing
 * independently forked JVMs to overlap without exposing fixture internals as production API.
 */
object SharedCockroachLeaseProbe
