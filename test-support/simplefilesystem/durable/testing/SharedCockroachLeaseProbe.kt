package simplefilesystem.durable.testing

/**
 * Child-process probe used by the cross-process fixture test.
 *
 * The process holds one managed-node lease until its caller creates the requested release file.
 * Callers can use its start gate to release independently forked JVMs together and keep their
 * leases overlapping without exposing fixture internals as production API.
 */
object SharedCockroachLeaseProbe
