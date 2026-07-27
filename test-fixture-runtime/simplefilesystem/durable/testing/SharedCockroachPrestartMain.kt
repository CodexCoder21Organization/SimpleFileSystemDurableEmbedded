package simplefilesystem.durable.testing

import java.io.File
import java.time.Instant

fun main(args: Array<String>) {
    require(args.size >= 2) {
        "SharedCockroachPrestartMain requires <session-owner-pid> " +
            "<session-owner-started-at-millis> [fixture-build-rule-cache-entry...], but received " +
            "${args.size} argument(s)."
    }
    val ownerPid = args[0].toLongOrNull() ?: throw IllegalArgumentException(
        "Shared CockroachDB session-owner PID must be numeric, but was '${args[0]}'.",
    )
    val ownerStartedAtMillis = args[1].toLongOrNull() ?: throw IllegalArgumentException(
        "Shared CockroachDB session-owner start time must be numeric, but was '${args[1]}'.",
    )
    val owner = SharedProcessIdentity(ownerPid, Instant.ofEpochMilli(ownerStartedAtMillis))
    check(owner.liveHandle() != null) {
        "Cannot prestart shared CockroachDB for Kompile session PID $ownerPid started at " +
            "${owner.startedAt} because that process is not alive."
    }
    prestartSharedCockroachForSession(owner, args.drop(2).map(::File))
}
