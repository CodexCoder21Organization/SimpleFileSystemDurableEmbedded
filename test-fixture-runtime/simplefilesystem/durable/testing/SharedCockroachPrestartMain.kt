package simplefilesystem.durable.testing

import java.io.File
import java.time.Instant

fun main(args: Array<String>) {
    require(args.size >= 3) {
        "SharedCockroachPrestartMain requires <session-owner-pid> " +
            "<session-owner-started-at-millis> <invalidate-cache-while-owner-live> " +
            "[fixture-build-rule-cache-entry...], but received ${args.size} argument(s)."
    }
    val ownerPid = args[0].toLongOrNull() ?: throw IllegalArgumentException(
        "Shared CockroachDB session-owner PID must be numeric, but was '${args[0]}'.",
    )
    val ownerStartedAtMillis = args[1].toLongOrNull() ?: throw IllegalArgumentException(
        "Shared CockroachDB session-owner start time must be numeric, but was '${args[1]}'.",
    )
    val invalidateCacheWhileOwnerLive = args[2].toBooleanStrictOrNull()
        ?: throw IllegalArgumentException(
            "Shared CockroachDB live-owner cache invalidation flag must be 'true' or 'false', " +
                "but was '${args[2]}'.",
        )
    val owner = SharedProcessIdentity(ownerPid, Instant.ofEpochMilli(ownerStartedAtMillis))
    check(owner.liveHandle() != null) {
        "Cannot prestart shared CockroachDB for Kompile session PID $ownerPid started at " +
            "${owner.startedAt} because that process is not alive."
    }
    prestartSharedCockroachForSession(
        owner,
        args.drop(3).map(::File),
        invalidateCacheWhileOwnerLive,
    )
}
