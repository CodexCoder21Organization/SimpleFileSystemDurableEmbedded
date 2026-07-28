package simplefilesystem.durable.testing

import java.io.File
import java.time.Instant

fun main(args: Array<String>) {
    require(args.size >= 4) {
        "SharedCockroachPrestartMain requires <session-owner-pid> " +
            "<session-owner-started-at-millis> <invalidate-cache-while-owner-live> " +
            "<required-schema-ready-databases> [fixture-build-rule-cache-entry...], but received " +
            "${args.size} argument(s)."
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
    val requiredSchemaReadyDatabases = args[3].toIntOrNull()
        ?.takeIf { it in 0..FIXTURE_DATABASE_POOL_SIZE }
        ?: throw IllegalArgumentException(
            "Shared CockroachDB required schema-ready database count must be between 0 and " +
                "$FIXTURE_DATABASE_POOL_SIZE, but was '${args[3]}'.",
        )
    val owner = SharedProcessIdentity(ownerPid, Instant.ofEpochMilli(ownerStartedAtMillis))
    val adminJdbcUrl = prestartSharedCockroachForSession(
        owner,
        args.drop(4).map(::File),
        invalidateCacheWhileOwnerLive,
        initialFixtureDatabasePoolSize = requiredSchemaReadyDatabases.coerceAtMost(
            FIXTURE_DATABASE_READY_POOL_SIZE,
        ),
    )
    if (requiredSchemaReadyDatabases > FIXTURE_DATABASE_READY_POOL_SIZE) {
        warmUpDurableSchema(
            adminJdbcUrl = adminJdbcUrl,
            targetPoolSize = requiredSchemaReadyDatabases,
        )
    }
}
