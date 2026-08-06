package simplefilesystem.durable.testing

import java.io.File
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    require(args.size >= 2) {
        "SharedCockroachPrestartMain requires <session-owner-pid> " +
            "<session-owner-started-at-millis> [build-rule-cache-entry...], but received " +
            "${args.size} argument(s)."
    }
    val ownerPid = args[0].toLongOrNull() ?: throw IllegalArgumentException(
        "The shared CockroachDB session-owner PID must be numeric, but was '${args[0]}'.",
    )
    val ownerStartedAtMillis = args[1].toLongOrNull() ?: throw IllegalArgumentException(
        "The shared CockroachDB session-owner start time must be numeric, but was '${args[1]}'.",
    )
    ensureSharedCockroachFixtureForSession(
        SharedProcessIdentity(ownerPid, ownerStartedAtMillis),
        args.drop(2).map(::File),
    )
}

internal fun ensureSharedCockroachFixtureForSession(
    sessionOwner: SharedProcessIdentity,
    cacheEntries: List<File>,
): SharedCockroachFixtureRecord {
    check(sessionOwner.liveHandle() != null) {
        "Cannot prestart the shared CockroachDB fixture because session-owner process " +
            "${sessionOwner.pid} with start time ${sessionOwner.startedAtMillis} is not live."
    }
    liveFixtureForSession(sessionOwner)?.let { return it }
    val javaBinary = File(System.getProperty("java.home"), "bin/java")
    val candidate = ProcessBuilder(
        javaBinary.absolutePath,
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=1",
        "-XX:TieredStopAtLevel=1",
        "-cp",
        System.getProperty("java.class.path"),
        "simplefilesystem.durable.testing.CockroachSuiteFixtureMainKt",
        "-",
        sessionOwner.pid.toString(),
        sessionOwner.startedAtMillis.toString(),
        *cacheEntries.map(File::getAbsolutePath).toTypedArray(),
    )
        .directory(File(".").canonicalFile)
        .inheritIO()
        .start()
    val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(120L)
    try {
        while (true) {
            liveFixtureForSession(sessionOwner)?.let { record ->
                if (record.fixtureOwner.pid != candidate.pid() && candidate.isAlive) {
                    candidate.destroyForcibly()
                    candidate.waitFor(5L, TimeUnit.SECONDS)
                }
                return record
            }
            if (!candidate.isAlive) {
                throw IllegalStateException(
                    "Shared CockroachDB fixture owner candidate ${candidate.pid()} exited with " +
                        "code ${candidate.exitValue()} before publishing " +
                        "${sharedCockroachReadyFile().absolutePath}.",
                )
            }
            check(System.nanoTime() < deadlineNanos) {
                "Shared CockroachDB fixture owner candidate ${candidate.pid()} did not publish " +
                    "${sharedCockroachReadyFile().absolutePath} within 120 seconds."
            }
            Thread.sleep(25L)
        }
    } catch (failure: Throwable) {
        if (candidate.isAlive) {
            candidate.destroyForcibly()
            candidate.waitFor(5L, TimeUnit.SECONDS)
        }
        throw failure
    }
}

private fun liveFixtureForSession(
    sessionOwner: SharedProcessIdentity,
): SharedCockroachFixtureRecord? = runCatching(::readSharedCockroachFixture)
    .getOrNull()
    ?.takeIf { record ->
        record.sessionOwner == sessionOwner &&
            record.fixtureOwner.liveHandle() != null &&
            record.cockroach.liveHandle() != null
    }
