@KotlinBuildScript("https://tools.kotlin.build/")
@file:WithArtifact("kompile:build-kotlin-jvm:0.0.23")
package simplefilesystem.durable

import build.kotlin.annotations.MavenArtifactCoordinates
import build.kotlin.jvm.BuildJar
import build.kotlin.jvm.Manifest
import build.kotlin.jvm.MavenPrebuilt2
import build.kotlin.jvm.buildSimpleKotlinMavenArtifact2
import build.kotlin.jvm.jar
import build.kotlin.jvm.resolveDependencies2
import build.kotlin.withartifact.WithArtifact
import java.io.File
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

val dependencies = listOf(
    MavenPrebuilt2("simplefilesystem:simplefilesystem-api:0.3.0"),
    MavenPrebuilt2("blobstore.api:blobstore-api:0.0.2"),
    MavenPrebuilt2("sql:sql-api:0.0.1"),
    MavenPrebuilt2("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.3"),
    MavenPrebuilt2("com.squareup.okio:okio-jvm:3.4.0"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-common:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-jdk7:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.9.22"),
)

@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-embedded:")
fun buildMaven(): File = buildSimpleKotlinMavenArtifact2(
    coordinates = "simplefilesystem.durable:simplefilesystem-durable-embedded:0.1.2",
    src = File("src"),
    compileDependencies = dependencies,
)

fun buildSkinnyJar(): File = buildMaven()

val testSupportDependencies = listOf(
    MavenPrebuilt2("blobstore.api:blobstore-api:0.0.2"),
    MavenPrebuilt2("community.kotlin.blobstore.inmemory:blobstore-in-memory:0.0.3"),
    MavenPrebuilt2("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.7"),
    MavenPrebuilt2("cockroachdb.testharness:cockroachdb-test-harness:0.0.4"),
    MavenPrebuilt2("org.apache.commons:commons-dbcp2:2.9.0"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib:1.9.22"),
    MavenPrebuilt2("org.postgresql:postgresql:42.6.0"),
    MavenPrebuilt2("sql:sql-api:0.0.1"),
    MavenPrebuilt2("sql:sql:0.0.2"),
)

@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-test-support:")
fun buildTestSupportMaven(): File = buildSimpleKotlinMavenArtifact2(
    coordinates = "simplefilesystem.durable:simplefilesystem-durable-test-support:0.1.2",
    src = File("test-support"),
    compileDependencies = testSupportDependencies,
)

val fixtureRuntimeDependencies = dependencies + testSupportDependencies

@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-test-fixture-runtime:")
fun buildCockroachTestFixtureRuntime(): File = buildSimpleKotlinMavenArtifact2(
    coordinates = "simplefilesystem.durable:simplefilesystem-durable-test-fixture-runtime:0.1.2",
    // This toolchain cannot put sibling build-rule outputs on a *2 builder's classpath, so the
    // fixture jar compiles the repository-owned production and fixture source trees together.
    src = File("."),
    compileDependencies = fixtureRuntimeDependencies,
)

private fun assembleCockroachTestFixtureFatJar(): File = BuildJar(
    Manifest("simplefilesystem.durable.testing.CockroachSuiteFixtureMainKt"),
    resolveDependencies2(fixtureRuntimeDependencies).map { it.jar } +
        buildCockroachTestFixtureRuntime().jar,
)

/**
 * Builds the fixture runtime and starts its single owner while Kompile is resolving test
 * dependencies. The owner holds one workspace lock for the complete runner session, so every test
 * JVM sees a ready node without participating in an election or lifecycle protocol.
 *
 * Tests resolve this rule through its workspace-local Maven coordinate instead of a direct
 * `package.rule()` dependency. BuildTest deliberately prepares and distributes only direct rule
 * dependencies; a process-owning prestart must execute independently in each shard's live runner
 * session and must never be restored from another host's packaged build cache.
 */
@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-test-fixture:")
fun buildCockroachTestFixtureFatJar(): File {
    val fixtureJar = assembleCockroachTestFixtureFatJar()
    if (System.getenv("SIMPLE_FILESYSTEM_DURABLE_TEST_COCKROACH_JDBC_URL").isNullOrBlank()) {
        prestartCockroachForKompileSession(fixtureJar)
    }
    return fixtureJar
}

/**
 * Builds the suite-owned fixture without also starting the direct-dispatch managed fixture.
 * scripts/test.bash owns this process explicitly and exports its JDBC URL to every test.
 */
fun buildCockroachSuiteFixtureFatJar(): File = assembleCockroachTestFixtureFatJar()

private fun processCommandArguments(process: ProcessHandle): List<String> {
    val procCommandLine = File("/proc/${process.pid()}/cmdline")
    if (procCommandLine.isFile) {
        return procCommandLine.readBytes()
            .toString(StandardCharsets.UTF_8)
            .split('\u0000')
            .filter(String::isNotEmpty)
    }
    return process.info().arguments().orElse(emptyArray()).toList()
}

private fun currentKompileSessionOwner(): ProcessHandle {
    val ancestors = generateSequence(
        ProcessHandle.current().parent().orElse(null),
    ) { process ->
        process.parent().orElse(null)
    }.toList()
    return ancestors.firstOrNull { process ->
        val arguments = processCommandArguments(process)
        arguments.any {
            it == "kompile.cli.CliKt" ||
                it == "buildtest.runner.MainKt" ||
                it.contains("/kompile/cli/kompile-cli/")
        }
    } ?: throw IllegalStateException(
        "Cannot prestart the shared CockroachDB fixture because the build-rule process " +
            "${ProcessHandle.current().pid()} has no live Kompile CLI or BuildTestRunner " +
            "ancestor. Ancestors were " +
            ancestors.map { process ->
                "${process.pid()}:${processCommandArguments(process).joinToString(" ")}"
            },
    )
}

private fun fixtureBuildRuleCacheEntries(sessionOwner: ProcessHandle): List<File> {
    val arguments = processCommandArguments(sessionOwner)
    val cacheArgument = arguments.indices.firstNotNullOfOrNull { index ->
        when {
            arguments[index] == "--cache-location" ||
                arguments[index] == "--cache" ||
                arguments[index] == "-c" ->
                arguments.getOrNull(index + 1)
            arguments[index].startsWith("--cache-location=") ->
                arguments[index].substringAfter('=')
            arguments[index].startsWith("--cache=") ->
                arguments[index].substringAfter('=')
            else -> null
        }
    }
    val cacheDirectory = if (cacheArgument == null) {
        File(System.getProperty("user.home"), ".buildcache")
    } else {
        val configured = File(cacheArgument)
        if (configured.isAbsolute) {
            configured
        } else {
            val sessionWorkingDirectory = File("/proc/${sessionOwner.pid()}/cwd")
                .takeIf(File::exists)
                ?.canonicalFile
                ?: File(".").canonicalFile
            File(sessionWorkingDirectory, cacheArgument)
        }
    }
    val invocation = "simplefilesystem.durable.buildCockroachTestFixtureFatJar()"
    val key = MessageDigest.getInstance("SHA-256")
        .digest(invocation.toByteArray(StandardCharsets.UTF_8))
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
    return listOf(File(cacheDirectory.canonicalFile, "buildRuleResultIndex/$key.json"))
}

private fun prestartCockroachForKompileSession(fixtureJar: File) {
    val workspace = File(".").canonicalFile
    val sessionOwner = currentKompileSessionOwner()
    val cacheEntries = if (processCommandArguments(sessionOwner).any { it == "--build-only" }) {
        fixtureBuildRuleCacheEntries(sessionOwner)
    } else {
        emptyList()
    }
    val sessionStartedAt = requireNotNull(sessionOwner.info().startInstant().orElse(null)) {
        "The Kompile CLI session process ${sessionOwner.pid()} did not expose its start time."
    }
    val javaBinary = File(System.getProperty("java.home"), "bin/java")
    val process = ProcessBuilder(
        javaBinary.absolutePath,
        "-XX:+UseSerialGC",
        "-XX:ActiveProcessorCount=1",
        "-XX:TieredStopAtLevel=1",
        "-cp",
        fixtureJar.absolutePath,
        "simplefilesystem.durable.testing.SharedCockroachPrestartMainKt",
        sessionOwner.pid().toString(),
        sessionStartedAt.toEpochMilli().toString(),
        *cacheEntries.map(File::getAbsolutePath).toTypedArray(),
    )
        .directory(workspace)
        .inheritIO()
        .start()
    if (!process.waitFor(120L, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        process.waitFor()
        throw IllegalStateException(
            "Shared CockroachDB prestart process ${process.pid()} did not finish within 120 seconds.",
        )
    }
    check(process.exitValue() == 0) {
        "Shared CockroachDB prestart process ${process.pid()} exited with code " +
            "${process.exitValue()}."
    }
}
