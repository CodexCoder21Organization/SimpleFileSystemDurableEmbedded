@KotlinBuildScript("https://tools.kotlin.build/")
@file:WithArtifact("kompile:build-kotlin-jvm:0.0.23")
package simplefilesystem.durable

import build.kotlin.annotations.MavenArtifactCoordinates
import build.kotlin.jvm.BuildJar
import build.kotlin.jvm.BuildKotlin
import build.kotlin.jvm.Manifest
import build.kotlin.jvm.MavenPrebuilt2
import build.kotlin.jvm.buildSimpleKotlinMavenArtifact2
import build.kotlin.jvm.jar
import build.kotlin.jvm.resolveDependencies2
import build.kotlin.withartifact.WithArtifact
import java.io.File

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
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib:1.9.22"),
)

@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-test-support:")
fun buildTestSupportMaven(): File = buildSimpleKotlinMavenArtifact2(
    coordinates = "simplefilesystem.durable:simplefilesystem-durable-test-support:0.1.2",
    src = File("test-support"),
    compileDependencies = testSupportDependencies,
)

val fixtureRuntimeDependencies = dependencies + testSupportDependencies + listOf(
    MavenPrebuilt2("sql:sql:0.0.2"),
)

val fixtureRuntimeSources = listOf(File("test-support"), File("test-fixture-runtime"))
    .flatMap { sourceDirectory ->
        sourceDirectory.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .toList()
    }
    .sortedBy(File::getPath)

fun buildCockroachTestFixtureRuntime(): File = BuildKotlin(
    src = fixtureRuntimeSources,
    classpath = resolveDependencies2(fixtureRuntimeDependencies) +
        buildMaven(),
    buildAsJar = true,
)

fun buildCockroachTestFixtureFatJar(): File = BuildJar(
    Manifest("simplefilesystem.durable.testing.CockroachSuiteFixtureMainKt"),
    resolveDependencies2(fixtureRuntimeDependencies).map { it.jar } +
        listOf(
            buildMaven().jar,
            buildCockroachTestFixtureRuntime(),
        ),
)

/**
 * Gives protocol-scenario tests their own declared build-rule dependency so additions to the
 * scenario runtime cannot be hidden by a previously resolved fixture annotation.
 */
fun buildCockroachProtocolV2WarmupAttestedScenarioFatJar(): File =
    buildCockroachTestFixtureFatJar()
