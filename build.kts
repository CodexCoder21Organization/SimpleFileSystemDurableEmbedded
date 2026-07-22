@KotlinBuildScript("https://tools.kotlin.build/")
@file:WithArtifact("kompile:build-kotlin-jvm:0.0.23")
package simplefilesystem.durable

import build.kotlin.annotations.MavenArtifactCoordinates
import build.kotlin.jvm.MavenPrebuilt2
import build.kotlin.jvm.buildSimpleKotlinMavenArtifact2
import build.kotlin.withartifact.WithArtifact
import java.io.File

val dependencies = listOf(
    MavenPrebuilt2("simplefilesystem:simplefilesystem-api:0.1.0"),
    MavenPrebuilt2("blobstore.api:blobstore-api:0.0.2"),
    MavenPrebuilt2("sql:sql-api:0.0.1"),
    MavenPrebuilt2("community.kotlin.clocks.simple:community-kotlin-clocks-simple:0.0.7"),
    MavenPrebuilt2("com.squareup.okio:okio-jvm:3.4.0"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-common:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-jdk7:1.9.22"),
    MavenPrebuilt2("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.9.22"),
)

@MavenArtifactCoordinates("simplefilesystem.durable:simplefilesystem-durable-embedded:")
fun buildMaven(): File = buildSimpleKotlinMavenArtifact2(
    coordinates = "simplefilesystem.durable:simplefilesystem-durable-embedded:0.1.0",
    src = File("src"),
    compileDependencies = dependencies,
)

fun buildSkinnyJar(): File = buildMaven()
