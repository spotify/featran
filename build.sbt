/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.typesafe.sbt.SbtSite.SiteKeys._
import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo

val scala211 = "2.11.11"
val scala212 = "2.12.3"

val algebirdVersion = "0.13.0"
val breezeVersion = "0.13.1"
val circeVersion = "0.8.0"
val commonsMathVersion = "3.6.1"
val flinkVersion = "1.2.0"
val hadoopVersion = "2.8.0"
val scalacheckVersion = "1.13.5"
val scalatestVersion = "3.0.1"
val scaldingVersion = "0.17.0"
val scioVersion = "0.4.0"
val sparkVersion = "2.1.1"
val tensorflowVersion = "1.1.0"

val commonSettings = Seq(
  organization := "com.spotify",
  name := "featran",
  description := "Feature Transformers",
  scalaVersion := scala211,
  crossScalaVersions := Seq(scala211),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile, doc) ++= Seq("-skip-packages", "org.apache"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc)  := Seq("-source", "1.8"),

  // Release settings
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  sonatypeProfileName           := "com.spotify",
  pomExtra                      := {
    <url>https://github.com/spotify/featran</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>git@github.com/spotify/featran.git</url>
      <connection>scm:git:git@github.com:spotify/featran.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sinisa_lyh</id>
        <name>Neville Li</name>
        <url>https://twitter.com/sinisa_lyh</url>
      </developer>
      <developer>
        <id>rwhitcomb</id>
        <name>Richard Whitcomb</name>
        <url>https://twitter.com/rwhitcomb</url>
      </developer>
    </developers>
  }
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "root",
  file(".")
).enablePlugins(GhpagesPlugin, ScalaUnidocPlugin).settings(
  commonSettings ++ noPublishSettings,
  siteSubdirName in ScalaUnidoc := "",
  addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
  gitRemoteRepo := "git@github.com:spotify/featran.git",
  // com.spotify.featran.java pollutes namespaces and breaks unidoc class path
  unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(java)
).aggregate(
  core,
  java,
  flink,
  scalding,
  scio,
  spark,
  numpy,
  tensorflow
)

lazy val core: Project = Project(
  "core",
  file("core")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-core",
  description := "Feature Transformers",
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "org.apache.commons" % "commons-math3" % commonsMathVersion,
    "org.scalanlp" %% "breeze" % breezeVersion,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
  ),
  libraryDependencies ++= Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % circeVersion)
)

lazy val java: Project = Project(
  "java",
  file("java")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-java",
  description := "Feature Transformers - java",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  core,
  core % "test->test"
)

lazy val flink: Project = Project(
  "flink",
  file("flink")
).settings(
  commonSettings,
  moduleName := "featran-flink",
  description := "Feature Transformers - Flink",
  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
    "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  core,
  core % "test->test"
)

lazy val scalding: Project = Project(
  "scalding",
  file("scalding")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-scalding",
  description := "Feature Transformers - Scalding",
  resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
  libraryDependencies ++= Seq(
    "com.twitter" %% "scalding-core" % scaldingVersion % "provided",
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  core,
  core % "test->test"
)

lazy val scio: Project = Project(
  "scio",
  file("scio")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-scio",
  description := "Feature Transformers - Scio",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % "test"
  )
).dependsOn(
  core,
  core % "test->test"
)

lazy val spark: Project = Project(
  "spark",
  file("spark")
).settings(
  commonSettings,
  moduleName := "featran-spark",
  description := "Feature Transformers - Spark",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(
  core,
  core % "test->test"
)

lazy val numpy: Project = Project(
  "numpy",
  file("numpy")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-numpy",
  description := "Feature Transformers - NumPy",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )
).dependsOn(core)

lazy val tensorflow: Project = Project(
  "tensorflow",
  file("tensorflow")
).settings(
  commonSettings,
  crossScalaVersions := Seq(scala211, scala212),
  moduleName := "featran-tensorflow",
  description := "Feature Transformers - TensorFlow",
  libraryDependencies ++= Seq(
    "org.tensorflow" % "proto" % tensorflowVersion,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
  )
).dependsOn(core)
