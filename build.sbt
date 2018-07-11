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

import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo
import sbt.Def

val algebirdVersion = "0.13.4"
val breezeVersion = "1.0-RC2"
val circeVersion = "0.9.1"
val commonsMathVersion = "3.6.1"
val flinkVersion = "1.4.2"
val hadoopVersion = "2.8.0"
val paradiseVersion = "2.1.1"
val scalacheckVersion = "1.13.5"
val scalatestVersion = "3.0.5"
val scaldingVersion = "0.17.4"
val scioVersion = "0.5.5"
val simulacrumVersion = "0.12.0"
val sparkVersion = "2.3.0"
val tensorflowVersion = "1.8.0"
val xgBoostVersion = "0.72-20180627-1214081f"
val shapelessDatatypeVersion = "0.1.9"

val CompileTime = config("compile-time").hide

val commonSettings = Seq(
  organization := "com.spotify",
  name := "featran",
  description := "Feature Transformers",
  scalacOptions ++= commonScalacOptions,
  scalacOptions in (Compile, doc) ++= Seq("-skip-packages", "org.apache"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc) := Seq("-source", "1.8"),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
  libraryDependencies ++= Seq(
    "com.github.mpilquist" %% "simulacrum" % simulacrumVersion % CompileTime,
    compilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full)
  ),
  ivyConfigurations += CompileTime,
  unmanagedClasspath in Compile ++= update.value.select(configurationFilter(CompileTime.name))
)

val publishSettings = Seq(
  credentials ++= (for {
    username <- sys.env.get("SONATYPE_USERNAME")
    password <- sys.env.get("SONATYPE_PASSWORD")
  } yield
    Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq,
  publishTo := Some(
    if (isSnapshot.value) Opts.resolver.sonatypeSnapshots
    else Opts.resolver.sonatypeStaging),
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle := true,
  publishArtifact in Test := false,
  sonatypeProfileName := "com.spotify",
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/featran")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/spotify/featran.git"),
            "scm:git:git@github.com:spotify/featran.git")),
  developers := List(
    Developer(id = "sinisa_lyh",
              name = "Neville Li",
              email = "neville.lyh@gmail.com",
              url = url("https://twitter.com/sinisa_lyh")),
    Developer(id = "rwhitcomb",
              name = "Richard Whitcomb",
              email = "richwhitjr@gmail.com",
              url = url("https://twitter.com/rwhitcomb")),
    Developer(id = "ravwojdyla",
              name = "Rafal Wojdyla",
              email = "ravwojdyla@gmail.com",
              url = url("https://twitter.com/ravwojdyla")),
    Developer(id = "fallonfofallon",
              name = "Fallon Chen",
              email = "fallon@spotify.com",
              url = url("https://twitter.com/fallonfofallon")),
    Developer(id = "andrew_martin92",
              name = "Andrew Martin",
              email = "andrewsmartin.mg@gmail.com",
              url = url("https://twitter.com/andrew_martin92")),
    Developer(id = "regadas",
              name = "Filipe Regadas",
              email = "filiperegadas@gmail.com",
              url = url("https://twitter.com/regadas")),
    Developer(id = "slhansen",
              name = "Samantha Hansen",
              email = "slhansen@spotify.com",
              url = url("https://github.com/slhansen"))
  )
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = project
  .in(file("."))
  .enablePlugins(GhpagesPlugin, ScalaUnidocPlugin)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    scalaVersion := "2.11.12",
    siteSubdirName in ScalaUnidoc := "api",
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
    gitRemoteRepo := "git@github.com:spotify/featran.git",
    // com.spotify.featran.java pollutes namespaces and breaks unidoc class path
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(java) -- inProjects(
      examples),
    mappings in makeSite ++= Seq(
      file("site/index.html") -> "index.html",
      file("examples/target/site/Examples.scala.html") -> "examples/Examples.scala.html"
    )
  )
  .aggregate(
    core,
    java,
    flink,
    scalding,
    scio,
    spark,
    numpy,
    tensorflow,
    xgboost
  )

lazy val core: Project = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-core"))
  .settings(
    name := "core",
    moduleName := "featran-core",
    description := "Feature Transformers",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.apache.commons" % "commons-math3" % commonsMathVersion % "test"
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion)
  )

lazy val java: Project = project
  .in(file("java"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-java"))
  .settings(
    name := "java",
    moduleName := "featran-java",
    description := "Feature Transformers - java",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "me.lyh" % "xgboost4j" % xgBoostVersion % "provided"
    )
  )
  .dependsOn(
    core,
    core % "test->test",
    tensorflow,
    xgboost
  )

lazy val flink: Project = project
  .in(file("flink"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-flink"))
  .settings(
    name := "flink",
    moduleName := "featran-flink",
    description := "Feature Transformers - Flink",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12"),
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val scalding: Project = project
  .in(file("scalding"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-scalding"))
  .settings(
    name := "scalding",
    moduleName := "featran-scalding",
    description := "Feature Transformers - Scalding",
    resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    libraryDependencies ++= Seq(
      "com.twitter" %% "scalding-core" % scaldingVersion % "provided",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val scio: Project = project
  .in(file("scio"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-scio"))
  .settings(
    name := "scio",
    moduleName := "featran-scio",
    description := "Feature Transformers - Scio",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion % "provided",
      "com.spotify" %% "scio-test" % scioVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val spark: Project = project
  .in(file("spark"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-spark"))
  .settings(
    name := "spark",
    moduleName := "featran-spark",
    description := "Feature Transformers - Spark",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val numpy: Project = project
  .in(file("numpy"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-numpy"))
  .settings(
    name := "numpy",
    moduleName := "featran-numpy",
    description := "Feature Transformers - NumPy",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(core)

lazy val tensorflow: Project = project
  .in(file("tensorflow"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-tensorflow"))
  .settings(
    name := "tensorflow",
    moduleName := "featran-tensorflow",
    description := "Feature Transformers - TensorFlow",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    libraryDependencies ++= Seq(
      "org.tensorflow" % "proto" % tensorflowVersion,
      "me.lyh" %% "shapeless-datatype-tensorflow" % shapelessDatatypeVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val xgboost: Project = project
  .in(file("xgboost"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(mimaSettings("featran-xgboost"))
  .settings(
    name := "xgboost",
    moduleName := "featran-xgboost",
    description := "Feature Transformers - XGBoost",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12"),
    libraryDependencies ++= Seq(
      "me.lyh" % "xgboost4j" % xgBoostVersion % "provided",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val examples: Project = project
  .in(file("examples"))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(soccoSettings)
  .settings(
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    name := "examples",
    moduleName := "featran-examples",
    description := "Feature Transformers - examples",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion
    )
  )
  .dependsOn(core, scio, tensorflow)

lazy val featranJmh: Project = project
  .in(file("jmh"))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.6"),
    name := "jmh",
    description := "Featran JMH Microbenchmarks",
    sourceDirectory in Jmh := (sourceDirectory in Test).value,
    classDirectory in Jmh := (classDirectory in Test).value,
    dependencyClasspath in Jmh := (dependencyClasspath in Test).value
  )
  .dependsOn(
    core,
    tensorflow
  )
  .enablePlugins(JmhPlugin)

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Xfuture"
)

lazy val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:examples/target/site",
      "-P:socco:package_com.spotify.featran:http://spotify.github.io/featran/api",
      "-P:socco:package_com.spotify.scio:http://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.9")
  )
} else {
  Nil
}

// based on the nice https://github.com/typelevel/cats/blob/master/build.sbt#L208
def mimaSettings(moduleName: String): Seq[Def.Setting[Set[sbt.ModuleID]]] = {
  import sbtrelease.Version
  // Safety Net for Exclusions
  lazy val excludedVersions: Set[String] = Set()
  // Safety Net for Inclusions
  lazy val extraVersions: Set[String] = Set()

  def semverBinCompatVersions(major: Int, minor: Int, patch: Int): Set[(Int, Int, Int)] = {
    val majorVersions: List[Int] = List(major)
    val minorVersions: List[Int] =
      if (major >= 1) {
        Range(0, minor).inclusive.toList
      } else {
        List(minor)
      }

    def patchVersions(currentMinVersion: Int): List[Int] =
      if (minor == 0 && patch == 0) {
        List.empty[Int]
      } else {
        if (currentMinVersion != minor) {
          List(0)
        } else {
          Range(0, patch - 1).inclusive.toList
        }
      }

    val versions = for {
      maj <- majorVersions
      min <- minorVersions
      pat <- patchVersions(min)
    } yield (maj, min, pat)
    versions.toSet
  }

  def mimaVersions(version: String): Set[String] =
    Version(version) match {
      case Some(Version(major, Seq(minor, patch), _)) =>
        semverBinCompatVersions(major.toInt, minor.toInt, patch.toInt)
          .map { case (maj, min, pat) => s"${maj}.${min}.${pat}" }
      case _ =>
        Set.empty[String]
    }

  Seq(
    mimaPreviousArtifacts := (mimaVersions(version.value) ++ extraVersions)
      .diff(excludedVersions)
      .map(v => "com.spotify" %% moduleName % v))
}
