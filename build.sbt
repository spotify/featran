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

val algebirdVersion = "0.13.4"
val breezeVersion = "1.0-RC2"
val circeVersion = "0.9.1"
val commonsMathVersion = "3.6.1"
val flinkVersion = "1.4.2"
val hadoopVersion = "2.8.0"
val scalacheckVersion = "1.13.5"
val scalatestVersion = "3.0.5"
val scaldingVersion = "0.17.4"
val scioVersion = "0.5.4"
val sparkVersion = "2.3.0"
val tensorflowVersion = "1.8.0"
val xgBoostVersion = "0.71-20180420-230cb9b7"

val commonSettings = Seq(
  organization := "com.spotify",
  name := "featran",
  description := "Feature Transformers",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.11.12", "2.12.6"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked"),
  scalacOptions in (Compile, doc) ++= Seq("-skip-packages", "org.apache"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  javacOptions in (Compile, doc) := Seq("-source", "1.8"),
  // Release settings
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
              url = url("https://twitter.com/andrew_martin92"))
  )
)

val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val root: Project = Project(
  "root",
  file(".")
).enablePlugins(GhpagesPlugin, ScalaUnidocPlugin)
  .settings(
    commonSettings ++ noPublishSettings,
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

lazy val core: Project = Project(
  "core",
  file("core")
).settings(
  commonSettings,
  moduleName := "featran-core",
  description := "Feature Transformers",
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

lazy val java: Project = Project(
  "java",
  file("java")
).settings(
    commonSettings,
    moduleName := "featran-java",
    description := "Feature Transformers - java",
    skip in Compile := scalaBinaryVersion.value == "2.12",
    skip in Test := scalaBinaryVersion.value == "2.12",
    skip in publish := scalaBinaryVersion.value == "2.12",
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.12") Nil
      else
        Seq(
          "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
          "org.scalatest" %% "scalatest" % scalatestVersion % "test",
          "me.lyh" % "xgboost4j" % xgBoostVersion % "provided"
        )
    },
    // workaround for `dependsOn(core % "test->test")` pulling in scalacheck & scalatest dependencies
    projectDependencies := {
      if (scalaBinaryVersion.value == "2.12") Nil
      else
        Seq(
          (projectID in core).value,
          (projectID in tensorflow).value,
          (projectID in xgboost).value
        )
    }
  )
  .dependsOn(
    core,
    core % "test->test",
    tensorflow,
    xgboost
  )

lazy val flink: Project = Project(
  "flink",
  file("flink")
).settings(
    commonSettings,
    moduleName := "featran-flink",
    description := "Feature Transformers - Flink",
    skip in Compile := scalaBinaryVersion.value == "2.12",
    skip in Test := scalaBinaryVersion.value == "2.12",
    skip in publish := scalaBinaryVersion.value == "2.12",
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.12") Nil
      else
        Seq(
          "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
          "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
          "org.scalatest" %% "scalatest" % scalatestVersion % "test"
        )
    }
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val scalding: Project = Project(
  "scalding",
  file("scalding")
).settings(
    commonSettings,
    moduleName := "featran-scalding",
    description := "Feature Transformers - Scalding",
    resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo",
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

lazy val scio: Project = Project(
  "scio",
  file("scio")
).settings(
    commonSettings,
    moduleName := "featran-scio",
    description := "Feature Transformers - Scio",
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion % "provided",
      "com.spotify" %% "scio-test" % scioVersion % "test"
    )
  )
  .dependsOn(
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
    skip in Compile := scalaBinaryVersion.value == "2.12",
    skip in Test := scalaBinaryVersion.value == "2.12",
    skip in publish := scalaBinaryVersion.value == "2.12",
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.12") Nil
      else
        Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
          "org.scalatest" %% "scalatest" % scalatestVersion % "test"
        )
    }
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val numpy: Project = Project(
  "numpy",
  file("numpy")
).settings(
    commonSettings,
    moduleName := "featran-numpy",
    description := "Feature Transformers - NumPy",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
  .dependsOn(core)

lazy val tensorflow: Project = Project(
  "tensorflow",
  file("tensorflow")
).settings(
    commonSettings,
    moduleName := "featran-tensorflow",
    description := "Feature Transformers - TensorFlow",
    libraryDependencies ++= Seq(
      "org.tensorflow" % "proto" % tensorflowVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    )
  )
  .dependsOn(
    core,
    core % "test->test"
  )

lazy val xgboost: Project = Project(
  "xgboost",
  file("xgboost")
).settings(
    commonSettings,
    moduleName := "featran-xgboost",
    description := "Feature Transformers - XGBoost",
    skip in Compile := scalaBinaryVersion.value == "2.12",
    skip in Test := scalaBinaryVersion.value == "2.12",
    skip in publish := scalaBinaryVersion.value == "2.12",
    libraryDependencies ++= {
      if (scalaBinaryVersion.value == "2.12") Nil
      else
        Seq(
          "me.lyh" % "xgboost4j" % xgBoostVersion % "provided",
          "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
        )
    }
  )
  .dependsOn(
    core,
    core % "test->test"
  )

val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:examples/target/site",
      "-P:socco:package_com.spotify.featran:http://spotify.github.io/featran/api",
      "-P:socco:package_com.spotify.scio:http://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.9"),
  )
} else {
  Nil
}

lazy val examples: Project = Project(
  "examples",
  file("examples")
).settings(
    commonSettings ++ noPublishSettings ++ soccoSettings,
    moduleName := "featran-examples",
    description := "Feature Transformers - examples",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion
    )
  )
  .dependsOn(core, scio, tensorflow)

lazy val featranJmh: Project = Project(
  "jmh",
  file("jmh")
).settings(
    commonSettings ++ noPublishSettings,
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
