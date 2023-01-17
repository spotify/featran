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

import laika.ast.Styles
import laika.helium.config._
import laika.helium.Helium
import laika.io.model.InputTree
import laika.markdown.github.GitHubFlavor
import laika.parse.code.SyntaxHighlighting

val algebirdVersion = "0.13.9"
val breezeVersion = "2.1.0"
val circeVersion = "0.14.3"
val commonsMathVersion = "3.6.1"
val flinkVersion = "1.14.4"
val hadoopVersion = "3.3.4"
val paradiseVersion = "2.1.1"
val scalacheckVersion = "1.17.0"
val scalatestVersion = "3.2.15"
val scaldingVersion = "0.17.4"
val scioVersion = "0.11.14"
val simulacrumVersion = "1.0.1"
val sparkVersion = "3.3.1"
val tensorflowVersion = "0.4.2"
val xgBoostVersion = "1.3.1"

// project
ThisBuild / tlBaseVersion := "0.8"
ThisBuild / organization := "com.spotify"
ThisBuild / organizationName := "Spotify AB"
ThisBuild / startYear := Some(2016)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / homepage := Some(url("https://github.com/spotify/featran"))
ThisBuild / developers := List(
  Developer(
    id = "sinisa_lyh",
    name = "Neville Li",
    email = "neville.lyh@gmail.com",
    url = url("https://twitter.com/sinisa_lyh")
  ),
  Developer(
    id = "rwhitcomb",
    name = "Richard Whitcomb",
    email = "richwhitjr@gmail.com",
    url = url("https://twitter.com/rwhitcomb")
  ),
  Developer(
    id = "ravwojdyla",
    name = "Rafal Wojdyla",
    email = "ravwojdyla@gmail.com",
    url = url("https://twitter.com/ravwojdyla")
  ),
  Developer(
    id = "fallonfofallon",
    name = "Fallon Chen",
    email = "fallon@spotify.com",
    url = url("https://twitter.com/fallonfofallon")
  ),
  Developer(
    id = "andrew_martin92",
    name = "Andrew Martin",
    email = "andrewsmartin.mg@gmail.com",
    url = url("https://twitter.com/andrew_martin92")
  ),
  Developer(
    id = "regadas",
    name = "Filipe Regadas",
    email = "filiperegadas@gmail.com",
    url = url("https://twitter.com/regadas")
  ),
  Developer(
    id = "slhansen",
    name = "Samantha Hansen",
    email = "slhansen@spotify.com",
    url = url("https://github.com/slhansen")
  )
)

// scala versions
val scala3 = "3.2.1"
val scala213 = "2.13.10"
val scala212 = "2.12.17"
val defaultScala = scala212

// github actions
val java11 = JavaSpec.corretto("11")
val java8 = JavaSpec.corretto("8")
val defaultJava = java11
val coverageCond = Seq(
  s"matrix.scala == '$defaultScala'",
  s"matrix.java == '${defaultJava.render}'"
).mkString(" && ")

ThisBuild / scalaVersion := defaultScala
ThisBuild / crossScalaVersions := Seq(scala3, scala213, scala212)
ThisBuild / githubWorkflowTargetBranches := Seq("main")
ThisBuild / githubWorkflowJavaVersions := Seq(java11, java8)
ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Sbt(
    List("coverage", "test", "coverageAggregate"),
    name = Some("Build project"),
    cond = Some(coverageCond)
  ),
  WorkflowStep.Run(
    List("bash <(curl -s https://codecov.io/bash)"),
    name = Some("Upload coverage report"),
    cond = Some(coverageCond)
  ),
  WorkflowStep.Sbt(
    List("test"),
    name = Some("Build project"),
    cond = Some(s"!($coverageCond)"))
)

val CompileTime = config("compile-time").hide

lazy val currentYear = _root_.java.time.LocalDate.now().getYear
lazy val keepExistingHeader =
  HeaderCommentStyle.cStyleBlockComment.copy(commentCreator =
    (text: String, existingText: Option[String]) =>
      existingText
        .getOrElse(HeaderCommentStyle.cStyleBlockComment.commentCreator(text))
        .trim()
  )

lazy val commonSettings = Seq(
  description := "Feature Transformers",
  tlFatalWarningsInCi := false,
  tlJdkRelease := Some(8),
  tlSkipIrrelevantScalas := true,
  headerLicense := Some(HeaderLicense.ALv2(currentYear.toString, organizationName.value)),
  headerMappings ++= Map(
    HeaderFileType.scala -> keepExistingHeader,
    HeaderFileType.java -> keepExistingHeader
  ),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "3"),
  libraryDependencies ++= Seq(
    ("org.typelevel" %% "simulacrum-scalafix-annotations" % "0.5.4" % CompileTime)
      .cross(CrossVersion.for3Use2_13)
  ),
  ivyConfigurations += CompileTime,
  Compile / unmanagedClasspath ++= update.value.select(configurationFilter(CompileTime.name))
)

lazy val soccoSettings = if (sys.env.contains("SOCCO")) {
  Seq(
    scalacOptions ++= Seq(
      "-P:socco:out:examples/target/site",
      "-P:socco:package_com.spotify.featran:http://spotify.github.io/featran/api",
      "-P:socco:package_com.spotify.scio:http://spotify.github.io/scio/api"
    ),
    autoCompilerPlugins := true,
    addCompilerPlugin(("io.regadas" %% "socco-ng" % "0.1.8").cross(CrossVersion.full))
  )
} else {
  Nil
}

lazy val root: Project = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(
    core,
    java,
    flink,
    scalding,
    scio,
    spark,
    numpy,
    tensorflow,
    xgboost,
    unidocs
  )

lazy val core: Project = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "core",
    moduleName := "featran-core",
    description := "Feature Transformers",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.apache.commons" % "commons-math3" % commonsMathVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    ),
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.scalanlp" %% "breeze" % breezeVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ).map(_.cross(CrossVersion.for3Use2_13))
  )

lazy val java: Project = project
  .in(file("java"))
  .dependsOn(
    core % "compile->compile;test->test",
    tensorflow,
    xgboost
  )
  .settings(commonSettings)
  .settings(
    name := "java",
    moduleName := "featran-java",
    description := "Feature Transformers - java",
    crossScalaVersions := Seq(scala213, scala212),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val flink: Project = project
  .in(file("flink"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "flink",
    moduleName := "featran-flink",
    description := "Feature Transformers - Flink",
    crossScalaVersions := Seq(scala212),
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
      "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val scalding: Project = project
  .in(file("scalding"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "scalding",
    moduleName := "featran-scalding",
    description := "Feature Transformers - Scalding",
    resolvers += "Concurrent Maven Repo" at "https://conjars.org/repo",
    crossScalaVersions := Seq(scala212),
    libraryDependencies ++= Seq(
      "com.twitter" %% "scalding-core" % scaldingVersion % "provided",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val scio: Project = project
  .in(file("scio"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "scio",
    moduleName := "featran-scio",
    description := "Feature Transformers - Scio",
    crossScalaVersions := Seq(scala213, scala212),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion % "provided",
      "com.spotify" %% "scio-test" % scioVersion % "test"
    )
  )

lazy val spark: Project = project
  .in(file("spark"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "spark",
    moduleName := "featran-spark",
    description := "Feature Transformers - Spark",
    crossScalaVersions := Seq(scala213, scala212),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val numpy: Project = project
  .in(file("numpy"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "numpy",
    moduleName := "featran-numpy",
    description := "Feature Transformers - NumPy",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val tensorflow: Project = project
  .in(file("tensorflow"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "tensorflow",
    moduleName := "featran-tensorflow",
    description := "Feature Transformers - TensorFlow",
    libraryDependencies ++= Seq(
      "org.tensorflow" % "tensorflow-core-api" % tensorflowVersion
    ),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    )
  )

lazy val xgboost: Project = project
  .in(file("xgboost"))
  .dependsOn(
    core % "compile->compile;test->test",
  )
  .settings(commonSettings)
  .settings(
    name := "xgboost",
    moduleName := "featran-xgboost",
    description := "Feature Transformers - XGBoost",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
    )
  )

lazy val examples: Project = project
  .in(file("examples"))
  .dependsOn(
    core,
    scio,
    tensorflow
  )
  .settings(commonSettings)
  .settings(soccoSettings)
  .settings(
    name := "examples",
    moduleName := "featran-examples",
    description := "Feature Transformers - examples",
    crossScalaVersions := Seq(scala213, scala212),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion
    ),
    publish / skip := true
  )

lazy val jmh: Project = project
  .in(file("jmh"))
  .enablePlugins(JmhPlugin)
  .dependsOn(
    core,
    tensorflow
  )
  .settings(commonSettings)
  .settings(
    name := "jmh",
    moduleName := "featran-docs",
    description := "Featran JMH Microbenchmarks",
    crossScalaVersions := Seq(scala212),
    Jmh / sourceDirectory := (Test / sourceDirectory).value,
    Jmh / classDirectory := (Test / classDirectory).value,
    Jmh / dependencyClasspath := (Test / dependencyClasspath).value,
    publish / skip := true
  )

lazy val site = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(
    examples,
    unidocs
  )
  .settings(
    name := "site",
    moduleName := "featran-site",
    crossScalaVersions := Seq(scala212),
    tlSitePublishBranch := None,
    tlSitePublishTags := true,
    tlSiteGenerate := Seq(
      WorkflowStep.Sbt(
        List(s"${thisProject.value.id}/${tlSite.key.toString}"),
        name = Some("Generate site"),
        env = Map("SOCCO" -> "True")
      )
    ),
    laikaIncludeAPI := true,
    laikaGenerateAPI / mappings := (unidocs / ScalaUnidoc / packageDoc / mappings).value,
    Global / excludeLintKeys += laikaGenerateAPI / mappings, // false warning
    laikaInputs := InputTree[cats.effect.IO]
      .addDirectory("docs", laika.ast.Path.Root)
      .addDirectory((examples / target).value / "site", laika.ast.Path.Root / "examples"),
    laikaTheme := Helium.defaults.all
      .metadata(
        title = Some("featran"),
        language = Some("en")
      )
      .site
      .topNavigationBar(
        navLinks = List(
          IconLink.internal(
            laika.ast.Path.Root / "api" / "index.html",
            HeliumIcon.api,
            options = Styles("svg-link")
          ),
          IconLink.external(
            scmInfo.value.get.browseUrl.toString,
            HeliumIcon.github,
            options = Styles("svg-link")
          )
        )
      )
      .build,
    laikaExtensions := Seq(GitHubFlavor, SyntaxHighlighting)
  )

lazy val unidocs = project
  .in(file("unidocs"))
  .enablePlugins(TypelevelUnidocPlugin)
  .settings(commonSettings)
  .settings(
    name := "docs",
    moduleName := "featran-docs",
    crossScalaVersions := Seq(scala212),
    ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject --
      inProjects(java) --
      inProjects(examples)
  )
