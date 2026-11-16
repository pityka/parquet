inThisBuild(
  List(
    organization := "io.github.pityka",
    homepage := Some(url("https://github.com/pityka/parquet")),
    licenses := List(("MIT", url("https://opensource.org/licenses/MIT"))),
    developers := List(
      Developer(
        "pityka",
        "Istvan Bartha",
        "bartha.pityu@gmail.com",
        url("https://github.com/pityka/parquet")
      )
    )
  )
)

lazy val scalaVersion3 = "3.9.0"

ThisBuild / scalaVersion := scalaVersion3

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / publishTo := Some(
  "GitHub Packages" at "https://maven.pkg.github.com/pityka/parquet"
)

ThisBuild / credentials ++= {
  for {
    actor <- sys.env.get("GITHUB_ACTOR")
    token <- sys.env.get("GITHUB_TOKEN")
  } yield Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    actor,
    token
  )
}.toSeq

lazy val commonSettings = Seq(
  scalaVersion := scalaVersion3,
  Test / parallelExecution := false,
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "utf-8",
    "-feature",
    "-language:postfixOps",
    "-no-indent",
    "-explain",
    "-unchecked",
    "-Werror"
  ),
  Compile / console / scalacOptions ~= (_.filterNot(_ == "-Werror")),
  Global / cancelable := true,
  fork := true
)

lazy val root = project
  .in(file("."))
  .settings(commonSettings)
  .settings(
    name := "parquet",
    publishArtifact := false,
    publish / skip := true
  )
  .aggregate(interface, core)

lazy val interface = project
  .in(file("interface"))
  .settings(commonSettings)
  .settings(
    name := "parquet-interface"
  )

lazy val core = project
  .in(file("core"))
  .dependsOn(interface)
  .settings(commonSettings)
  .settings(
    name := "parquet-core",
    libraryDependencies ++= List(
      "io.airlift" % "aircompressor" % "0.25",
      "org.scalameta" %% "munit" % "1.3.6" % Test
    )
  )
