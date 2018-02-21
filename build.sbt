name := "rdb-connector-postgresql"

version := "0.1"

scalaVersion := "2.12.3"

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )


resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech" %  "rdb-connector-common" % "159e1ff513",
    "com.typesafe.slick"  %% "slick"                % slickV,
    "com.typesafe.slick"  %% "slick-hikaricp"       % slickV,
    "org.scalatest"       %% "scalatest"            % scalaTestV   % "test",
    "com.typesafe.akka"   %% "akka-stream-testkit"  % "2.5.6"      % "test",
    "com.github.emartech" %  "rdb-connector-test"   % "f66163b23e" % "test",
    "org.postgresql"      %  "postgresql"           % "42.1.4"
  )
}