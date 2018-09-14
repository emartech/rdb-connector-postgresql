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

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Xlint"
)

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech" %  "rdb-connector-common" % "ed038bfb07",
    "com.typesafe.slick"  %% "slick"                % slickV,
    "com.typesafe.slick"  %% "slick-hikaricp"       % slickV,
    "org.scalatest"       %% "scalatest"            % scalaTestV   % Test,
    "com.typesafe.akka"   %% "akka-stream-testkit"  % "2.5.6"      % Test,
    "com.github.emartech" %  "rdb-connector-test"   % "0140815d5c" % Test,
    "org.postgresql"      %  "postgresql"           % "42.1.4",
    "com.typesafe.akka"   %% "akka-http-spray-json" % "10.0.7"     % Test,
    "org.mockito"         %  "mockito-core"         % "2.11.0"     % Test
  )
}
