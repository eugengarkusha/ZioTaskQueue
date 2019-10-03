name := "CancellableTaskQueue"

version := "0.1"

scalaVersion := "2.13.0"

scalacOptions ++= Seq("-Xfatal-warnings", "-feature")
libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "1.0.0-RC11-1",
  "dev.zio" %% "zio-interop-cats" % "2.0.0.0-RC2",
  "dev.zio" %% "zio-streams" % "1.0.0-RC11-1",
  "org.typelevel" %% "cats-effect" % "2.0.0-RC1",
  "org.typelevel" %% "cats-core" % "2.0.0-RC1",
  "com.lihaoyi" %% "utest" % "0.7.1" % "test"
)

testFrameworks += new TestFramework("utest.runner.Framework")
