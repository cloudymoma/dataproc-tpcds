name := "tpcds-datagen"
version := "1.0.0"
scalaVersion := "2.12.18"

// Spark version matching Dataproc 2.3 (Spark 3.5.x)
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)

// spark-sql-perf dependency from local JAR (compile only, not packaged)
// This JAR will be passed separately to Spark via jar_file_uris
Compile / unmanagedJars += file("lib/spark-sql-perf-assembly-0.5.1.jar")

// Package settings - create a simple JAR with just our main class
Compile / packageBin / mainClass := Some("com.tpcds.TPCDSDataGen")

// For assembly (optional, creates fat JAR if needed)
assembly / assemblyJarName := s"tpcds-datagen-${version.value}.jar"
assembly / mainClass := Some("com.tpcds.TPCDSDataGen")

// Merge strategy for assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ => MergeStrategy.concat
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x => MergeStrategy.first
}

// Don't include Scala library (provided by Spark)
assembly / assemblyOption := (assembly / assemblyOption).value
  .withIncludeScala(false)

// Compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)
