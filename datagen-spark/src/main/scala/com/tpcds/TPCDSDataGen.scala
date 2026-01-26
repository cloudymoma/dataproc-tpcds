/*
 * TPC-DS Data Generator Main Class
 *
 * A simple wrapper around spark-sql-perf's TPCDSTables to generate
 * TPC-DS benchmark data on Spark clusters.
 */
package com.tpcds

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import org.apache.spark.sql.SparkSession

object TPCDSDataGen {

  case class Config(
    dsdgenDir: String = "./tpcds_kit/tools",
    location: String = "",
    scaleFactor: Int = 1,
    format: String = "parquet",
    numPartitions: Int = 100,
    partitionTables: Boolean = true,
    clusterByPartitionColumns: Boolean = true,
    filterOutNullPartitionValues: Boolean = false,
    useDoubleForDecimal: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    if (config.location.isEmpty) {
      System.err.println("Error: --location is required")
      printUsage()
      System.exit(1)
    }

    println(s"=== TPC-DS Data Generator ===")
    println(s"Scale Factor: ${config.scaleFactor} GB")
    println(s"Output Location: ${config.location}")
    println(s"Format: ${config.format}")
    println(s"Partitions: ${config.numPartitions}")
    println(s"dsdgen Directory: ${config.dsdgenDir}")
    println()

    val spark = SparkSession.builder()
      .appName(s"TPC-DS Data Generation SF=${config.scaleFactor}")
      .getOrCreate()

    try {
      val tables = new TPCDSTables(
        spark.sqlContext,
        dsdgenDir = config.dsdgenDir,
        scaleFactor = config.scaleFactor.toString,
        useDoubleForDecimal = config.useDoubleForDecimal,
        useStringForDate = false
      )

      println(s"Generating ${tables.tables.length} TPC-DS tables...")

      tables.genData(
        location = config.location,
        format = config.format,
        overwrite = true,
        partitionTables = config.partitionTables,
        clusterByPartitionColumns = config.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.filterOutNullPartitionValues,
        numPartitions = config.numPartitions
      )

      println()
      println("=== Data Generation Complete ===")
      println(s"Data written to: ${config.location}")

    } finally {
      spark.stop()
    }
  }

  def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0

    while (i < args.length) {
      args(i) match {
        case "--dsdgenDir" =>
          config = config.copy(dsdgenDir = args(i + 1))
          i += 2
        case "--location" =>
          config = config.copy(location = args(i + 1))
          i += 2
        case "--scaleFactor" =>
          config = config.copy(scaleFactor = args(i + 1).toInt)
          i += 2
        case "--format" =>
          config = config.copy(format = args(i + 1))
          i += 2
        case "--numPartitions" =>
          config = config.copy(numPartitions = args(i + 1).toInt)
          i += 2
        case "--partitionTables" =>
          config = config.copy(partitionTables = args(i + 1).toBoolean)
          i += 2
        case "--clusterByPartitionColumns" =>
          config = config.copy(clusterByPartitionColumns = args(i + 1).toBoolean)
          i += 2
        case "--filterOutNullPartitionValues" =>
          config = config.copy(filterOutNullPartitionValues = args(i + 1).toBoolean)
          i += 2
        case "--useDoubleForDecimal" =>
          config = config.copy(useDoubleForDecimal = args(i + 1).toBoolean)
          i += 2
        case "--tableFilter" =>
          // Ignored for compatibility
          i += 2
        case "--help" | "-h" =>
          printUsage()
          System.exit(0)
        case unknown =>
          System.err.println(s"Unknown argument: $unknown")
          printUsage()
          System.exit(1)
      }
    }

    config
  }

  def printUsage(): Unit = {
    println("""
      |Usage: TPCDSDataGen [options]
      |
      |Options:
      |  --dsdgenDir <path>                Path to dsdgen binary (default: ./tpcds_kit/tools)
      |  --location <path>                 Output location (GCS or HDFS path) [required]
      |  --scaleFactor <int>               Scale factor in GB (default: 1)
      |  --format <string>                 Output format: parquet, orc (default: parquet)
      |  --numPartitions <int>             Number of partitions (default: 100)
      |  --partitionTables <bool>          Partition large tables (default: true)
      |  --clusterByPartitionColumns <bool> Cluster by partition columns (default: true)
      |  --filterOutNullPartitionValues <bool> Filter null partitions (default: false)
      |  --useDoubleForDecimal <bool>      Use double for decimal (default: false)
      |""".stripMargin)
  }
}
