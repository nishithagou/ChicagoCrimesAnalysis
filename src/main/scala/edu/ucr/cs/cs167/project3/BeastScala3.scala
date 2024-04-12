package edu.ucr.cs.cs167.project3

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import scala.collection.Map

/**
 * Project Group 3 - Chicago Crimes - Task 3
 **/
object BeastScala3 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = "count-crime-types"
    val inputFile: String = "Chicago_Crimes_ZIP"
    val start_dt: String = args(0)
    val end_dt: String = args(1)
    val outputFile: String = "chicago_crime_zip_graph"
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "count-crime-types" =>
          sparkSession.read.parquet(inputFile).createOrReplaceTempView("ccz")
          val df = sparkSession.sql(s"""
            SELECT PrimaryType, Count(*) Count
            FROM ccz
            WHERE to_timestamp(Date, "MM/dd/yyyy hh:mm:ss a") BETWEEN to_date("$start_dt","MM/DD/yyyy") AND to_date("$end_dt","MM/DD/yyyy")
            GROUP BY PrimaryType
          """).coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(outputFile)
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}
