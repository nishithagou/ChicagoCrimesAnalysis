package edu.ucr.cs.cs167.project3

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.Map

/**
 * Project Group 3 - Chicago Crimes - Task 2
 **/

object BeastScala2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = args(0)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()

      // load parquet file called inputFile
      sparkSession.read.parquet(inputFile)
        .createOrReplaceTempView("crimes")

      // create temp view of aggregate called crime_counts
      sparkSession.sql(s"""
            SELECT ZIPCode, count(*) AS count
            FROM crimes
            GROUP BY ZIPCode
          """).createOrReplaceTempView("crime_counts")

      // load in shapefile tl_2018_us_zcta510.zip
      sparkContext.shapefile("tl_2018_us_zcta510.zip")
        .toDataFrame(sparkSession)
        .createOrReplaceTempView("ZIPCodes")

      // join, coalesce, and save as shapeFile with name ZIPCodeCrimeCount
      sparkSession.sql(s"""
            SELECT ZIPCode, g, count
            FROM crime_counts, ZIPCodes
            WHERE ZIPCode = ZCTA5CE10
          """)
        .toSpatialRDD
        .coalesce(1)
        .saveAsShapefile("ZIPCodeCrimeCount")

      val t2 = System.nanoTime()
      println(s"Operation on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
    } finally {
      sparkSession.stop()
    }
  }
}
