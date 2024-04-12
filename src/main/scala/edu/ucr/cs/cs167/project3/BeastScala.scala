package edu.ucr.cs.cs167.project3

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Project Group 3 - Chicago Crimes - Task 1
 **/

object BeastScala {
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

    //set input and output file values
    val inputFile: String = args(0)
    val outputFile: String = args(1)

    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      //parse and load csv file using the Dataframe API
      val crimesDF = sparkSession.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputFile)
      //crimesDF.printSchema()

      //add a geometry attribute to represent the location of each crime
      crimesDF.selectExpr("*", "ST_CreatePoint(x,y) AS geometry")

      //rename all the columns with a space and remove it
      var renamedDF = crimesDF
      for(col <- renamedDF.columns){
        renamedDF  = renamedDF.withColumnRenamed(col,col.replaceAll("\\s", ""))
      }

      //convert the dataframe into a SpatialRDD
      val crimesRDD: SpatialRDD = renamedDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD
      //load the zip dataset using Beast
      val zipsRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
      //spatial join to find zip of each crime
      val crimesZipRDD: RDD[(IFeature, IFeature)] = crimesRDD.spatialJoin(zipsRDD)
      //add new attribute ZIPCode to crime df
      val crimesZip: DataFrame = crimesZipRDD.map({ case (crime, zip) => Feature.append(crime, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
        .toDataFrame(sparkSession)

      //drop geometry column
      val finalDF: DataFrame = crimesZip.drop(crimesZip("geometry"))
      //finalDF.printSchema()
      //write output to parquet file
      finalDF.write.mode(SaveMode.Overwrite).parquet(outputFile)
    }

    sparkSession.stop()
  }

}
