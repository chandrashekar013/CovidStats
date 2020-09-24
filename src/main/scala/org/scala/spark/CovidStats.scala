package org.scala.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

/**
 * @author :Chandra
 */
object CovidStats {

  def loadData(): Unit = {
    val spark = SparkSession.builder().appName("Store Forecast").
      config("spark.master", "local").getOrCreate()

    //build schema
    val covidSchema = StructType(Array(
      StructField("Date_reported", DateType),
      StructField("Country_Code", StringType),
      StructField("Country", StringType),
      StructField("WHO_region", StringType),
      StructField("New_cases", LongType),
      StructField("Cumulative_cases", LongType),
      StructField("New_deaths", LongType),
      StructField("Cumulative_deaths", LongType)
    ))

    import spark.implicits._
    //load covid Data as oon 22nd Sept 2020
    val covidGlobalDF = spark.read.format("csv").schema(covidSchema).
      option("header", "true").
      load("src/main/resources/data/CovidGlobalData.csv")

    covidGlobalDF.createOrReplaceTempView("covid")

    //Different use cases:
    //a. list of countries under each WHO region
    val countryGroups = spark.sql("select distinct WHO_region, Country from covid order by WHO_region, Country " +
      "")
    countryGroups.show(200)

    //b. Top 5 effected countries in Europe
    val europeMostEffected = spark.sql(
      """select * from (select Country,sum(Cumulative_cases) as Total_Cases
          from covid  where WHO_region ='EURO' group by Country
           order by sum(Cumulative_cases) desc)  limit 5 """)
    europeMostEffected.show()

    // Month-wise Top 20 effected Countries in each month till 22nd Sept 2020
    val monthWiseWorstEffected = spark.sql(
      """select * from (select Country,Total_cases,MON, RANK() over (PARTITION BY MON ORDER BY TOTAL_cases desc) as rank from (select Country,sum(Cumulative_cases) as Total_cases,MONTH(Date_reported)
        |   as MON
        |   from covid group by Country,MONTH(Date_reported))) where rank <=20 order by MON,rank """.stripMargin)
    monthWiseWorstEffected.show(200)

  }

  def main(args: Array[String]) {
    println("Program about to start!")
    loadData()
  }
}