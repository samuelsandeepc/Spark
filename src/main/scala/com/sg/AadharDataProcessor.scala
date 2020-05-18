package com.sg

import org.apache.spark.sql.SparkSession
//Problem2
object AadharDataProcessor {

  def main(args: Array[String]): Unit = {

    val inputPath = args(0)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load(inputPath)
    val filteredOutput = df.filter(row => row.getAs[String]("sa").matches("""\d+"""))
    val result = filteredOutput.filter(filteredOutput("aua") > 650000 && filteredOutput("res_state_name") =!= "Delhi" ).show(false)
  }

}
