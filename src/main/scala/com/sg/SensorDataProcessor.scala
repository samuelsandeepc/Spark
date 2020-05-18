package com.sg
//problem4
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lead

object SensorDataProcessor {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val inputDf = spark.read.format("csv").option("header", "true").load(inputPath)
    /*    root
        |-- Sensor: string (nullable = true)
        |-- Mnemonic: string (nullable = true)
        |-- data: string (nullable = true)
        |-- timestamp: string (nullable = true)*/
    val res = inputDf.withColumn("end_date", lead("timestamp", 1).over(Window.partitionBy("Mnemonic").orderBy("Mnemonic"))).withColumn("nextColData", lead("data", 1).over(Window.partitionBy("Mnemonic").orderBy("Mnemonic"))).withColumnRenamed("timestamp", "start_date")
    val finalOutput = res.where(res("data") =!= res("nextColData")).drop("nextColData").show(false)
  }
}
