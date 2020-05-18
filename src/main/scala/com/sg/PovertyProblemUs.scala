package com.sg

// Spark program1 - PovertyProblemUS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType

object PovertyProblemUs {

  def main(args: Array[String]): Unit = {
    val povertyInput = args(0)
    val statesInput = args(1)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val poverty_df = spark.read.option("header", true).option("delimiter", """:""").csv(povertyInput)
      .select(col("state"), col("area_name"), col("Urban_Influence_Code_2003").cast(LongType), col("Rural-urban_Continuum_Code_2013").cast(LongType), col("POVALL_2018").cast(LongType), col("POV017_2018").cast(LongType))
      .filter((col("Urban_Influence_Code_2003").isNotNull) && (col("Rural-urban_Continuum_Code_2013").isNotNull) && col("POVALL_2018").isNotNull && col("POV017_2018").isNotNull)
      .filter((col("Urban_Influence_Code_2003") % 2 !== 0) && (col("Rural-urban_Continuum_Code_2013") % 2 === 0))

    val states_df = spark.read.option("header", true).csv(statesInput)

    val POV_elder_than17_2018_udf = udf((pov_all: Long, pov_17: Long) => {
      (pov_17.toDouble / pov_all.toDouble) * 100
    })

    val area_name_with_state_udf = udf((area_name: String, state: String) => {
      s"$area_name $state"
    })


    val df = poverty_df.join(states_df, col("state") === col("Postal Abbreviation"), "inner")
      .withColumn("area_name_with_state", area_name_with_state_udf(col("area_name"), (col("state"))))
      .withColumn("POV_elder_than17_2018", POV_elder_than17_2018_udf(col("POVALL_2018"), col("POV017_2018")))
      .select("Capital Name", "area_name_with_state", "Urban_Influence_Code_2003", "Rural-urban_Continuum_Code_2013", "POV_elder_than17_2018")
      .withColumnRenamed("Capital Name", "state").withColumnRenamed("area_name_with_state", "area_name")

    df.show(false)

  }
}
