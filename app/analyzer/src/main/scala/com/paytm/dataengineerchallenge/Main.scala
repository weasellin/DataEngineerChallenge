package com.paytm.dataengineerchallenge

import com.paytm.dataengineerchallenge.Utils.{loadELBLog, calculateSessionStat}

import org.apache.spark.sql.{SparkSession, functions => f}

/** Do the Analysis to Web Access Session */
object WebAnalyze {
  val ENGAGED_TOPN = 20
  val INPUT_FILE = "file:///data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Web Analyzer")
      .config("spark.sql.shuffle.partitions", "10")
      .getOrCreate()

    val rawDf = loadELBLog(spark, INPUT_FILE)
    val sessionStatDf = calculateSessionStat(rawDf)

    sessionStatDf.cache()

    // Save each session stat
    sessionStatDf
      .write
      .format("csv")
      .mode("overwrite")
      .save("file:///data/output/session")
    
    // Calculate the averaged session duration and save
    sessionStatDf
      .select(f.avg("session_duration_sec"))
      .write
      .format("csv")
      .mode("overwrite")
      .save("file:///data/output/session_duration_avg")

    // Calaulate the most engaged session and save
    sessionStatDf
      .orderBy(f.desc("session_duration_sec"))
      .limit(ENGAGED_TOPN)
      .write
      .format("csv")
      .mode("overwrite")
      .save("file:///data/output/session_longest")

    spark.stop()
  }
}
