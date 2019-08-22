package com.paytm.dataengineerchallenge

import org.scalactic._
import org.scalatest.FlatSpec

import org.apache.spark.sql.{SparkSession, functions => f}

import com.paytm.dataengineerchallenge.WebAnalyze._
import com.paytm.dataengineerchallenge.Utils._

class TestSpec extends FlatSpec {
  
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  "Session Stat Calculation" must "Consistent" in {

    val spark = SparkSession
      .builder
      .master("local")
      .config("spark.sql.shuffle.partitions", "10")
      .getOrCreate()

    val rawDf = loadELBLog(spark, INPUT_FILE)                                                                                                                                 
    val sessionStatDf = calculateSessionStat(rawDf)                                                                                                                           
    sessionStatDf.cache()
    
    val avgSessionDuration = sessionStatDf.select(f.avg("session_duration_sec")).collect()
    assert(avgSessionDuration(0).getDouble(0).equals(211.20631982892198))

    val engagedSession = sessionStatDf
      .orderBy(f.desc("session_duration_sec"))
      .select("client_id", "session_duration_sec", "visit", "uniqe_visit")
      .take(1)
    assert(
      engagedSession(0).getString(0).equals("103.29.159.138:57045;Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0") &&
      engagedSession(0).getLong(1).equals(3007L) &&
      engagedSession(0).getLong(2).equals(96L) &&
      engagedSession(0).getLong(3).equals(3L)
    )
  }
}
