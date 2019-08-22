package com.paytm.dataengineerchallenge

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import com.nuvola_tech.spark.MyUDWF

/** Computes an approximation to pi */
object Utils {
  /*
   * CONSTANTS
   */
  val SESSION_UDWF_MINUTE = 60 * 1000 * 1000
  val SESSION_DEFAULT_WINDOW = 15 * SESSION_UDWF_MINUTE

  /*
   * Load ELB Access Log from Input CSV File
   * @param spark
   * @param inputFile
   * @return DataFrame
   */
  def loadELBLog(spark: SparkSession, inputFile: String): DataFrame = {
    // Define ELB access log schema refer to:
    // https://docs.aws.amazon.com/en_us/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
    val ELBAccessLogSchema = StructType(Array(
      StructField("timestamp",                TimestampType,  true),
      StructField("elb",                      StringType,     true),
      StructField("client",                   StringType,     true),
      StructField("backend",                  StringType,     true),
      StructField("request_processing_time",  DoubleType,     true),
      StructField("backend_processing_time",  DoubleType,     true),
      StructField("response_processing_time", DoubleType,     true),
      StructField("elb_status_code",          IntegerType,    true),
      StructField("backend_status_code",      IntegerType,    true),
      StructField("received_bytes",           IntegerType,    true),
      StructField("sent_bytes",               IntegerType,    true),
      StructField("request",                  StringType,     true),
      StructField("user_agent",               StringType,     true),
      StructField("ssl_cipher",               StringType,     true),
      StructField("ssl_protocol",             StringType,     true)
      )
    )

    val df = spark
      .read
      .option("sep", " ")
      .option("quote", "\"")
      .option("header", "true")
      .schema(ELBAccessLogSchema)
      .csv(inputFile)

    // Add user_agent to Help Identify Distinct User
    val logDf = df.withColumn("client_id", f.concat(f.col("client"), f.lit(";"), f.col("user_agent")))

    logDf
  }

  /*
   * Calculate the Session Stat
   * (client, starttime, endtime, session_duration_sec, visit, uniqe_visit)
   * from ELB Access Log DataFrame
   * @param accessLogDf
   * @param sessionWindow
   * @return DataFrame
   */
  def calculateSessionStat(accessLogDf: DataFrame, sessionWindow: Integer = SESSION_DEFAULT_WINDOW): DataFrame =  {
    // Add session_id for by MyUDWF.calculateSession
    val sessionDf = accessLogDf.withColumn(
      "session_id",
      MyUDWF.calculateSession(
        f.col("timestamp"),
        f.lit(null).cast(StringType),
        f.lit(sessionWindow)
      ) over
      Window
        .partitionBy(f.col("client_id"))
        .orderBy(f.col("timestamp").asc)
    )

    // Calculate the starttime, endtime, duration, visit count, etc. among each session
    val sessionStatDf = sessionDf.groupBy("session_id").agg(
      f.first("client_id").as("client_id"),
      f.min("timestamp").as("starttime"),
      f.max("timestamp").as("endtime"),
      (f.unix_timestamp(f.max("timestamp")) - f.unix_timestamp(f.min("timestamp"))).as("session_duration_sec"),
      f.count("request").as("visit"),
      f.countDistinct("request").as("uniqe_visit")
    )

    sessionStatDf
  }
}
