package weblog.challenge
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Process {
  val schema = StructType(
    Array(
      StructField("ts", TimestampType),
      StructField("elb_name", StringType),
      StructField("client_ip_with_port", StringType),
      StructField("backend_ip_with_port", StringType),
      StructField("request_processing_time", DoubleType),
      StructField("backend_processing_time", DoubleType),
      StructField("response_processing_time", DoubleType),
      StructField("elb_status_code", IntegerType),
      StructField("backend_status_code", IntegerType),
      StructField("received_bytes", LongType),
      StructField("send_bytes", LongType),
      StructField("request", StringType),
      StructField("user_agent", StringType),
      StructField("ssl_cipher", StringType),
      StructField("ssl_protocol", StringType)
    )
  )

  def sessionize(spark: SparkSession, df: DataFrame): DataFrame = {
    // The work here is to strip any fields that are not valuable (given the goals
    // defined in the problem), and to generate session_ids
    // The strategy here is to generate a session_number that is incremented every
    // time we see a new "session" (not requests for past 15 minutes) over
    // defined window, then generate a hash from client_id, and session_number
    // and take that as a session_id
    import spark.implicits._
    val clientIpWindowSpec = Window.partitionBy($"client_ip").orderBy($"ts")
    df
      .withColumn("client_ip", split($"client_ip_with_port", ":").getItem(0))
      .withColumn("previous_ts", lag($"ts", 1).over(clientIpWindowSpec))
      .withColumn("interval", unix_timestamp($"ts") - unix_timestamp($"previous_ts"))
      .withColumn("is_new_session", when($"interval" > (15 * 60), 1).otherwise(0))
      .withColumn("session_number", sum($"is_new_session").over(clientIpWindowSpec))
      .withColumn("session_id", hash($"client_ip", $"session_number"))
      .withColumn("method", split($"request", " ").getItem(0))
      .withColumn("url", split(split($"request", " ").getItem(1), "\\?").getItem(0))
      .select($"ts", $"session_id", $"client_ip", $"url")
  }

  def process(spark: SparkSession, file: String, outputDirectory: String): Unit = {
    import spark.implicits._
    val data = spark.read.format("csv").option("sep", " ").schema(schema).load(file)

    // Generate Session level data to help with future calculations
    // It's also convenient to generate the number of unique URLs per session here
    // We don't actually need the total number of requests for our defined goals
    // but it might be a useful metric for determining the most engaged users
    val sessionLevelData = sessionize(spark, data)
      .groupBy($"session_id", $"client_ip")
      .agg(
        count($"url").as("requests"),
        countDistinct($"url").as("distinct_urls"),
        max(unix_timestamp($"ts")).as("max_ts"),
        min(unix_timestamp($"ts")).as("min_ts")
      )
      .cache()

    // Calculate the average session time (mean and estimate of median)
    // This is just an estimate of the session time. Since we can't actually tell
    // when a session ends, We're just taking the time in between the first request
    // and the final request seen from a particular IP address (within 15 minutes).
    // Choose to calculate median as well in the case that we want to know how
    // skewed the mean is for any particular session.
    val averageSessionLength = sessionLevelData
      .withColumn("session_length", $"max_ts" - $"min_ts")
      .selectExpr(
        "avg(session_length) as mean",
        "approx_percentile(session_length, 0.5, 100) as median"
      )

    // Find the IPs with the longest session times.
    // Again this is only an estimate of session length calculated by taking the
    // difference between time of first request and time of final request within 15
    // minutes for each IP.
    // Calculate both the mean and the actual longest session time, as both of these
    // may be useful to determine how "engaged" a user actually was.
    val mostEngagedUserIps = sessionLevelData
      .groupBy($"client_ip")
      .agg(
        max($"max_ts" - $"min_ts").as("longest_session_duration"),
        avg($"max_ts" - $"min_ts").as("average_session_duration")
      )
      .orderBy(desc("longest_session_duration"))

    sessionLevelData.write.parquet(s"$outputDirectory/session_level_data")
    averageSessionLength.write.parquet(s"$outputDirectory/average_session_length")
    mostEngagedUserIps.write.parquet(s"$outputDirectory/session_lengths_by_client_ip")
  }

  def main(args: Array[String]): Unit = {
    val (inputFile, outputDirectory) = args.toList match {
      case inFile :: outDir :: _ => (inFile, outDir)
      case _ => sys.error("Provide file to process, and output directory")
    }
    val spark = SparkSession.builder.appName("WeblogChallenge").getOrCreate()
    process(spark, inputFile, outputDirectory)
  }
}
