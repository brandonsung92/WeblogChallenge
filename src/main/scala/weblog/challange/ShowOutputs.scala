package weblog.challenge
import org.apache.spark.sql.SparkSession

object ShowOutputs {

  def main(args: Array[String]): Unit = {
    val outputDirectory = args.toList match {
      case outDir :: _ => outDir
      case _ => sys.error("Provide output directory for processed data")
    }

    val spark = SparkSession.builder.appName("WeblogChallengeShowOutputs").getOrCreate()
    import spark.implicits._
    println("Reading session level data")
    spark
      .read
      .format("parquet")
      .load(s"$outputDirectory/session_level_data")
      .show(50)

    println("Reading average session length")
    spark
      .read
      .format("parquet")
      .load(s"$outputDirectory/average_session_length")
      .show(1) // There should only be one record

    println("Reading session lengths by client IP")
    spark
      .read
      .format("parquet")
      .load(s"$outputDirectory/session_lengths_by_client_ip")
      .show(50)
  }
}
