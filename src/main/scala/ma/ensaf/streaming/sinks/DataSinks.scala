package ma.ensaf.streaming.sinks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Section II – Envoi des Données vers différents Puits de Données
 *
 * Adjust the path constants below before running.
 */
object DataSinks {

  // ── Adjust these paths to match your local environment ──────────────────
  private val BASE = "./Sinks"

  private val FILE_OUTPUT_PATH      = s"$BASE/File_OUTPUT"
  private val CHECKPOINT_FILE_PATH  = s"$BASE/checkpoints/checkpoint_file"
  private val CHECKPOINT_KAFKA_PATH = s"$BASE/checkpoints/checkpoint_kafka"

  // ─────────────────────────────────────────────────────────────────────────
  // II.1  File Sink  (append only, fault-tolerant)
  // ─────────────────────────────────────────────────────────────────────────
  def rateToFileSink(spark: SparkSession): Unit = {
    val rateSourceDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "2")
      .load()

    rateSourceDF.printSchema()

    val rateSQ = rateSourceDF.writeStream
      .outputMode("append")
      .format("json")
      .option("path",               FILE_OUTPUT_PATH)
      .option("checkpointLocation", CHECKPOINT_FILE_PATH)
      .start()

    rateSQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // II.2  Foreach Sink  (fully custom write logic via ForeachWriter)
  // ─────────────────────────────────────────────────────────────────────────
  def rateToForeachSink(spark: SparkSession): Unit = {
    val ratesSourceDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "2")
      .load()

    val rateSQ = ratesSourceDF.writeStream
      .foreach(new MyWriter)
      .start()

    rateSQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // II.3  Console Sink  (debug/learning only)
  // ─────────────────────────────────────────────────────────────────────────
  def rateToConsoleSink(spark: SparkSession): Unit = {
    val ratesSourceDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "2")
      .load()

    val ratesSQ = ratesSourceDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .start()

    ratesSQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // II.4  Memory Sink  (debug/learning only – data stored on the Driver)
  // ─────────────────────────────────────────────────────────────────────────
  def rateToMemorySink(spark: SparkSession): Unit = {
    val ratesSourceDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .option("numPartitions", "2")
      .load()

    val ratesSQ = ratesSourceDF.writeStream
      .outputMode("append")
      .format("memory")
      .queryName("rates")   // registers as SQL table "rates"
      .start()

    // Wait a couple of seconds then query the in-memory table
    Thread.sleep(5000)
    spark.sql("SELECT * FROM rates").show(10, truncate = false)

    ratesSQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // II.5  Kafka Sink
  //   • Kafka broker must be running on localhost:9092
  //   • Topic "Sport-0" must exist
  // ─────────────────────────────────────────────────────────────────────────
  def rateToKafkaSink(spark: SparkSession): Unit = {
    import spark.implicits._

    val ratesDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()

    ratesDF.printSchema()

    // Build a key (String) and a value (JSON) as required by Kafka sink
    val ratesSinkDF = ratesDF.select(
      col("value").cast(StringType).as("key"),
      to_json(struct($"timestamp", $"value")).as("value")
    )

    ratesSinkDF.printSchema()

    val rateSinkSQ = ratesSinkDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "Sport-0")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT_KAFKA_PATH)
      .start()

    rateSinkSQ.awaitTermination()
  }
}
