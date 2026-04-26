package ma.ensaf.streaming.sources

import ma.ensaf.streaming.utils.Schemas
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Section I – Réception des Données à partir de différents Sources
 *
 * Each method corresponds to one sub-section of the TP.
 * Run the one you need from Main.scala (comment / uncomment).
 */
object DataSources {

  // ─────────────────────────────────────────────────────────────────────────
  // I.1  Source « Socket »
  //   Prerequisites:
  //     Windows : ncat -l -p 9999   (Nmap/Ncat must be installed)
  //     macOS   : nc -lk 9999
  // ─────────────────────────────────────────────────────────────────────────
  def fromSocket(spark: SparkSession): Unit = {
    import spark.implicits._

    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    // Word count – classic streaming hello-world
    val words      = socketDF.as[String].flatMap(s => s.split(" "))
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // I.2  Source « Rate »   (test/learning only)
  // ─────────────────────────────────────────────────────────────────────────
  def fromRate(spark: SparkSession): Unit = {
    import spark.implicits._

    val rateSourceDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "3")
      .option("numPartitions", "4")
      .load()

    // Add partition_id column to observe distribution
    val rateDF = rateSourceDF.withColumn("partition_id", spark_partition_id())

    val rateQuery = rateDF.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    rateQuery.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // I.3  Source « File »
  //   • Put JSON files in  data/json_input/mobile/  (or adjust dir_path)
  //   • Each file must have rows: { "id":"...", "action":"...", "ts":"..." }
  // ─────────────────────────────────────────────────────────────────────────
  def fromFile(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val mobileSSDF = spark.readStream
      .schema(Schemas.MobileDataSchema)
      // Uncomment to limit files per trigger:
      // .option("maxFilesPerTrigger", 5)
      // Uncomment to process newest files first:
      // .option("latestFirst", "true")
      .json(dirPath)

    // Group by 10-minute tumbling window on the event timestamp
    val actionCountDF = mobileSSDF
      .groupBy(window($"ts", "10 minutes"), $"action")
      .count()

    val fileQuery = actionCountDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    fileQuery.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // I.4  Source « Kafka »
  //   Prerequisites:
  //     • Kafka broker running on localhost:9092
  //     • Topic "Sport-0" created
  //     • SBT dependency: spark-sql-kafka-0-10 (already in build.sbt)
  // ─────────────────────────────────────────────────────────────────────────
  def fromKafka(spark: SparkSession): Unit = {
    import spark.implicits._

    // ── Option A: single topic, from earliest offset ──
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Sport-0")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.printSchema()

    // Deserialize key & value from binary to String
    val kafkaDFStr = kafkaDF
      .selectExpr(
        "partition",
        "offset",
        "CAST(key   AS STRING)",
        "CAST(value AS STRING)"
      )
      .as[(String, Long, String, String)]

    /* ── Option B: multiple topics ──
    val kafkaDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Sport-0,Sport-1")
      .load()

    ── Option C: topic pattern ──
    val kafkaDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribePattern", "Sport.*")
      .load()

    ── Option D: start from specific partition offset ──
    val kafkaDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "Sport-0")
      .option("startingOffsets", """{"Sport-0":{"0":3}}""")
      .load()
    */

    val kafkaQuery = kafkaDFStr.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    kafkaQuery.awaitTermination()
  }
}
