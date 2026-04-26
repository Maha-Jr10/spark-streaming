package ma.ensaf.streaming.transformations

import ma.ensaf.streaming.utils.{Patient, Schemas}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Section III – Spark Structured Streaming Transformations
 *
 * All methods read from the hospital patient JSON directory (dir_path)
 * unless stated otherwise.
 *
 * Sample patient JSON row:
 *   {"NSS":"1234","Nom":"Maria","DID":10,"DNom":"Cardio","Fecha":"2023-02-23T00:00:00.002Z"}
 */
object Transformations {

  // ─────────────────────────────────────────────────────────────────────────
  // III.1a  Stateless – live socket ingestion (raw JSON lines)
  // ─────────────────────────────────────────────────────────────────────────
  def fromHospitalSocket(spark: SparkSession): Unit = {
    import spark.implicits._

    val fromSocket = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "5555")
      .load()

    fromSocket.printSchema()

    val patientDS = fromSocket
      .select(from_json(col("value"), Schemas.PatientsSchema).as("patient"))
      .selectExpr("patient.*")
      .as[Patient]

    // Simple stateless SELECT *
    val selectDF = patientDS.select("*")

    val SQ = selectDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.1b  Stateless – file source with a simple aggregation by DID
  // ─────────────────────────────────────────────────────────────────────────
  def fromJsonFile(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)

    // Stateful aggregation: count occurrences of each DID
    val groupDF = patientDF
      .groupBy("DID")
      .agg(count("DID").as("Accumulated"))
      .sort(desc("Accumulated"))

    groupDF.printSchema()

    val SQ = groupDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2a  Stateful – Global aggregation (no key)
  // ─────────────────────────────────────────────────────────────────────────
  def globalAggregation(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .as[Patient]

    val counts = patientDF.groupBy().count()

    val SQ = counts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2b  Stateful – Grouped aggregation (by DID + DNom)
  // ─────────────────────────────────────────────────────────────────────────
  def groupedAggregation(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .as[Patient]

    val counts = patientDF
      .groupBy(col("DID"), col("DNom"))
      .count()

    val SQ = counts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2c  Stateful – Multiple aggregations in one groupBy
  // ─────────────────────────────────────────────────────────────────────────
  def multipleAggregations(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .as[Patient]

    val aggMultiple = patientDF
      .groupBy(col("DID"), col("DNom"))
      .agg(
        count("*").as("countDID"),
        sum("DID").as("sumDID"),
        mean("DID").as("meanDID"),
        stddev("DID").as("stddevDID"),
        approx_count_distinct("DID").as("distinctDID"),
        collect_list("DID").as("collect_listDID")
      )

    val SQ = aggMultiple.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "50")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2d  Time-based – Tumbling Window (10 s, no overlap)
  // ─────────────────────────────────────────────────────────────────────────
  def tumblingWindows(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)

    patientDF.printSchema()

    // NOTE: Fecha is a String in the schema, cast to Timestamp for window()
    val patients = patientDF
      .withColumn("Fecha", to_timestamp(col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
      .groupBy(window(col("Fecha"), "10 seconds"))
      .agg(count("DNom").as("Suma_x_Dpt"))

    val SQ = patients.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2e  Time-based – Sliding Window (10 s window, 5 s slide)
  // ─────────────────────────────────────────────────────────────────────────
  def slidingWindows(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .withColumn("Fecha", to_timestamp(col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    patientDF.printSchema()

    val patientsCount = patientDF
      .groupBy(window(col("Fecha"), "10 seconds", "5 seconds"))
      .agg(count("DID").as("Suma_x_Dpt"))

    // Optionally split window into start / end columns:
    // .select("window.start", "window.end", "Suma_x_Dpt")

    val SQ = patientsCount.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2f  Time-based – Session Window (fixed gap = 10 s)
  // ─────────────────────────────────────────────────────────────────────────
  def sessionWindows(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .withColumn("Fecha", to_timestamp(col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    patientDF.printSchema()

    val patientsCount = patientDF
      .groupBy(
        session_window(col("Fecha"), "10 seconds"),
        col("DID")
      )
      .count()

    val SQ = patientsCount.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2g  Time-based – Session Window with DYNAMIC gap (per NSS value)
  // ─────────────────────────────────────────────────────────────────────────
  def sessionWindowsDynamic(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      .withColumn("Fecha", to_timestamp(col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    patientDF.printSchema()

    val patientsCount = patientDF
      .groupBy(
        session_window(
          col("Fecha"),
          when(col("NSS") === "1009", "10 seconds")
            .when(col("NSS") === "2001", "30 seconds")
            .when(col("NSS") === "5000", "50 seconds")
            .otherwise("60 seconds")
        ),
        col("DID")
      )
      .count()

    val SQ = patientsCount.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }

  // ─────────────────────────────────────────────────────────────────────────
  // III.2h  Watermarking – Session window + 30-second watermark
  //   Late events arriving more than 30 s after the max observed Fecha
  //   are dropped; state older than (window_end + 30 s) is evicted.
  // ─────────────────────────────────────────────────────────────────────────
  def sessionWindowsWithWatermark(spark: SparkSession, dirPath: String): Unit = {
    import spark.implicits._

    val patientDF = spark.readStream
      .schema(Schemas.PatientsSchema)
      .json(dirPath)
      // Convert String → Timestamp before withWatermark
      .withColumn("Fecha", to_timestamp(col("Fecha"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    patientDF.printSchema()

    val patientsCount = patientDF
      .withWatermark("Fecha", "30 seconds")
      .groupBy(
        session_window(col("Fecha"), "10 seconds"),
        col("DID")
      )
      .count()

    val SQ = patientsCount.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    SQ.awaitTermination()
  }
}
