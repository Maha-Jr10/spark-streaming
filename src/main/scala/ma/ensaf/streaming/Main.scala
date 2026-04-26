package ma.ensaf.streaming

import ma.ensaf.streaming.sources.DataSources
import ma.ensaf.streaming.sinks.DataSinks
import ma.ensaf.streaming.transformations.Transformations
import org.apache.spark.sql.SparkSession

/**
 * ═══════════════════════════════════════════════════════════════════
 *  Atelier N°5  –  Apache Spark: Spark Structured Streaming
 *  Pr. MOUNTASSER IMADEDDINE  |  ENSAF – ISDIA S4
 * ═══════════════════════════════════════════════════════════════════
 *
 *  HOW TO RUN:
 *    1. In IntelliJ: right-click Main → Run 'Main'
 *    2. Uncomment EXACTLY ONE exercise call below, save, and re-run.
 *
 *  BEFORE RUNNING:
 *    • Adjust BASE_DIR to a real directory on your machine.
 *    • For Socket sources: start Netcat first
 *        Windows:  ncat -l -p 9999   (port 5555 for hospital socket)
 *        macOS:    nc -lk 9999
 *    • For Kafka exercises: start Zookeeper + Kafka broker and
 *      create topic "Sport-0".
 */
object Main {

  // ── Paths – change these to match YOUR environment ─────────────────────
  val BASE_DIR       = "./Datasets"
  val JSON_DIR       = s"$BASE_DIR/json_files_patients"   // drop patient .json files here
  val MOBILE_DIR     = s"$BASE_DIR/json_files"     // mobile action .json files
  // ────────────────────────────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Atelier5_SparkStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")   // smaller for local mode
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ────────────────────────────────────────────────────────────────────
    //  SECTION I  –  Sources  (uncomment one at a time)
    // ────────────────────────────────────────────────────────────────────

    // I.1  Socket word-count (start ncat -l -p 9999 first)
    // DataSources.fromSocket(spark)

    // I.2  Rate source (no setup needed)
    // DataSources.fromRate(spark)

    // I.3  File source – mobile actions
    // DataSources.fromFile(spark, MOBILE_DIR)

    // I.4  Kafka source (Kafka must be running)
    // DataSources.fromKafka(spark)

    // ────────────────────────────────────────────────────────────────────
    //  SECTION II  –  Sinks  (uncomment one at a time)
    // ────────────────────────────────────────────────────────────────────

    // II.1  File sink (JSON output)
    //DataSinks.rateToFileSink(spark)

    // II.2  Foreach sink (custom writer)
    //DataSinks.rateToForeachSink(spark)

    // II.3  Console sink
    //DataSinks.rateToConsoleSink(spark)

    // II.4  Memory sink + ad-hoc SQL query
    //DataSinks.rateToMemorySink(spark)

    // II.5  Kafka sink (Kafka must be running)
    //DataSinks.rateToKafkaSink(spark)

    // ────────────────────────────────────────────────────────────────────
    //  SECTION III  –  Transformations  (uncomment one at a time)
    // ────────────────────────────────────────────────────────────────────

    // III.1a  Stateless – hospital socket (start ncat -l -p 5555 first)
    //Transformations.fromHospitalSocket(spark)

    // III.1b  Stateless – file source + DID count
    //Transformations.fromJsonFile(spark, JSON_DIR)

    // III.2a  Stateful – global count
    //Transformations.globalAggregation(spark, JSON_DIR)

    // III.2b  Stateful – grouped by DID + DNom
    //Transformations.groupedAggregation(spark, JSON_DIR)

    // III.2c  Stateful – multiple agg functions
    //Transformations.multipleAggregations(spark, JSON_DIR)

    // III.2d  Time – Tumbling window (10 s)
    //Transformations.tumblingWindows(spark, JSON_DIR)

    // III.2e  Time – Sliding window (10 s / 5 s)
    //Transformations.slidingWindows(spark, JSON_DIR)

    // III.2f  Time – Session window (fixed gap 10 s)
    //Transformations.sessionWindows(spark, JSON_DIR)

    // III.2g  Time – Session window (dynamic gap per NSS)
    //Transformations.sessionWindowsDynamic(spark, JSON_DIR)

    // III.2h  Watermark + Session window (30 s watermark)
    Transformations.sessionWindowsWithWatermark(spark, JSON_DIR)

    spark.stop()
  }
}
