package ma.ensaf.streaming.utils

import org.apache.spark.sql.types._

// Case class matching the hospital JSON schema used throughout the TP
case class Patient(
  NSS:   String,
  Nom:   String,
  DID:   Int,
  DNom:  String,
  Fecha: String
)

object Schemas {

  /** Schema for the hospital queue-management JSON events */
  val PatientsSchema: StructType = new StructType()
    .add("NSS",   StringType,    nullable = false)
    .add("Nom",   StringType,    nullable = false)
    .add("DID",   IntegerType,   nullable = false)
    .add("DNom",  StringType,    nullable = false)
    .add("Fecha", StringType,    nullable = false)

  /** Schema for mobile-activity JSON files (Section I.3) */
  val MobileDataSchema: StructType = new StructType()
    .add("id",     StringType,    nullable = false)
    .add("action", StringType,    nullable = false)
    .add("ts",     TimestampType, nullable = false)
}
