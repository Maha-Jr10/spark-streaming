package ma.ensaf.streaming.sinks

import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * Custom ForeachWriter used in Section II.2.
 *
 * Lifecycle (called per partition per trigger):
 *   open()    – initialise resources (DB connection, socket, …)
 *   process() – called once per row
 *   close()   – clean up, always guaranteed (except JVM crash)
 *
 * This implementation simply prints to stdout for illustration.
 * In production you would open a JDBC connection in open() and
 * use it in process().
 */
class MyWriter(private var pId: Long = 0, private var ver: Long = 0)
  extends ForeachWriter[Row] {

  def open(partitionId: Long, version: Long): Boolean = {
    pId = partitionId
    ver = version
    println(s"[ForeachWriter] open => (partitionId=$partitionId, version=$version)")
    true   // return false to skip this partition/version
  }

  def process(row: Row): Unit = {
    println(s"[ForeachWriter] writing => $row")
  }

  def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull != null)
      println(s"[ForeachWriter] close with ERROR => $errorOrNull")
    else
      println(s"[ForeachWriter] close => (partitionId=$pId, version=$ver)")
  }
}
