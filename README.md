# 🚀 Spark Structured Streaming – Real-Time Data Processing (Atelier 5)

[![Apache Spark](https://img.shields.io/badge/Spark-3.5.8-orange.svg)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.12.15-red.svg)](https://www.scala-lang.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.3.1-black.svg)](https://kafka.apache.org/)

A complete implementation of **Apache Spark Structured Streaming (Scala)** covering core real-time data processing concepts:

* 🔌 **Sources**: Socket, Rate, File (JSON), Kafka
* 📤 **Sinks**: File (JSON), Foreach (custom writer), Console, Memory, Kafka
* 🔄 **Transformations**: Stateless & Stateful
* ⏱️ **Time-based Windows**: Tumbling, Sliding, Session (fixed & dynamic)
* 💧 **Watermarking**: Late event handling & state cleanup

---

## 📁 Project Structure

```
spark-streaming/
├── build.sbt
├── Datasets/
│   ├── json_files_patients/
│   └── json_files/
└── src/main/scala/ma/ensaf/streaming/
    ├── Main.scala
    ├── utils/
    │   └── Schemas.scala
    ├── sources/
    │   └── DataSources.scala
    ├── sinks/
    │   ├── DataSinks.scala
    │   └── MyWriter.scala
    └── transformations/
        └── Transformations.scala
```

---

## 🧰 Prerequisites

* Java **8 or 11**
* **sbt**
* **Netcat / Ncat** (for socket streaming)
* **Apache Kafka**

---

## ⚙️ Setup & Execution

```bash
git clone https://github.com/Maha-Jr10/spark-structured-streaming.git
cd spark-structured-streaming
sbt compile
```

👉 In `Main.scala`, **uncomment only one exercise**, then run:

```bash
sbt run
```

---

## 🧪 Environment Preparation

| Feature         | Required Setup                                             |
| --------------- | ---------------------------------------------------------- |
| Socket Source   | `nc -lk 9999` (macOS/Linux) or `ncat -l -p 9999` (Windows) |
| Kafka           | Start Zookeeper & Kafka, create topic `Sport-0`            |
| File Source     | Add JSON files to `Datasets/json_files/`                   |
| Transformations | Add patient JSON files to `Datasets/json_files_patients/`  |
| Hospital Socket | Run Netcat on port `5555`                                  |

---

## 🔌 Sources (Section I)

Implemented in `DataSources.scala`:

| Source | Description                                     |
| ------ | ----------------------------------------------- |
| Socket | Real-time word count from TCP stream            |
| Rate   | Synthetic data generator (`timestamp`, `value`) |
| File   | Processes JSON files with event-time windowing  |
| Kafka  | Reads messages and deserializes key/value       |

### Example Output (Socket)

```
+-------+-----+
| value |count|
+-------+-----+
| hello | 1   |
| world | 1   |
+-------+-----+
```

---

## 📤 Sinks (Section II)

Implemented in `DataSinks.scala` and `MyWriter.scala`:

| Sink    | Modes           | Description                      |
| ------- | --------------- | -------------------------------- |
| File    | Append          | Writes JSON with checkpointing   |
| Foreach | All             | Custom logic via `ForeachWriter` |
| Console | All             | Debug output                     |
| Memory  | Append/Complete | Queryable in-memory table        |
| Kafka   | All             | Publishes JSON messages          |

---

## 🔄 Transformations (Section III)

Based on hospital event data:

```json
{"NSS":"1234","Nom":"Maria","DID":10,"DNom":"Cardio","Fecha":"2023-02-23T00:00:00.002Z"}
```

### Stateless

* `fromHospitalSocket` → JSON parsing from socket
* `fromJsonFile` → Aggregation by department

### Stateful

| Function             | Description                  |
| -------------------- | ---------------------------- |
| globalAggregation    | Global count                 |
| groupedAggregation   | Grouped by department        |
| multipleAggregations | Multiple statistical metrics |

---

## ⏱️ Window Operations

| Function                    | Type     | Details               |
| --------------------------- | -------- | --------------------- |
| tumblingWindows             | Tumbling | 10s                   |
| slidingWindows              | Sliding  | 10s window / 5s slide |
| sessionWindows              | Session  | Fixed 10s gap         |
| sessionWindowsDynamic       | Session  | Dynamic gap           |
| sessionWindowsWithWatermark | Session  | Watermark + gap       |

### Example Output

```
+------------------------------------------+-----------+
| window                                   | count     |
+------------------------------------------+-----------+
| [00:00:00, 00:00:10]                     | 2         |
| [00:00:10, 00:00:20]                     | 1         |
+------------------------------------------+-----------+
```

---

## 💾 Output Modes Compatibility

| Sink    | Append | Update | Complete |
| ------- | ------ | ------ | -------- |
| File    | ✔      | ✘      | ✘        |
| Kafka   | ✔      | ✔      | ✔        |
| Console | ✔      | ✔      | ✔        |
| Memory  | ✔      | ✘      | ✘        |
| Foreach | ✔      | ✔      | ✔        |

---

## 🛠️ Build Configuration

```scala
name := "spark-streaming"

scalaVersion := "2.12.15"
val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)
```

---

## 📚 Key Concepts

* Event-time processing & windowing
* Stateful computations (StateStore)
* Watermarking for late data
* Kafka integration (producer & consumer)
* Custom sinks (`ForeachWriter`)
* Streaming SQL via in-memory tables

---

## 👤 Author

**John Muhammed**
