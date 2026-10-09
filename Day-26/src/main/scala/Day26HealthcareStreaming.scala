import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day26HealthcareStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 26 Healthcare Streaming")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Input format: patientId,heartRate
    val readings = lines.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 2) {
        try {
          val patientId = parts(0).trim
          val heartRate = parts(1).trim.toInt

          if (patientId.nonEmpty && heartRate > 0) {
            Some((patientId, heartRate))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Display all valid readings
    readings.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== PATIENT READINGS ==========")
        rdd.collect().foreach {
          case (patientId, heartRate) =>
            println(s"Patient: $patientId | Heart Rate: $heartRate BPM")
        }
      }
    }

    // Flag high readings for demonstration
    val alerts = readings.filter {
      case (_, heartRate) => heartRate > 100
    }

    alerts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== HIGH HEART-RATE ALERTS ==========")
        rdd.collect().foreach {
          case (patientId, heartRate) =>
            println(
              s"ALERT: Patient $patientId has a reading of $heartRate BPM"
            )
        }
      }
    }

    println("Healthcare streaming started.")
    println("Input format: patientId,heartRate")
    println("Example: P101,85")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
