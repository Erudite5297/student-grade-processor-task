import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day29IoTSensorStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 29 IoT Sensor Streaming")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Input format: sensorId,temperature,humidity
    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Parse and validate sensor readings
    val readings = lines.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 3) {
        try {
          val sensorId = parts(0).trim
          val temperature = parts(1).trim.toDouble
          val humidity = parts(2).trim.toDouble

          if (
            sensorId.nonEmpty &&
            !temperature.isNaN &&
            !temperature.isInfinity &&
            !humidity.isNaN &&
            !humidity.isInfinity &&
            humidity >= 0 &&
            humidity <= 100
          ) {
            Some((sensorId, temperature, humidity))
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

    // Display all valid sensor readings
    readings.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== IOT SENSOR READINGS ==========")

        rdd.collect().foreach {
          case (sensorId, temperature, humidity) =>
            println(
              f"Sensor: $sensorId | Temperature: $temperature%.1f C | Humidity: $humidity%.1f%%"
            )
        }
      }
    }

    // Alert when temperature exceeds 40 C
    val temperatureAlerts = readings.filter {
      case (_, temperature, _) => temperature > 40
    }

    temperatureAlerts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== HIGH TEMPERATURE ALERTS ==========")

        rdd.collect().foreach {
          case (sensorId, temperature, _) =>
            println(
              f"ALERT: Sensor $sensorId reports $temperature%.1f C"
            )
        }
      }
    }

    // Alert when humidity exceeds 80%
    val humidityAlerts = readings.filter {
      case (_, _, humidity) => humidity > 80
    }

    humidityAlerts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== HIGH HUMIDITY ALERTS ==========")

        rdd.collect().foreach {
          case (sensorId, _, humidity) =>
            println(
              f"ALERT: Sensor $sensorId reports $humidity%.1f%% humidity"
            )
        }
      }
    }

    println("IoT sensor streaming started.")
    println("Input format: sensorId,temperature,humidity")
    println("Temperature alert threshold: > 40 C")
    println("Humidity alert threshold: > 80%")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
