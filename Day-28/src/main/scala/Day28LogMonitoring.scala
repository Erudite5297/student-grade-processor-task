import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day28LogMonitoring {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 28 Real-Time Log Monitoring")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Receive live log lines from a local socket
    val logs = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Display all incoming logs
    logs.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== INCOMING LOGS ==========")
        rdd.collect().foreach(println)
      }
    }

    // Detect ERROR logs
    val errors = logs.filter { line =>
      line.contains("ERROR")
    }

    errors.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== ERROR ALERTS ==========")

        rdd.collect().foreach { line =>
          println(s"ALERT: $line")
        }

        println(s"Total errors in this batch: ${rdd.count()}")
      }
    }

    // Detect WARN logs
    val warnings = logs.filter { line =>
      line.contains("WARN")
    }

    warnings.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== WARNING ALERTS ==========")

        rdd.collect().foreach { line =>
          println(s"WARNING: $line")
        }

        println(s"Total warnings in this batch: ${rdd.count()}")
      }
    }

    println("Real-time log monitoring started.")
    println("Enter application logs in the Netcat terminal.")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
