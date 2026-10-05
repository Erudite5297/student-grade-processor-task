import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreams {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 23 Spark DStreams")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Show the lines received by Spark
    lines.foreachRDD { rdd =>
      println("========== NEW BATCH ==========")

      if (rdd.isEmpty()) {
        println("No data received in this batch.")
      } else {
        println("Data received:")
        rdd.collect().foreach(println)
      }
    }

    // Split lines into words
    val words = lines.flatMap(_.split("\\s+"))

    // Convert words into (word, 1)
    val pairs = words.map(word => (word, 1))

    // Count words
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print word counts
    wordCounts.print()

    println("Spark Streaming started...")
    println("Waiting for data on 127.0.0.1:9999")

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
