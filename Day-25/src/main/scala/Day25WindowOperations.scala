import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day25WindowOperations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 25 Window Operations")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    val words = lines.flatMap(_.split("\\s+"))

    val pairs = words.map(word => (word, 1))

    // 10-second window, sliding every 5 seconds
    val windowedCounts =
      pairs.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(10),
        Seconds(5)
      )

    windowedCounts.print()

    println("===================================")
    println("Window Streaming Started")
    println("Window Duration: 10 seconds")
    println("Sliding Interval: 5 seconds")
    println("Waiting for data on 127.0.0.1:9999")
    println("===================================")

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
