import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day24StatefulStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 24 Stateful Streaming")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Required for maintaining state
    streamingContext.checkpoint("checkpoint")

    // Connect to Netcat
    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Split lines into words
    val words = lines.flatMap(_.split("\\s+"))

    // Convert words into (word, 1)
    val pairs = words.map(word => (word, 1))

    // Stateful function
    val updateFunction = (
      newValues: Seq[Int],
      runningCount: Option[Int]
    ) => {

      val newCount = newValues.sum
      val previousCount = runningCount.getOrElse(0)

      Some(newCount + previousCount)
    }

    // Maintain running count across batches
    val runningCounts =
      pairs.updateStateByKey[Int](updateFunction)

    // Print running counts
    runningCounts.print()

    println("===================================")
    println("Stateful Streaming Started")
    println("Waiting for data on 127.0.0.1:9999")
    println("===================================")

    streamingContext.start()

    streamingContext.awaitTermination()
  }
}
