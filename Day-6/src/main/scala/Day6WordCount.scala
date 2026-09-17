import org.apache.spark.{SparkConf, SparkContext}

object Day6WordCount {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Configuration
    // ------------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day 6 - Word Count")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)


    // ------------------------------------------------------------
    // Sample Text
    // ------------------------------------------------------------

    val lines = sc.parallelize(
      List(
        "Spark is fast and powerful",
        "Scala is powerful and easy",
        "Spark makes big data processing easy",
        "Scala and Spark are popular",
        "spark is fast"
      )
    )


    // ------------------------------------------------------------
    // 1. flatMap
    // Convert lines into individual words
    // ------------------------------------------------------------

    val words = lines.flatMap(line => line.split("\\s+"))


    // ------------------------------------------------------------
    // 2. Clean the words
    // Convert to lowercase
    // Remove punctuation
    // Ignore empty words
    // ------------------------------------------------------------

    val cleanedWords = words
      .map(word => word.toLowerCase.replaceAll("[^a-z0-9]", ""))
      .filter(word => word.nonEmpty)


    // ------------------------------------------------------------
    // 3. map
    // Convert each word into (word, 1)
    // ------------------------------------------------------------

    val wordPairs = cleanedWords.map(word => (word, 1))


    // ------------------------------------------------------------
    // 4. reduceByKey
    // Count how many times each word appears
    // ------------------------------------------------------------

    val wordCounts = wordPairs.reduceByKey((a, b) => a + b)


    // ------------------------------------------------------------
    // Display Word Counts
    // ------------------------------------------------------------

    println("Word Counts:")

    wordCounts
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }


    // ------------------------------------------------------------
    // 5. Top 10 Most Frequent Words
    // ------------------------------------------------------------

    val top10 = wordCounts
      .sortBy(pair => pair._2, ascending = false)
      .take(10)

    println("\nTop 10 Most Frequent Words:")

    top10.foreach {
      case (word, count) =>
        println(s"$word -> $count")
    }


    // ------------------------------------------------------------
    // Stop Spark
    // ------------------------------------------------------------

    sc.stop()
  }
}
