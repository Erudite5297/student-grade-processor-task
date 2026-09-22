import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 12 Cache and Persist")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // Create an RDD
    val numbers = sc.parallelize(1 to 10)

    // Apply transformations
    val processedNumbers = numbers
      .map(x => x * 2)
      .filter(x => x > 10)

    // Cache the RDD in memory
    processedNumbers.cache()

    println("First Action - Count:")
    println(s"Count: ${processedNumbers.count()}")

    println()
    println("Second Action - Collect:")
    processedNumbers.collect().foreach(println)

    // Display storage level
    println()
    println(s"Storage Level: ${processedNumbers.getStorageLevel}")

    // Remove the RDD from cache
    processedNumbers.unpersist()

    println()
    println("RDD removed from cache.")

    sc.stop()
  }
}
