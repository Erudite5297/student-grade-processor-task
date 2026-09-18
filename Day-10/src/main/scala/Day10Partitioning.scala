import org.apache.spark.{SparkConf, SparkContext}

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 10 - Partitioning")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // Create an RDD
    val numbers = sc.parallelize(1 to 20)

    println("Original Number of Partitions:")
    println(numbers.getNumPartitions)

    // Repartition the RDD into 4 partitions
    val repartitioned = numbers.repartition(4)

    println("\nAfter repartition(4):")
    println(repartitioned.getNumPartitions)

    // Coalesce the RDD into 2 partitions
    val coalesced = repartitioned.coalesce(2)

    println("\nAfter coalesce(2):")
    println(coalesced.getNumPartitions)

    // Show the data in each partition
    println("\nData in Each Partition:")

    coalesced
      .mapPartitionsWithIndex {
        (index, iterator) =>
          Iterator(
            s"Partition $index: ${iterator.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    println("\nPartition Concepts:")
    println("repartition() can increase or decrease partitions and causes a shuffle.")
    println("coalesce() is mainly used to reduce partitions with less shuffling.")
    println("Each partition can be processed by a Spark task.")

    sc.stop()
  }
}
