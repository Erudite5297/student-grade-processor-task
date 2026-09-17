import org.apache.spark.{SparkConf, SparkContext}

object Day7LineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // Spark Configuration
    // ------------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day 7 - Lineage and Fault Tolerance")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)


    // ------------------------------------------------------------
    // 1. Scala Immutability
    // ------------------------------------------------------------

    val numbers = List(1, 2, 3, 4, 5)

    // map creates a new collection.
    // The original collection is not changed.

    val doubledNumbers = numbers.map(x => x * 2)

    println("Original List:")
    println(numbers.mkString(", "))

    println("\nNew List after map:")
    println(doubledNumbers.mkString(", "))


    // ------------------------------------------------------------
    // 2. RDD Lineage
    // ------------------------------------------------------------

    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5))

    val evenNumbers = numbersRDD.filter(x => x % 2 == 0)

    val squaredNumbers = evenNumbers.map(x => x * x)

    println("\nRDD Result:")
    println(squaredNumbers.collect().mkString(", "))


    // ------------------------------------------------------------
    // 3. Display RDD Lineage
    // ------------------------------------------------------------

    println("\nRDD Lineage:")
    println(squaredNumbers.toDebugString)


    // ------------------------------------------------------------
    // 4. Fault Tolerance Concept
    // ------------------------------------------------------------

    println("\nFault Tolerance:")
    println("Spark stores RDD lineage instead of requiring every intermediate result.")
    println("If a partition is lost, Spark can recompute it using the lineage.")


    // ------------------------------------------------------------
    // 5. More Transformations
    // ------------------------------------------------------------

    val data = sc.parallelize(
      List(10, 20, 30, 40, 50)
    )

    val step1 = data.map(x => x + 5)
    val step2 = step1.filter(x => x > 30)
    val step3 = step2.map(x => x * 2)

    println("\nTransformation Chain:")
    println(step3.collect().mkString(", "))

    println("\nTransformation Chain Lineage:")
    println(step3.toDebugString)


    // ------------------------------------------------------------
    // Stop Spark
    // ------------------------------------------------------------

    sc.stop()
  }
}
