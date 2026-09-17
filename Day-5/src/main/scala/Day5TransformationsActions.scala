import org.apache.spark.{SparkConf, SparkContext}

object Day5TransformationsActions {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 5 - Transformations and Actions")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // ------------------------------------------------------------
    // 1. Create an RDD
    // ------------------------------------------------------------

    val numbers = sc.parallelize(List(1, 2, 3, 4, 5))

    println("Original RDD:")
    println(numbers.collect().mkString(", "))


    // ------------------------------------------------------------
    // 2. map
    // Transformation: multiplies every number by 2
    // ------------------------------------------------------------

    val mapped = numbers.map(x => x * 2)

    println("\nmap:")
    println(mapped.collect().mkString(", "))


    // ------------------------------------------------------------
    // 3. filter
    // Transformation: keeps only even numbers
    // ------------------------------------------------------------

    val filtered = numbers.filter(x => x % 2 == 0)

    println("\nfilter:")
    println(filtered.collect().mkString(", "))


    // ------------------------------------------------------------
    // 4. flatMap
    // Transformation: splits sentences into words
    // ------------------------------------------------------------

    val sentences = sc.parallelize(
      List(
        "Spark is fast",
        "Scala is powerful"
      )
    )

    val words = sentences.flatMap(line => line.split(" "))

    println("\nflatMap:")
    println(words.collect().mkString(", "))


    // ------------------------------------------------------------
    // 5. distinct
    // Transformation: removes duplicate values
    // ------------------------------------------------------------

    val duplicateNumbers =
      sc.parallelize(List(1, 2, 2, 3, 3, 4, 4, 5))

    val distinctNumbers = duplicateNumbers.distinct()

    println("\ndistinct:")
    println(distinctNumbers.collect().sorted.mkString(", "))


    // ------------------------------------------------------------
    // 6. union
    // Transformation: combines two RDDs
    // ------------------------------------------------------------

    val rdd1 = sc.parallelize(List(1, 2, 3))
    val rdd2 = sc.parallelize(List(4, 5, 6))

    val combined = rdd1.union(rdd2)

    println("\nunion:")
    println(combined.collect().mkString(", "))


    // ------------------------------------------------------------
    // ACTIONS
    // ------------------------------------------------------------

    // count
    println("\ncount:")
    println(numbers.count())


    // collect
    println("\ncollect:")
    println(numbers.collect().mkString(", "))


    // first
    println("\nfirst:")
    println(numbers.first())


    // take
    println("\ntake:")
    println(numbers.take(3).mkString(", "))


    // reduce
    val sum = numbers.reduce((a, b) => a + b)

    println("\nreduce:")
    println(sum)


    // ------------------------------------------------------------
    // 7. LOG ANALYZER
    // Scenario: Count ERROR messages
    // ------------------------------------------------------------

    val logs = sc.parallelize(
      List(
        "INFO Application started",
        "ERROR Database connection failed",
        "INFO User logged in",
        "ERROR File not found",
        "WARNING Low memory",
        "ERROR Network timeout",
        "INFO Application stopped"
      )
    )

    val errorLogs = logs.filter(line => line.contains("ERROR"))

    println("\nERROR logs:")
    errorLogs.collect().foreach(println)

    println("\nTotal ERROR messages:")
    println(errorLogs.count())


    // ------------------------------------------------------------
    // Stop Spark
    // ------------------------------------------------------------

    sc.stop()
  }
}
