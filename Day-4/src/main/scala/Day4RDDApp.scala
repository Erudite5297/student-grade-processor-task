import org.apache.spark.{SparkConf, SparkContext}

object Day4RDDApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 4 RDD Practice")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 4 RDD Practice ===")

    // --------------------------------------------------
    // 1. RDD FROM TEXT FILE
    // --------------------------------------------------

    val lines = sc.textFile("src/main/resources/transactions.txt")

    println("\n=== Original Transactions ===")
    lines.collect().foreach(println)

    // --------------------------------------------------
    // 2. MAP
    // Convert each line into (customer, amount)
    // --------------------------------------------------

    val transactions = lines.map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toInt)
    }

    println("\n=== Parsed Transactions ===")
    transactions.collect().foreach(println)

    // --------------------------------------------------
    // 3. FILTER
    // Keep transactions >= 2000
    // --------------------------------------------------

    val highValueTransactions = transactions.filter {
      transaction =>
        transaction._2 >= 2000
    }

    println("\n=== High Value Transactions ===")
    highValueTransactions.collect().foreach(println)

    // --------------------------------------------------
    // 4. FLATMAP
    // Split each transaction into words
    // --------------------------------------------------

    val words = lines.flatMap { line =>
      line.split(",")
    }

    println("\n=== FlatMap Result ===")
    words.collect().foreach(println)

    // --------------------------------------------------
    // 5. TOTAL SALES
    // --------------------------------------------------

    val totalSales = transactions
      .map { transaction =>
        transaction._2
      }
      .reduce(_ + _)

    println("\n=== Total Sales ===")
    println("Total Sales: " + totalSales)

    // --------------------------------------------------
    // 6. RDD FROM SCALA COLLECTION
    // --------------------------------------------------

    val customerNames = Seq(
      "Alice",
      "Bob",
      "Charlie",
      "David"
    )

    val customerRDD = sc.parallelize(customerNames)

    println("\n=== RDD From Collection ===")
    customerRDD.collect().foreach(println)

    // --------------------------------------------------
    // 7. PARTITIONS
    // --------------------------------------------------

    println("\n=== Partition Information ===")

    println(
      "Default Parallelism: " +
        sc.defaultParallelism
    )

    println(
      "Transaction RDD Partitions: " +
        transactions.getNumPartitions
    )

    println(
      "Customer RDD Partitions: " +
        customerRDD.getNumPartitions
    )

    // --------------------------------------------------
    // 8. RECORD COUNT
    // --------------------------------------------------

    println("\n=== Record Count ===")
    println("Number of transactions: " + lines.count())

    // --------------------------------------------------
    // 9. LARGE CUSTOMER FILE SCENARIO
    // --------------------------------------------------

    val largeCustomerRDD =
      sc.parallelize(
        Seq(
          "Customer001",
          "Customer002",
          "Customer003",
          "Customer004",
          "Customer005",
          "Customer006",
          "Customer007",
          "Customer008"
        ),
        4
      )

    println("\n=== Large Customer File Simulation ===")

    println(
      "Customer RDD Partitions: " +
        largeCustomerRDD.getNumPartitions
    )

    largeCustomerRDD
      .mapPartitionsWithIndex {
        (partitionId, records) =>
          Iterator(
            "Partition " +
              partitionId +
              ": " +
              records.mkString(", ")
          )
      }
      .collect()
      .foreach(println)

    sc.stop()
  }
}
