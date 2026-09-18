import org.apache.spark.{SparkConf, SparkContext}

object Day9PairRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 9 - Pair RDD")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // Create a Pair RDD: (product, sales)
    val sales = sc.parallelize(
      List(
        ("Laptop", 50000),
        ("Phone", 30000),
        ("Laptop", 45000),
        ("Tablet", 20000),
        ("Phone", 25000),
        ("Laptop", 55000)
      )
    )

    println("Original Sales:")
    sales.collect().foreach(println)

    // Add sales values for each product
    val totalSales = sales.reduceByKey((a, b) => a + b)

    println("\nTotal Sales by Product:")
    totalSales.collect().foreach(println)

    // Sort products by total sales
    val sortedSales = totalSales.sortBy(pair => pair._2, ascending = false)

    println("\nProducts Sorted by Sales:")
    sortedSales.collect().foreach(println)

    // Get only product names
    val products = totalSales.mapValues(_ => "Product")

    println("\nMapValues Example:")
    products.collect().foreach(println)

    // Count number of different products
    val productCount = totalSales.count()

    println(s"\nNumber of Products: $productCount")

    sc.stop()
  }
}
