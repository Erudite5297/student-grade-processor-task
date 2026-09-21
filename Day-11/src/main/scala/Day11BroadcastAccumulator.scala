import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 11 Broadcast and Accumulator")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // Broadcast variable:
    // Shared read-only data sent efficiently to all worker nodes
    val productPrices = Map(
      "Laptop" -> 50000,
      "Phone" -> 20000,
      "Tablet" -> 15000
    )

    val broadcastPrices = sc.broadcast(productPrices)

    // Accumulator:
    // Used to count invalid or unknown products
    val invalidProductCount = sc.longAccumulator("Invalid Product Count")

    val products = sc.parallelize(
      List("Laptop", "Phone", "Watch", "Tablet", "Camera")
    )

    val productDetails = products.map { product =>

      if (broadcastPrices.value.contains(product)) {

        val price = broadcastPrices.value(product)
        s"$product costs ₹$price"

      } else {

        invalidProductCount.add(1)
        s"$product is not available"

      }
    }

    // Action triggers Spark execution
    productDetails.collect().foreach(println)

    println()
    println(s"Invalid Products: ${invalidProductCount.value}")

    // Release broadcast variable
    broadcastPrices.destroy()

    sc.stop()
  }
}
