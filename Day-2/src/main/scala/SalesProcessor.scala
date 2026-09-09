object SalesProcessor {

  def main(args: Array[String]): Unit = {

    // ===============================
    // DAY 2 - Scala Collections Practice
    // ===============================

    // 1. List - map, filter, flatMap, reduce

    val sales = List(
      ("Laptop", 2, 55000),
      ("Mouse", 5, 800),
      ("Keyboard", 3, 1500),
      ("Laptop", 1, 55000),
      ("Monitor", 2, 12000)
    )

    // map - calculate revenue for each sale
    val saleTotals = sales.map {
      case (product, quantity, price) =>
        (product, quantity, quantity * price)
    }

    println("\n--- Sale Totals ---")
    saleTotals.foreach(println)

    // filter - select high-value sales
    val highValueSales = saleTotals.filter {
      case (_, _, total) => total > 5000
    }

    println("\n--- High Value Sales ---")
    highValueSales.foreach(println)

    // flatMap - create individual product entries
    val productEntries = sales.flatMap {
      case (product, quantity, _) =>
        List.fill(quantity)(product)
    }

    println("\n--- Product Entries ---")
    println(productEntries)

    // reduce - calculate total sales
    val totalSales = saleTotals.map(_._3).reduce(_ + _)

    println("\n--- Total Sales ---")
    println(totalSales)


    // 2. Vector - indexed customer records

    val customers = Vector(
      (1, "Rahul"),
      (2, "Priya"),
      (3, "Arjun"),
      (4, "Sneha")
    )

    println("\n--- Customers ---")
    customers.foreach(println)

    println("Customer at index 2: " + customers(2))


    // 3. Map - product quantities and prices

    val productQuantities = Map(
      "Laptop" -> 3,
      "Mouse" -> 5,
      "Keyboard" -> 3,
      "Monitor" -> 2
    )

    val productPrices = Map(
      "Laptop" -> 55000,
      "Mouse" -> 800,
      "Keyboard" -> 1500,
      "Monitor" -> 12000
    )

    val productValues = productQuantities.map {
      case (product, quantity) =>
        val price = productPrices.getOrElse(product, 0)
        (product, quantity * price)
    }

    println("\n--- Product Values ---")
    productValues.foreach(println)


    // 4. For-comprehension - combine customers and orders

    val orders = List(
      (1, "Laptop"),
      (2, "Mouse"),
      (3, "Keyboard"),
      (1, "Monitor")
    )

    val customerOrders =
      for {
        customer <- customers
        order <- orders
        if customer._1 == order._1
      } yield (customer._2, order._2)

    println("\n--- Customer Orders ---")
    customerOrders.foreach(println)


    // 5. Daily Sales Summary

    val dailySummary = saleTotals
      .groupBy(_._1)
      .map {
        case (product, records) =>
          val quantity = records.map(_._2).sum
          val revenue = records.map(_._3).sum
          (product, quantity, revenue)
      }

    println("\n--- Daily Sales Summary ---")
    dailySummary.foreach(println)

    println("\nDay 2 Scala Collections Practice Completed!")
  }
}
