import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day30RealtimePipeline {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 30 End-to-End Real-Time Pipeline")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Input format: transactionId,accountId,amount
    val input = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Stage 1: Parse and validate transactions
    val transactions = input.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 3) {
        try {
          val transactionId = parts(0).trim
          val accountId = parts(1).trim
          val amount = parts(2).trim.toDouble

          if (
            transactionId.nonEmpty &&
            accountId.nonEmpty &&
            amount >= 0 &&
            !amount.isNaN &&
            !amount.isInfinity
          ) {
            Some((transactionId, accountId, amount))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Stage 2: Process every batch and calculate statistics
    transactions.foreachRDD { rdd =>
      val batch = rdd.collect()

      if (batch.nonEmpty) {
        val totalAmount = batch.map(_._3).sum
        val transactionCount = batch.length

        println("\n========== BATCH SUMMARY ==========")
        println(s"Transactions processed: $transactionCount")
        println(f"Total transaction value: ₹$totalAmount%.2f")
        println(
          f"Average transaction value: ₹${totalAmount / transactionCount}%.2f"
        )

        println("\n---------- VALID TRANSACTIONS ----------")

        batch.foreach {
          case (transactionId, accountId, amount) =>
            println(
              f"$transactionId | Account: $accountId | Amount: ₹$amount%.2f"
            )
        }
      }
    }

    // Stage 3: Flag high-value transactions
    val alerts = transactions.filter {
      case (_, _, amount) => amount > 50000
    }

    alerts.foreachRDD { rdd =>
      val batch = rdd.collect()

      if (batch.nonEmpty) {
        println("\n========== HIGH-VALUE ALERTS ==========")

        batch.foreach {
          case (transactionId, accountId, amount) =>
            println(
              f"ALERT: $transactionId | Account: $accountId | Amount: ₹$amount%.2f"
            )
        }

        println(s"High-value transactions in batch: ${batch.length}")
      }
    }

    println("\nReal-time transaction pipeline started.")
    println("Input format: transactionId,accountId,amount")
    println("Alert threshold: transaction amount > ₹50,000")
    println("Batch interval: 5 seconds")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
