import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day27BankingStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 27 Banking Transaction Streaming")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val streamingContext =
      new StreamingContext(conf, Seconds(5))

    // Receive transactions from a local socket
    val lines = streamingContext.socketTextStream(
      "127.0.0.1",
      9999
    )

    // Input format: transactionId,accountId,amount
    val transactions = lines.flatMap { line =>
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
            !amount.isInfinity &&
            !amount.isNaN
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

    // Display every valid transaction
    transactions.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== BANK TRANSACTIONS ==========")

        rdd.collect().foreach {
          case (transactionId, accountId, amount) =>
            println(
              f"Transaction: $transactionId | Account: $accountId | Amount: ₹$amount%.2f"
            )
        }
      }
    }

    // Flag transactions above ₹50,000
    val suspiciousTransactions = transactions.filter {
      case (_, _, amount) => amount > 50000
    }

    suspiciousTransactions.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n========== TRANSACTION ALERTS ==========")

        rdd.collect().foreach {
          case (transactionId, accountId, amount) =>
            println(
              f"ALERT: Transaction $transactionId | Account: $accountId | Amount: ₹$amount%.2f"
            )
        }
      }
    }

    println("Banking transaction streaming started.")
    println("Input format: transactionId,accountId,amount")
    println("Demo alert threshold: amount > ₹50,000")

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
