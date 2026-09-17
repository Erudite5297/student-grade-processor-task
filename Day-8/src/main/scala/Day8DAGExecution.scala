import org.apache.spark.{SparkConf, SparkContext}

object Day8DAGExecution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 8 - DAG and Spark Execution")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    val sc = new SparkContext(conf)

    // Create an RDD
    val numbers = sc.parallelize(List(1, 2, 3, 4, 5, 6))

    // Transformation 1
    val doubled = numbers.map(x => x * 2)

    // Transformation 2
    val evenNumbers = doubled.filter(x => x % 2 == 0)

    // Transformation 3
    val pairs = evenNumbers.map(x => (x, x * 10))

    println("DAG Lineage:")
    println(pairs.toDebugString)

    // Action
    val result = pairs.collect()

    println("\nFinal Result:")
    result.foreach {
      case (number, value) =>
        println(s"$number -> $value")
    }

    // Another action
    val total = pairs.map(_._2).reduce(_ + _)

    println(s"\nTotal: $total")

    println("\nExecution Concepts:")
    println("Transformations are lazy.")
    println("Actions trigger Spark execution.")
    println("Spark creates a DAG from the transformations.")
    println("The DAG is divided into stages and tasks.")
    println("Shuffle operations create stage boundaries.")

    sc.stop()
  }
}
