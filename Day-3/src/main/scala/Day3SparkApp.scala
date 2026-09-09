import org.apache.spark.sql.SparkSession

object Day3SparkApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 3 Spark First Application")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("=== Spark Application Started ===")
    println(s"Application Name: ${sc.appName}")
    println(s"Master: ${sc.master}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")

    // Read the text file using Spark
    val inputPath = "src/main/resources/input.txt"
    val lines = sc.textFile(inputPath)

    println("=== File Contents ===")
    lines.collect().foreach(println)

    spark.stop()
  }
}
