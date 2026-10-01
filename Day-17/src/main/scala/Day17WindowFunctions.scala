import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 17 Window Functions")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Sample employee data
    val employees = Seq(
      (1, "Erudite", "Data Engineering", 45000),
      (2, "Rahul", "Software Development", 55000),
      (3, "Ankit", "Data Engineering", 50000),
      (4, "Priya", "Testing", 40000),
      (5, "Sneha", "Data Engineering", 60000),
      (6, "Ravi", "Software Development", 65000)
    )

    val employeeDF = employees.toDF(
      "id",
      "name",
      "department",
      "salary"
    )

    println("Employee Data:")
    employeeDF.show()

    // Window: divide employees by department
    // and order them by salary from highest to lowest
    val departmentWindow = Window
      .partitionBy("department")
      .orderBy(desc("salary"))

    // Add row number, rank and dense rank
    val resultDF = employeeDF
      .withColumn("row_number", row_number().over(departmentWindow))
      .withColumn("rank", rank().over(departmentWindow))
      .withColumn("dense_rank", dense_rank().over(departmentWindow))

    println("Window Function Results:")
    resultDF.show()

    spark.stop()
  }
}
