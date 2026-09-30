import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 16 Aggregations")
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

    // Group employees by department
    println("Department-wise Aggregations:")

    val departmentStats = employeeDF
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("average_salary"),
        sum("salary").alias("total_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_salary")
      )

    departmentStats.show()

    spark.stop()
  }
}
