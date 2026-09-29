import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Day15UDF {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 15 UDF")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Sample employee data
    val employees = Seq(
      (1, "Erudite", 45000),
      (2, "Rahul", 55000),
      (3, "Ankit", 50000),
      (4, "Priya", 40000),
      (5, "Sneha", 60000)
    )

    val employeeDF = employees.toDF("id", "name", "salary")

    println("Original Data:")
    employeeDF.show()

    // Create a UDF to classify salary
    val salaryCategory = udf((salary: Int) => {
      if (salary >= 50000) {
        "High Salary"
      } else {
        "Normal Salary"
      }
    })

    // Apply the UDF
    val resultDF = employeeDF.withColumn(
      "salary_category",
      salaryCategory($"salary")
    )

    println("Data after applying UDF:")
    resultDF.show()

    spark.stop()
  }
}
