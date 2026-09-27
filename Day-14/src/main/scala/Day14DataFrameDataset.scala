import org.apache.spark.sql.{SparkSession, Encoders}

case class Employee(
  id: Int,
  name: String,
  department: String,
  salary: Int
)

object Day14DataFrameDataset {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 14 DataFrame and Dataset")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Sample employee data
    val employees = Seq(
      Employee(1, "Erudite", "Data Engineering", 45000),
      Employee(2, "Rahul", "Software Development", 55000),
      Employee(3, "Ankit", "Data Engineering", 50000),
      Employee(4, "Priya", "Testing", 40000),
      Employee(5, "Sneha", "Data Engineering", 60000)
    )

    // Create a Dataset
    val employeeDS = employees.toDS()

    println("Employee Dataset:")
    employeeDS.show()

    // Select specific columns
    println("Employee Names and Salaries:")
    employeeDS
      .select("name", "salary")
      .show()

    // Filter employees
    println("Employees with salary greater than 50000:")
    employeeDS
      .filter($"salary" > 50000)
      .show()

    // Convert Dataset to DataFrame
    val employeeDF = employeeDS.toDF()

    println("DataFrame:")
    employeeDF.show()

    spark.stop()
  }
}
