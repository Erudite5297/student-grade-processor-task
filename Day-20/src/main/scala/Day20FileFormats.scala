import org.apache.spark.sql.SparkSession

object Day20FileFormats {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 20 File Formats and Output")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Create sample data
    val employees = Seq(
      (1, "Erudite", "Data Engineering", 45000),
      (2, "Rahul", "Software Development", 55000),
      (3, "Ankit", "Data Engineering", 50000),
      (4, "Priya", "Testing", 40000),
      (5, "Sneha", "Data Engineering", 60000)
    ).toDF(
      "id",
      "name",
      "department",
      "salary"
    )

    println("Original Data:")
    employees.show()

    // Write data as CSV
    employees.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/employees_csv")

    println("CSV file written successfully.")

    // Write data as JSON
    employees.write
      .mode("overwrite")
      .json("output/employees_json")

    println("JSON file written successfully.")

    // Write data as Parquet
    employees.write
      .mode("overwrite")
      .parquet("output/employees_parquet")

    println("Parquet file written successfully.")

    // Read the Parquet file
    val parquetData = spark.read
      .parquet("output/employees_parquet")

    println("Data read from Parquet:")
    parquetData.show()

    spark.stop()
  }
}
