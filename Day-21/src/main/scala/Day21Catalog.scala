import org.apache.spark.sql.SparkSession

object Day21Catalog {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 21 Spark Catalog")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Create employee data
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

    println("Employee Data:")
    employees.show()

    // Create temporary view
    employees.createOrReplaceTempView("employees")

    println("Tables in Catalog:")
    spark.catalog.listTables().show(false)

    // Check whether table exists
    println("Does employees table exist?")
    println(spark.catalog.tableExists("employees"))

    // Query using SQL
    println("Data Engineering Employees:")

    spark.sql("""
      SELECT name, salary
      FROM employees
      WHERE department = 'Data Engineering'
    """).show()

    // List columns
    println("Employee Table Columns:")
    spark.catalog.listColumns("employees").show(false)

    spark.stop()
  }
}
