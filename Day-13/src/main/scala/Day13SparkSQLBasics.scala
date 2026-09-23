import org.apache.spark.sql.SparkSession

object Day13SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 13 Spark SQL Basics")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Create sample employee data
    val employees = Seq(
      (1, "Erudite", "Data Engineering", 45000),
      (2, "Rahul", "Software Development", 55000),
      (3, "Ankit", "Data Engineering", 50000),
      (4, "Priya", "Testing", 40000),
      (5, "Sneha", "Data Engineering", 60000)
    )

    // Convert the data into a DataFrame
    val employeeDF = employees.toDF(
      "id",
      "name",
      "department",
      "salary"
    )

    println("All Employees:")
    employeeDF.show()

    // Create a temporary SQL view
    employeeDF.createOrReplaceTempView("employees")

    // SQL Query 1: Select employees from Data Engineering
    println("Data Engineering Employees:")

    val dataEngineers = spark.sql(
      """
        SELECT name, salary
        FROM employees
        WHERE department = 'Data Engineering'
      """
    )

    dataEngineers.show()

    // SQL Query 2: Find employees earning more than 50000
    println("Employees earning more than 50000:")

    val highSalaryEmployees = spark.sql(
      """
        SELECT name, department, salary
        FROM employees
        WHERE salary > 50000
      """
    )

    highSalaryEmployees.show()

    // SQL Query 3: Calculate average salary by department
    println("Average Salary by Department:")

    val averageSalary = spark.sql(
      """
        SELECT department, AVG(salary) AS average_salary
        FROM employees
        GROUP BY department
      """
    )

    averageSalary.show()

    spark.stop()
  }
}
