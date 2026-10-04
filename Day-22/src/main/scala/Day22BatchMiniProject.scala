import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day22BatchMiniProject {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 22 Batch Mini Project")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Employee data
    val employees = Seq(
      (1, "Erudite", "Data Engineering", 45000),
      (2, "Rahul", "Software Development", 55000),
      (3, "Ankit", "Data Engineering", 50000),
      (4, "Priya", "Testing", 40000),
      (5, "Sneha", "Data Engineering", 60000),
      (6, "Ravi", "Software Development", 65000),
      (7, "Aman", "Testing", 42000),
      (8, "Neha", "Data Engineering", 70000)
    ).toDF(
      "id",
      "name",
      "department",
      "salary"
    )

    println("========== EMPLOYEE DATA ==========")
    employees.show()

    // Department-wise salary statistics
    val departmentStats = employees
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("average_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_salary")
      )
      .orderBy(desc("average_salary"))

    println("========== DEPARTMENT STATISTICS ==========")
    departmentStats.show()

    // High salary employees
    val highSalaryEmployees = employees
      .filter($"salary" >= 60000)
      .orderBy(desc("salary"))

    println("========== HIGH SALARY EMPLOYEES ==========")
    highSalaryEmployees.show()

    // Create temporary view
    employees.createOrReplaceTempView("employees")

    // Spark SQL query
    val sqlResult = spark.sql("""
      SELECT department, COUNT(*) AS total_employees,
             ROUND(AVG(salary), 2) AS average_salary
      FROM employees
      GROUP BY department
      ORDER BY average_salary DESC
    """)

    println("========== SPARK SQL RESULT ==========")
    sqlResult.show()

    // Save department statistics
    departmentStats.write
      .mode("overwrite")
      .parquet("output/department_statistics")

    // Save high salary employees
    highSalaryEmployees.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/high_salary_employees")

    println("========== OUTPUT FILES CREATED ==========")
    println("output/department_statistics")
    println("output/high_salary_employees")

    spark.stop()
  }
}
