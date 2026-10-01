import org.apache.spark.sql.SparkSession

object Day18Joins {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 18 Joins")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Employee DataFrame
    val employees = Seq(
      (1, "Erudite", "Data Engineering"),
      (2, "Rahul", "Software Development"),
      (3, "Ankit", "Testing"),
      (4, "Priya", "Data Engineering")
    ).toDF("employee_id", "name", "department")

    // Salary DataFrame
    val salaries = Seq(
      (1, 45000),
      (2, 55000),
      (3, 40000),
      (5, 60000)
    ).toDF("employee_id", "salary")

    println("Employees:")
    employees.show()

    println("Salaries:")
    salaries.show()

    // INNER JOIN
    println("Inner Join:")
    val innerJoin = employees.join(
      salaries,
      employees("employee_id") === salaries("employee_id"),
      "inner"
    )

    innerJoin.show()

    // LEFT JOIN
    println("Left Join:")
    val leftJoin = employees.join(
      salaries,
      employees("employee_id") === salaries("employee_id"),
      "left"
    )

    leftJoin.show()

    spark.stop()
  }
}
