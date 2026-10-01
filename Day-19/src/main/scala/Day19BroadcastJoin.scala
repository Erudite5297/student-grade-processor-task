import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day19BroadcastJoin {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Day 19 Broadcast Join")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    import spark.implicits._

    // Large employee DataFrame
    val employees = Seq(
      (1, "Erudite", 101, 45000),
      (2, "Rahul", 102, 55000),
      (3, "Ankit", 101, 50000),
      (4, "Priya", 103, 40000),
      (5, "Sneha", 101, 60000)
    ).toDF(
      "employee_id",
      "name",
      "department_id",
      "salary"
    )

    // Small department DataFrame
    val departments = Seq(
      (101, "Data Engineering"),
      (102, "Software Development"),
      (103, "Testing")
    ).toDF(
      "department_id",
      "department_name"
    )

    println("Employees:")
    employees.show()

    println("Departments:")
    departments.show()

    // Broadcast the small DataFrame
    val result = employees.join(
      broadcast(departments),
      employees("department_id") === departments("department_id"),
      "inner"
    )

    println("Broadcast Join Result:")

    result
      .select(
        employees("employee_id"),
        employees("name"),
        departments("department_name"),
        employees("salary")
      )
      .show()

    spark.stop()
  }
}
