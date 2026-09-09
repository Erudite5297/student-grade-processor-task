trait Logger {
  def log(message: String): Unit
}

class ConsoleLogger extends Logger {
  def log(message: String): Unit = {
    println(s"[LOG] $message")
  }
}

class FileLogger extends Logger {
  def log(message: String): Unit = {
    println(s"[FILE LOG] $message")
  }
}

def calculateGrade(mark: Int): String = {
  if (mark >= 90) "A"
  else if (mark >= 80) "B"
  else if (mark >= 70) "C"
  else if (mark >= 60) "D"
  else "F"
}

object StudentGradeProcessor {

  def main(args: Array[String]): Unit = {

    // ===============================
    // DAY 1 - Scala Essentials
    // ===============================

    // val, var, lazy val

    val studentName = "Alice"
    var marks = 85

    println(s"Student: $studentName")
    println(s"Marks: $marks")

    marks = 90

    println(s"Updated Marks: $marks")

    lazy val message = {
      println("Creating message...")
      "Welcome to Scala!"
    }

    println("Before using message")
    println(message)

    // Immutable collections

    val students = List("Alice", "Bob", "Charlie", "David")

    val marksList = List(85, 72, 91, 64)

    val marksVector = Vector(85, 72, 91, 64)

    val subjects = Set("Scala", "Spark", "SQL", "Scala")

    val studentMarks = Map(
      "Alice" -> 85,
      "Bob" -> 72,
      "Charlie" -> 91,
      "David" -> 64
    )

    println(s"Students: $students")
    println(s"Marks List: $marksList")
    println(s"Marks Vector: $marksVector")
    println(s"Subjects: $subjects")
    println(s"Student Marks: $studentMarks")

    // map

    val upperCaseStudents =
      students.map(student => student.toUpperCase)

    println(s"Uppercase Students: $upperCaseStudents")

    // filter

    val passingMarks =
      marksList.filter(mark => mark >= 70)

    println(s"Passing Marks: $passingMarks")

    // foreach

    println("Students:")

    students.foreach(student => println(student))

    // for-comprehension with yield

    val studentNames = for {
      student <- students
    } yield student.toUpperCase

    println(s"Student Names using for-yield: $studentNames")

    val updatedMarks = for {
      mark <- marksList
    } yield mark + 5

    println(s"Updated Marks using for-yield: $updatedMarks")

    // Student grade processing

    val studentGrades = for {
      (student, mark) <- studentMarks
    } yield {
      (student, mark, calculateGrade(mark))
    }

    println("Student Grades:")

    studentGrades.foreach {
      case (student, mark, grade) =>
        println(s"$student -> $mark -> Grade: $grade")
    }

    // Logger implementations

    val consoleLogger = new ConsoleLogger()
    consoleLogger.log("Student processing started")

    val fileLogger = new FileLogger()
    fileLogger.log("Student processing completed")
  }
}
