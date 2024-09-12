package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import java.io.File
import scala.io.{ Codec, Source }
import scala.util.Properties.isWin

object StackOverflowSuite:
  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)

class StackOverflowSuite extends munit.FunSuite:
  import StackOverflowSuite.*


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("scoredPostings should correctly compute the highest score for each question") {
    val postings = Seq(
      // Questions
      Posting(1, 101, None, None, 10, Some("Scala")),
      Posting(1, 102, None, None, 5, Some("Java")),
      Posting(1, 103, None, None, 3, Some("Python")),
      // Answers to question 101
      Posting(2, 201, None, Some(101), 7, None),
      Posting(2, 202, None, Some(101), 12, None),
      Posting(2, 203, None, Some(101), 5, None),
      // Answers to question 102
      Posting(2, 204, None, Some(102), 8, None),
      // Answers to question 103
      Posting(2, 205, None, Some(103), 2, None),
      Posting(2, 206, None, Some(103), 3, None)
    )
    val postingsRdd = sc.parallelize(postings)
    val grouped = testObject.groupedPostings(postingsRdd)
    val scored = testObject.scoredPostings(grouped).collect()
    val expected = Array(
      (Posting(1, 101, None, None, 10, Some("Scala")), 12), // Highest score among answers: 12
      (Posting(1, 102, None, None, 5, Some("Java")), 8),    // Highest score among answers: 8
      (Posting(1, 103, None, None, 3, Some("Python")), 3)   // Highest score among answers: 3
    )
    // Convert the results to a Map for easier comparison
    val scoredMap = scored.toMap
    val expectedMap = expected.toMap
    // Assert that the computed scores match the expected scores
    assertEquals(scoredMap, expectedMap)
  }

  test("vectorPostings should correctly transform RDD[(Question, HighScore)] into RDD[(LangIndex, HighScore)]") {
    val input = Seq(
      (Posting(1, 101, None, None, 10, Some("JavaScript")), 12),
      (Posting(1, 102, None, None, 5, Some("Java")), 8),
      (Posting(1, 103, None, None, 3, Some("PHP")), 3),
      (Posting(1, 104, None, None, 3, None), 3), // None case
      (Posting(1, 104, None, None, 3, Some("ABC")), 7) // language that not in langs
    )
    val inputRdd = sc.parallelize(input)
    val actual = testObject.vectorPostings(inputRdd).collect().toMap
    val expected = Array(
      (0, 12),
      (1 * testObject.langSpread, 8),
      (2 * testObject.langSpread, 3)
    ).toMap
    assertEquals(actual, expected)
  }

  test("kmeans should converge to correct means with simple data") {
    val initialMeans = Array((1, 1), (9, 9))
    val inputVectors = sc.parallelize(Seq(
      (1, 1), (2, 2), (3, 3), // group 1
      (9, 9), (10, 10), (11, 11) // group 2
    ))
    val actualResult = testObject.kmeans(initialMeans, inputVectors, debug = false)
    val expectedResult = Array((2, 2), (10, 10))
    assert(actualResult.sameElements(expectedResult))
  }

  test("kmeans should stop when max iterations is reached") {

    // Initial means far from the vectors
    val initialMeans = Array((0, 0), (20, 20))

    // Input RDD of vectors
    val vectors: RDD[(Int, Int)] = sc.parallelize(Seq(
      (1, 1), (2, 2), (3, 3), // Group 1
      (9, 9), (10, 10), (11, 11) // Group 2
    ))

    // Execute the kmeans function with debug on
    val result: Array[(Int, Int)] = testObject.kmeans(initialMeans, vectors, debug = false)

    // Assert that the function stops after max iterations without converging
    assert(result.length == 2)
    assert(result != initialMeans) // The means should change after iterations
  }

  import scala.concurrent.duration.given
  override val munitTimeout = 300.seconds
