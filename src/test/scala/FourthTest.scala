import org.example.App
import org.scalatest.funsuite.AnyFunSuite


class FourthTest extends AnyFunSuite with SparkSessionHelper {

  import sqlImplicits._
  test("ex_4 should correctly join two DataFrames") {


    val inputData1 = Seq(
      ("App1", "Category1", "4.5"),
      ("App2", "Category2", "3.5"),
      ("App3", "Category3", "4.0")
    ).toDF("App", "Category", "Rating")

    val inputData2 = Seq(
      ("App1", "1000"),
      ("App2", "500"),
      ("App4", "2000")
    ).toDF("App", "Reviews")


    val expectedData = Seq(
      ("App1", "Category1", "4.5", "1000"),
      ("App2", "Category2", "3.5", "500")
    ).toDF("App", "Category", "Rating", "Reviews")

    val resultDf = App.ex_4(inputData1, inputData2)

    assertResult(expectedData.collect())(resultDf.collect())
  }
}