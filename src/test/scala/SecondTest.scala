import org.example.App
import org.scalatest.funsuite.AnyFunSuite


class SecondTest extends AnyFunSuite with SparkSessionHelper {
    import sqlImplicits._

  test("NaN values should be replaced by 0 and filtered by 4.0 or equal") {

    val inputData = Seq(
      ("App1", "4.5"),
      ("App2", "NaN"),
      ("App3", "3.5"),
      ("App4", "4.0"),
      ("App5", "5.0"),
      ("App6", null)
    ).toDF("App", "Rating")

    val expectedData = Seq(
      ("App5", 5.0),
      ("App1", 4.5),
      ("App4", 4.0)
    ).toDF("App", "Rating")

    val resultDf = App.ex_2(inputData)

    assertResult(expectedData.collect())(resultDf.collect())
  }
}
