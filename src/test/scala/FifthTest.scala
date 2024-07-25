import org.example.App
import org.scalatest.funsuite.AnyFunSuite


class FifthTest extends AnyFunSuite with SparkSessionHelper {

  import sqlImplicits._
  test("ex_5 should correctly join, aggregate and write the DataFrame") {

    val inputData1 = Seq(
      ("App1", "Action", 4.5),
      ("App2", "Adventure", 3.5),
      ("App3", "Action", 4.0),
      ("App4", "Adventure", 3.8)
    ).toDF("App", "Genres", "Rating")

    val inputData2 = Seq(
      ("App1", 0.2),
      ("App2", 0.4),
      ("App3", 0.3),
      ("App4", 0.5)
    ).toDF("App", "Average_Sentiment_Polarity")

    val expectedData = Seq(
      ("Action", 2, 4.25, 0.25), // 2 apps in Action genre: (4.5 + 4.0) / 2 = 4.25
      ("Adventure", 2, 3.65, 0.45) // 2 apps in Adventure genre: (3.5 + 3.8) / 2 = 3.65
    ).toDF("Genres", "Count", "Average_Rating", "Average_Sentiment_Polarity")


    val resultDf = App.ex_5(inputData1, inputData2)

    assertResult(expectedData.collect())(resultDf.collect())
  }
}