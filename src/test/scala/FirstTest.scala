import org.example.App
import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite with SparkSessionHelper {

  import sqlImplicits._

  test ("An nan value should be replaced by 0.0") {

    val inputData = Seq(
          ("App1", "0.5"),
          ("App1", "nan"),
          ("App2", "1.0"),
          ("App2", "nan"),
          ("App2", "0.5")
        ).toDF("App", "Sentiment_Polarity")

      val expectedData = Seq(
          ("App1", 0.25),
          ("App2", 0.5)
        ).toDF("App", "Average_Sentiment_Polarity")

    val df_1 = App.ex_1(inputData)

    assertResult(expectedData.collect())(df_1.collect())
  }
}