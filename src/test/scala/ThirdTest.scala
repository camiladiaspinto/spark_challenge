import org.example.App
import org.scalatest.funsuite.AnyFunSuite


class ThirdTest extends AnyFunSuite with SparkSessionHelper {

  import sqlImplicits._

  test("ex_3 should correctly transform the DataFrame") {


    val inputDf = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", "ART_AND_DESIGN", "4.1", "159", "19M", "10,000+", "Free", "0", "Everyone", "Art & Design", "January 7, 2018", "1.0.0", "4.0.3 and up"),
      ("Another App", "PHOTOGRAPHY", "3.5", "100", "15M", "5,000+", "Paid", "$2.99", "Teen", "Photography", "February 15, 2022", "2.0.0", "5.0 and up")
    ).toDF("App", "Category", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content Rating", "Genres",
        "Last Updated", "Current Ver", "Android Ver")


    val expected = Seq(
      ("Photo Editor & Candy Camera & Grid & ScrapBook", Array("ART_AND_DESIGN"), 4.1, 159, 19.0, "10,000+", "Free", 0.0, "Everyone", Array("Art & Design"), "2018-01-07 00:00:00", "1.0.0", "4.0.3 and up"),
      ("Another App", Array("PHOTOGRAPHY"), 3.5, 100, 15.0, "5,000+", "Paid", 2.6910000000000003, "Teen", Array("Photography"), "2022-02-15 00:00:00", "2.0.0", "5.0 and up")

    ).toDF("App", "Categories", "Rating", "Reviews", "Size",
        "Installs", "Type", "Price", "Content_Rating", "Genres",
        "Last_Updated", "Current_Version", "Minimum_Android_Version")


    val resultDf = App.ex_3(inputDf)

    assertResult(expected.collect())(resultDf.collect())
  }

}
