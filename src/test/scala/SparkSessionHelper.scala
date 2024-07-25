import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionHelper extends AnyFunSuite with BeforeAndAfterAll {

  private var spark : SparkSession = _

  protected lazy val sqlImplicits: SQLImplicits = spark.implicits

  protected override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test helper")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
  }

  protected override def afterAll(): Unit = {
    spark.stop()
  }
}