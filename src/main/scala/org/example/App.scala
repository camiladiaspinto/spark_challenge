package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{ avg, col, collect_set,count, date_format, desc, first, max, regexp_replace, to_timestamp, when}

/**
 * @author ${Camila Pinto}
 */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkChallenge")
      .getOrCreate()


    val google_playstore = read_csv(spark, "src/main/datasets/googleplaystore.csv")
    val google_playstore_reviews = read_csv(spark, "src/main/datasets/googleplaystore_user_reviews.csv")


    val df_1 = ex_1(google_playstore_reviews)

    val df_2 = ex_2(google_playstore)

    val df_3 = ex_3(google_playstore)
    val df_4 = ex_4(df_1, df_3)
    val df_5 = ex_5(df_1, df_3)

    //df_1.show()
   // df_2.show()
   // df_3.show()
   // df_4.show()
   // df_5.show()
    spark.stop()
  }

  def read_csv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\\")
      .csv(path)
  }

  def ex_1(inputDf: DataFrame): DataFrame = {
    val df = inputDf
      .na.replace("Sentiment_Polarity", Map("nan" -> "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("Double"))
      .groupBy("App")
      .agg(
        avg("Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )
    df
  }


  def ex_2(inputDf: DataFrame): DataFrame = {
    val df = inputDf
      .na.replace("Rating", Map("NaN" -> "0"))
      .withColumn("Rating", col("Rating").cast("Double"))
      .filter("Rating >= 4.0")
      .orderBy(desc("Rating"))
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\\")
      .option("delimiter", "§")
      .csv("C:\\Users\\camil\\IdeaProjects\\spark_challenge\\output\\best_apps")
    df
  }

  def ex_3(inputDf: DataFrame): DataFrame = {

    val df = inputDf

      //substituir valores nulos
      .na.replace("Reviews", Map("null" -> "0"))
      .na.replace("Rating", Map("NaN" -> "0"))
      .na.replace("Size", Map("Varies with device" -> "null"))

      //formato dataframe
      .withColumn("Rating", col("Rating").cast("Double"))
      .withColumn("Reviews", col("Reviews").cast("Long"))

      //se acabar com M faz a conversão para double, se acabar com k multiplica por 1024
      .withColumn("Size",
        when(col("Size").endsWith("M"), regexp_replace(col("Size"), "M", "").cast("Double"))
          .when(col("Size").endsWith("k"), regexp_replace(col("Size"), "k", "").cast("Double") / 1024)
          .otherwise(null.asInstanceOf[Double])
      )
      .withColumn("Price",
        when(col("Price").startsWith("$"), regexp_replace(col("Price"), "\\$", "").cast("Double") * 0.9)
          .otherwise(0.0)
      )
      // Remover milissegundos do timestamp
      .withColumn("Last Updated", date_format(to_timestamp(col("Last Updated"), "MMMM d, yyyy"), "yyyy-MM-dd HH:mm:ss"))


    // agrupar por App para dataframe
    val resultDf = df
      .groupBy("App")
      .agg(
        //collect_set para garantir que todas as categorias da app são guardados num array sem duplicados
        collect_set("Category").as("Categories"),

        first("Rating").as("Rating"),
        max("Reviews").as("Max_Reviews"),
        first("Size").as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content Rating").as("Content_Rating"),
        //Converter string de Genres para array de strings, sem duplicados
        collect_set("Genres").as("Genres"),
        first("Last Updated").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )
    resultDf
  }

  def ex_4(inputDf1: DataFrame, inputDf2: DataFrame): DataFrame = {
    val df = inputDf1
      .join(inputDf2, Seq("App"))
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("C:\\Users\\camil\\IdeaProjects\\spark_challenge\\output\\googleplaystore_cleaned")

    df
  }

  def ex_5(inputDf1: DataFrame, inputDf2: DataFrame): DataFrame = {
    val df = inputDf1
      .join(inputDf2, Seq("App"))
      .groupBy("Genres")
      .agg(
        count(col("Genres")).as("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("C:\\Users\\camil\\IdeaProjects\\spark_challenge\\output\\googleplaystore_metrics")

    df
  }
}

