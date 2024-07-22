package org.example

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.{to_date,first,array_distinct,filter, avg, col, collect_list, count, desc, max, max_by, regexp_extract, to_timestamp, when}
/**
 * @author ${Camila Pinto}
 */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkChallenge")
      .getOrCreate()

    val google_playstore = spark.read
      .option("header", value = true)
      .option("quote", "\"")
      .option("escape", "\\")
      .csv("src/main/datasets/googleplaystore.csv")

    val google_playstore_reviews = spark.read
      .option("header", value = true)
      .option("quote", "\"")
      .option("escape", "\\")
      .csv("src/main/datasets/googleplaystore_user_reviews.csv")
    //google_playstore_reviews.show()

    val df_1 = ex_1(google_playstore_reviews)

    val df_2 = ex_2(google_playstore)

    val df_3 = ex_3(google_playstore)
    val df_4 = ex_4(df_1, df_3)
    val df_5 = ex_5(df_1, df_3)

    df_1.show()
    df_2.show()
    df_3.show()
    df_4.show()
    df_5.show()
    spark.stop()
  }


  def ex_1(inputDf: DataFrame): DataFrame = {
    val df = inputDf
      .na.replace("Sentiment_Polarity", Map("nan" -> "0"))
      .withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast("Double"))
      .groupBy("App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_SentimentPolarity")
    df
  }


  def ex_2(inputDf: DataFrame): DataFrame = {
    val df = inputDf
      .na.replace("Rating", Map("NaN" -> "0"))
      .withColumn("Rating", col("Rating").cast("Double"))
      .filter("Rating >= 4.0")
      .orderBy(desc("Rating"))

    df
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\\")
      .option("delimiter", "§")
      .csv("C:\\Users\\camil\\IdeaProjects\\spark-challenge\\output\\best_apps")

    df

  }

  def ex_3(inputDf: DataFrame): DataFrame = {
    val conversionRate = 0.9 // Taxa de conversão de dólares para euros

    val df = inputDf
      //remover mais missing values
      .na.replace("Reviews", Map("null" -> "0"))
      .na.replace("Rating", Map("NaN" -> "0"))
      .na.replace("Size", Map("Varies with device" -> "0"))


      .withColumnRenamed("Category", "Categories")
      .withColumn("Rating", col("Rating").cast("Double"))
      .withColumn("Reviews", col("Reviews").cast("Long"))
      .withColumn("Size",
        when(col("Size").contains("M"),
          regexp_extract(col("Size"), "([0-9]+\\.?[0-9]*)", 1).cast("Double")) // Para valores terminados em M, extrai e converte diretamente
          .otherwise(
            regexp_extract(col("Size"), "([0-9]+\\.?[0-9]*)", 1).cast("Double") / 1024 // Para outros valores, assume que estão em KB e divide por 1024 para converter para MB
          )
      )
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Last Updated", to_date(col("Last Updated"), "MMMM d, yyyy"))
      .withColumn("Price",
        when(col("Price").contains("$"),
          regexp_extract(col("Price"), "([0-9]+\\.?[0-9]*)", 1).cast("Double") * conversionRate) // Remove o símbolo $ e converte para euros
          .otherwise(0.0) // Define 0 para valores que não começam com $
      )
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")


    val groupedDf = df
      .groupBy("App")
      .agg(
        collect_list("Categories").as("All_Categories"),
        avg("Rating").as("Average_Rating"),
        max("Reviews").as("Max_Reviews"),
        first("Genres").as("Genres"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content_Rating").as("Content_Rating"),
        first("Last_Updated").as("Last_Updated"),
        first("Current_Version").as("Current_Version"),
        first("Minimum_Android_Version").as("Minimum_Android_Version")
      )
      .withColumn("Categories", array_distinct(col("All_Categories")))
      .drop("All_Categories")

    groupedDf

  }

  def ex_4(inputDf1: DataFrame, inputDf2: DataFrame): DataFrame = {
    val df = inputDf1
      .join(inputDf2, Seq("App"), "left")
    df
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("C:\\Users\\camil\\IdeaProjects\\spark-challenge\\output\\googleplaystore_cleaned")

    df
  }

  def ex_5(inputDf1: DataFrame, inputDf2: DataFrame): DataFrame = {
    val df = inputDf1
      .join(inputDf2, Seq("App"))
      .groupBy("Genres")
      .agg(
        count(col("Genres")).as("Count"),
      )
        //avg(col("Average_Sentiment_Polarity")).as("Average_Sentiment_Polarity")

    df
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet("C:\\Users\\camil\\IdeaProjects\\spark-challenge\\output\\googleplaystore_cleaned")

    df
  }
}

