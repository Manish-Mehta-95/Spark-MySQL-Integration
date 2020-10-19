package spark.covid.com

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

object CovidProject extends App {

  //Creating SparkSession
  val spark = SparkSession
    .builder()
    .appName("CovidProject")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  //Importing covid data from MySQL database
  val world_data = spark.read
    .format("jdbc")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("url", "jdbc:Mysql://localhost:3306/covid")
    .option("dbtable", "all_data")
    .option("user", "root")
    .option("password", "manish")
    .load()

  //Persisting Dataframe
  world_data.persist()

  //Finding Top 5 most Covid infected Country till 2020-10-14
  val top5 = world_data
    .groupBy("Country")
    .max()
    .orderBy(desc("max(Confirmed)"))
    .limit(5)

  //Converting the name of top 5 countries in a list
  val list_top5 = top5
    .select("Country")
    .rdd
    .map(x => x(0))
    .collect
    .toList

  //Filtering Data of top 5 most infected countries
  val data_top5 = world_data
    .filter(col("Country").isin(list_top5) && col("Confirmed")>0)

  //Obtaining mean and standard deviation of 5 countries
  val (mean_view, std_view) = data_top5
    .select(mean("Confirmed"), stddev("Confirmed")).as[(Double, Double)]
    .first()

  //Creating table for date-wise normalized increment n covid cases of top 5 countries
  val Scaled_Data = data_top5
    .withColumn("view_scaled", (col("Confirmed") - mean_view)/std_view).groupBy("Date")
    .pivot("Country", list_top5)
    .max("view_scaled")
    .sort("Date")

  //Defining Window Specs
  val windowspec = Window
    .partitionBy("Country")
    .orderBy("Confirmed")

  //Creating table of India
  val India_Data = data_top5
    .filter(col("Country") === "India" && col("Confirmed") > 0)
    .withColumn("lag", lag("Confirmed", 1) over windowspec)
    .withColumn("Increased_Percentage", (col("Confirmed") - col("lag"))*100/col("lag"))
    .withColumn("Days", row_number.over(windowspec))
    .select(col("Date"), col("Days"), col("Confirmed").alias("Confirmed_Case"), col("Recovered"), col("Deaths"), col("Increased_Percentage"))

  //Here I choose another approach for writing file.
  //Defining url
  val url = "jdbc:Mysql://localhost:3306"

  //Creating properties object
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "manish")
  Class.forName("com.mysql.jdbc.Driver")

  //Defining Table
  val table1 = "covid.Scaled_Data"
  val table2 = "covid.India_Data"

  //Writing Table "Scaled_Data" and "India_Data" into MySQL database "covid"
  Scaled_Data.write.mode(SaveMode.Overwrite).jdbc(url,table1, properties)
  India_Data.write.mode(SaveMode.Overwrite).jdbc(url,table2, properties)

}
