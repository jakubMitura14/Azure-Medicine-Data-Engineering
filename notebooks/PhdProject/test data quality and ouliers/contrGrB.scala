// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

val dfprim = myImportFileLocalDataBricks("/FileStore/tables/contrGrScalaA.csv")
val divisions = spark.read.table("contrGroupDivisions")
//in this case csv did not recorded correctly the dates  and we need to sttick to the csv becouse of polish letters in column names
val shouldBeDate = List("data badania 1","data wszczepienia stentgraftu","ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy")
val df = advancedCasting(dfprim,shouldBeDate,"date", (c=>to_date(col(c)) ))

df.printSchema

// COMMAND ----------

display(divisions)

// COMMAND ----------

// MAGIC %md
// MAGIC We will calculate using  Utils methods the columns that do not meet the manually set criteria in case of the numeric columns where it makes sense we will look for ouliers 

// COMMAND ----------

//all columns needed for those calculations we will find in division teble in metricData
  val frameOfDatesNumbs = getFrameWithCols(df, List( "Rok z peselu", "data wszczepienia stentgraftu", "data badania 1"))
  .withColumn("monthsTillSurgery",year(col("data wszczepienia stentgraftu"))- col("Rok z peselu"))
  .withColumn("monthsTillStudy", year(col("data badania 1"))- col("Rok z peselu") )
  .withColumn("monthsFromSurgeryToStudy", months_between(col("data badania 1"), col("data wszczepienia stentgraftu") ))

// COMMAND ----------

display(frameOfDatesNumbs)

// COMMAND ----------

//we will also filter out all of the tuples where value is 0 
val listOfDataToReport = List(
countOutsideTheRange(df, List("SUV protezy", "tło"), 0,70), // suv values
countOutsideTheRange(frameOfDatesNumbs,List("monthsTillSurgery","monthsTillStudy","monthsFromSurgeryToStudy"),0,2000),//basicall this need to be positive time interval in months
countHowManyNulls(df,List("skala5Stopnie", "skala3Stopnie")),// image and study date is always available 
countOutliersOfZscore (df,(List("SUV protezy", "tło")))) .flatten.filter(it=>it._3 >0)

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(listOfDataToReport)
val dfFromRDD1 = rdd.toDF("description", "columnName", "number")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("reportDataQualitycontrGrB") 
dfFromRDD1.createOrReplaceTempView("temp")