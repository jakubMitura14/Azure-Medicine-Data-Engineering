// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

val dfprim = myImportFileLocalDataBricks("/FileStore/tables/twoPoinStudyrScalaA.csv")
val divisions = spark.read.table("twoPointStudiesDivisions")
//in this case csv did not recorded correctly the dates  and we need to sttick to the csv becouse of polish letters in column names
val shouldBeDates = List("Data badania wcześniejsze","Data badania późniejsze","Data operacji")
val df = advancedCasting(dfprim,shouldBeDates,"date", (c=>to_date(col(c)) ))

display(divisions)

// COMMAND ----------

// MAGIC %md
// MAGIC We will calculate using  Utils methods the columns that do not meet the manually set criteria in case of the numeric columns where it makes sense we will look for ouliers 

// COMMAND ----------

//all columns needed for those calculations we will find in division teble in metricData
  val frameOfDatesNumbs = getFrameWithCols(df, List(  "Data badania wcześniejsze",  "Data badania późniejsze"))
  .withColumn("monthsBetweenStudies",months_between(col("Data badania wcześniejsze"),col( "Data badania późniejsze")))



// COMMAND ----------

//we will also filter out all of the tuples where value is 0 
val listOfDataToReport = List(
countOutsideTheRange(df,getColNamesOfDivisions(divisions,List("imageNumbersFirst","imageNumbersSecond")), 0,70), // suv values
countOutsideTheRange(frameOfDatesNumbs,List("monthsBetweenStudies"),0,2000),//basicall this need to be positive time interval in months
countHowManyNulls(df,getColNamesOfDivisions(divisions,List("imageCharacteristicStudyOne","imageCharacteristicStudyTwo","imageNumbersFirst","imageNumbersSecond" ) ) ),// image and study date is always available 
countOutliersOfZscore (df,getColNamesOfDivisions(divisions,List("technicalData","clinicalNumericData","imageNumbersFirst","imageNumbersSecond")))) .flatten.filter(it=>it._3 >0)

// COMMAND ----------

// MAGIC %md
// MAGIC Report Table

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(listOfDataToReport)
val dfFromRDD1 = rdd.toDF("description", "columnName", "number")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("reportDataQualitytwoPointB") 
dfFromRDD1.createOrReplaceTempView("temp")