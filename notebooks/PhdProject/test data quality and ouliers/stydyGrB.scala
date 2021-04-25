// Databricks notebook source
// MAGIC %md 
// MAGIC #Data Quality Tests
// MAGIC goal here is to check weather  data quality is preserved in order to do this in this we will mainly check weather numerical data, and dates keeps in the manually set constraints and weather all columns that should not contain nulls are not 

// COMMAND ----------

// MAGIC %run /PhdProject/utils

// COMMAND ----------

val dfprim = myImportFileLocalDataBricks("/FileStore/tables/studyGrScalaA.csv")
val shouldBeDate = List("Rok urodzenia","Data badania","Data operacji")
val df = advancedCasting(dfprim,shouldBeDate,"date", (c=>to_date(col(c)) ))
val divisions = spark.read.table("studyGroupDivisions")
display(divisions)


// COMMAND ----------

// MAGIC %md
// MAGIC First we will define the column names and what we want to check in each case

// COMMAND ----------

// should be between 0 and 70
val listOfSuvs = List("SUV (max) w miejscu zapalenia", "SUV (max) tła")
//should be between 0 and 1
val zeroToOne = List("tumor to background ratio")
//should be between 10 and 500
val shouldBeBetween0And500 = getColNamesOfDivisions(divisions, List("technicalData"))
//should not be null
val shouldNotBeNull = getColNamesOfDivisions(divisions, List("imageCharacteristic","categoricalValuesCols","technicalData"))


// COMMAND ----------

// MAGIC %md
// MAGIC We need to separately analyze dates in order to calculate the age of the patient and check is it in given constraints; also date of the operation that is after birth and before imaging study and weather imaging study is after birth and after surgery (all patients in control group needed to have surgery in order to have the vascular prosthesis that can get inflammed) also we will keep the results in a separete table in ordet to prevent a need to recalculate it later to calculate summary statistics

// COMMAND ----------

//all columns needed for those calculations we will find in division teble in metricData
  val frameOfDatesNumbs = getFrameWithCols(df, List( "Rok urodzenia", "Data badania", "Data operacji"))
  .withColumn("monthsTillSurgery",months_between(col("Data operacji"), col("Rok urodzenia")))
  .withColumn("monthsTillStudy", months_between( col("Data badania"), col("Rok urodzenia") ))
  .withColumn("monthsFromSurgeryToStudy", months_between(col("Data badania"), col("Data operacji") ))



// COMMAND ----------

// MAGIC %md 
// MAGIC Now we will  check  weather all of our criteria are met , and weather oultiers are present and we will save the outcome into a table

// COMMAND ----------

//we will also filter out all of the tuples where value is 0 
val listOfDataToReport = List(
countOutsideTheRange(df, listOfSuvs, 0,70),
countOutsideTheRange(df,zeroToOne,0,1),
countOutsideTheRange(df,shouldBeBetween0And500,0,500),
countOutsideTheRange(frameOfDatesNumbs,List("monthsTillSurgery","monthsTillStudy","monthsFromSurgeryToStudy"),0,2000),//basicall this need to be positive time interval in months
countHowManyNulls(df,shouldNotBeNull),
countOutliersOfZscore (df,(listOfSuvs++zeroToOne))) .flatten.filter(it=>it._3 >0)



// COMMAND ----------

// MAGIC %md
// MAGIC #Report table
// MAGIC In the end we want to create report table where we will summarize the amount of null values;  whether all constraints were met and weather we have detected outliers if so how many and where

// COMMAND ----------

val rdd = spark.sparkContext.parallelize(listOfDataToReport)
val dfFromRDD1 = rdd.toDF("description", "columnName", "number")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("reportDataQualitystudyGrB") 
dfFromRDD1.createOrReplaceTempView("temp")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from temp