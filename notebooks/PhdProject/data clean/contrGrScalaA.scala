// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

val dfPrim = myImportFile("controlGroupCSV.csv")
display(dfPrim)

// COMMAND ----------



val dfPrim = myImportFile("controlGroupCSV.csv")
val df = dfPrim
.withColumnRenamed(dfPrim.columns(30), "skala5Stopnie")
.withColumnRenamed(dfPrim.columns(29), "skala3Stopnie")
.where("`data badania 1` is not null")
df.createOrReplaceTempView("df") // in order to enable sql queries if needed

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from df

// COMMAND ----------

// columns to drop
val toDropCols = List("_c0","_c1","_c2","_c3","_c4","rok z daty badania","wiek w momencie badania","lekarz kierujący","ośrodek kierujący",
                      "czas od wszczepienia do badania PET/CT","posiewy18","_c25","posiewy27")
val df2 = df.drop((toDropCols):_*)

// should be date is a string
val shouldBeDate = List("data badania 1","data wszczepienia stentgraftu","ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy")
val df3 = advancedCasting(df2,shouldBeDate,"date", (c=>to_date(col(c)) ))

//categorical data
val cateGoricalData = List("powód standaryzowany","stentgraft czy proteza","typ","skala5Stopnie","skala3Stopnie","skierowany")
//should be boolean 0 is false and 1 is true
val shouldBeBoolean = List("proteza udowo - podkolanowa","przetoka pachwinowa","cukrzyca","zarejestrowany zgon","reoperacje")

val df4 = advancedCasting(df3,shouldBeBoolean,"boolean", (c=>col(c) ))

///////////// dividing to logical subtables for convinience
val metricData = List("data badania 1","Rok z peselu","ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy","pacjent obserwowany po PET")
//technical data
val technicalData = List("aktywnosc w dniu podania [MBq]","glukoza w dniu podania [mg/dl]")
//clinical data
val clinicalData = List("powód","przetoka pachwinowa","przetoka pachwinowa","cukrzyca","zarejestrowany zgon","reoperacje","skierowany","coś charkterystycznego")
//main categories on the basis of which we will divide patients
val mainCatogries = List("powód standaryzowany","stentgraft czy proteza","typ")
//imageCharacteristic
val imageCharacteristics = List("skala5Stopnie","skala3Stopnie")
// numerical data about image
val numericImageCharacteristic = List("SUV protezy","tło")

// COMMAND ----------

display(df2)

// COMMAND ----------

display(df3)

// COMMAND ----------

df4.coalesce(1).write.mode("overwrite").option("delimiter", "\t").format("com.databricks.spark.csv").option("header", "true").save("/FileStore/tables/contrGrScalaA.csv")


// COMMAND ----------

display(df4)

// COMMAND ----------

val data = Seq(("cateGoricalData", cateGoricalData), ("metricData", metricData), ("technicalData", technicalData), 
               ("clinicalData", clinicalData), ("mainCatogries", mainCatogries), ("imageCharacteristics", imageCharacteristics),
               ("numericImageCharacteristic", numericImageCharacteristic))

val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF("divisionName", "listOfColumnNames")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("contrGroupDivisions") 