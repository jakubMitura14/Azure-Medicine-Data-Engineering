// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

val dfPrim = myImportFile("twoPointStudyCsv.csv")
val df = dfPrim
.withColumnRenamed(dfPrim.columns(46), "skala5StopnieStudy1")
.withColumnRenamed(dfPrim.columns(47), "skala3StopnieStudy1")
.withColumnRenamed(dfPrim.columns(74), "skala5StopnieStudy2")
.withColumnRenamed(dfPrim.columns(75), "skala3StopnieStudy2")
.withColumnRenamed(dfPrim.columns(107), "Hussman")
.where("`Data badania wcześniejsze` is not null")

df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %sql
// MAGIC describe df

// COMMAND ----------

//columns that should be dropped 
val shouldBeDropped = List("_c0","_c1","_c2","_c3","czas między operacjami w latach","wiek w momencie operacji","rożnica w glikemii między badaniami","atybiotyki","_c38","_c42","_c57","_c65","_c69","_c73","_c85","_c87","_c88","Opis interesującego miejsca w innych badaniach obrazowych np. w CT","ocena wzrokowa wg. 3 stopniowej (Bowles , Ambrosioni 2018; ProstheticVascularGraftKeidar ) (dodatnie - 1 i 2)90","Nieregularne zarysy91","_c92","Pęcherzyki gazu93","Skrzeplina w okolicy miejsca podejrzanego o zapalenie94","Obszar płynowy w okolicy95","wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej96","Naciek zapalny w okolicy97","przetoka ropna98", "tętniak rzekomy99" ,"tętniak rzekomy100","aktywne metabolicznie węzły chłonne w okolicy101" )

val df2 = df.drop((shouldBeDropped):_*)
// should be casted to numbers
val shouldBeNumbers = List("Podana aktywność badanie wcześniejsze","lok - aorta brzuszna34","łuk aorty39","aorta wstępująca - cała40","aorta wstępująca przyzastawkowo41","na wysokości spojenia łonowego43","SUV (max) w miejscu zapalenia44","SUV (max) tła45", "SUV (max) w miejscu zapalenia71","SUV (max) tła72")

val numbsCasted = advancedCasting(df2,shouldBeNumbers,"double", (c=>regexp_replace(col(c), ",", ".") ))

//column names that should be parsed to dates  https://sparkbyexamples.com/spark/spark-sql-how-to-convert-date-to-string-format/
val shouldBeDates = List("Data badania wcześniejsze","Data badania późniejsze","Data operacji")

val datesCasted = advancedCasting(numbsCasted,shouldBeDates,"date", (c=>to_date(col(c)) ))
datesCasted.createOrReplaceTempView("datesCasted") // in order to enable sql queries if needed
/////////////should be boolean we will store data about those as a map of sequences of sequences where each list will represent diffrent way how the boolean values are stored generally null means lack of measurement
//true = "Prawda" false = ""
val prawdaFalsz = List("Nieregularne zarysy48","Ogniskowe gromadzenie znacznika49","Pęcherzyki gazu50","Skrzeplina w okolicy miejsca podejrzanego o zapalenie51", "Obszar płynowy w okolicy52","wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej53", "Naciek zapalny w okolicy54", "przetoka ropna55", "tętniak rzekomy56", "aktywne metabolicznie węzły chłonne w okolicy58","Nieregularne zarysy76","Ogniskowe gromadzenie znacznika77","Pęcherzyki gazu78","Skrzeplina w okolicy miejsca podejrzanego o zapalenie79", "Obszar płynowy w okolicy80","wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej81", "Naciek zapalny w okolicy82", "przetoka ropna83", "tętniak rzekomy84", "aktywne metabolicznie węzły chłonne w okolicy86" )

val prawdaFalszCasted = booleanCastingIfString (datesCasted,"datesCasted" , "prawda" ,prawdaFalsz ) 

//true = 1 false = 0
val oneAndZero = List("lokalizacja ogniska podwyższonego gromadzenia33","lok - aorta brzuszna34","okolica rozwidlenia35","lewe ramię36","prawe ramię37","lokalizacja ogniska podwyższonego gromadzenia60","lok - aorta brzuszna61","okolica rozwidlenia62","lewe ramię63","prawe ramię64","łuk aorty66","aorta wstępująca - cała67","aorta wstępująca przyzastawkowo68","na wysokości spojenia łonowego70")

val OneOrZeroCasted = advancedCasting(prawdaFalszCasted,oneAndZero,"boolean", (c=>col(c) ))


//list of columns with categorical values 
val categoricalValuesCols = List("skala5StopnieStudy1","skala3StopnieStudy1","Rodzaj protezy starsze","Rodzaj protezy nowsze","skala5StopnieStudy2","skala3StopnieStudy2")

//here we will define some column names that will create some logical subdivision of data making the analysis of such a wide table easier we will store names of columns for each subdivision in a sepatate list
// metric data
val metricData = List("Płeć","Data badania wcześniejsze","Data badania późniejsze","Data operacji")
// table summarizing clinical data
val clinicalData = List("Powód operacji","Lokalizacja Graftu","Rodzaj protezy starsze","Rodzaj protezy nowsze","Rodzaj protezy - opis dodatkowy","Wcześniej operowany w danym miejsu pierwsze badanie","Wcześniej operowany w danym miejsu drugie badanie","cukrzyca","choroby przebyte i przewlekłe")
//localisation of focally increased acrivity  first study
val locOfFocFirst = List("lokalizacja ogniska podwyższonego gromadzenia33","lok - aorta brzuszna34","okolica rozwidlenia35","lewe ramię36","prawe ramię37","łuk aorty39","aorta wstępująca - cała40","aorta wstępująca przyzastawkowo41","na wysokości spojenia łonowego43")
//localisation of focally increased activity second study
val locOfSecond = List("lokalizacja ogniska podwyższonego gromadzenia60","lok - aorta brzuszna61","okolica rozwidlenia62","lewe ramię63","prawe ramię64","łuk aorty66","aorta wstępująca - cała67","aorta wstępująca przyzastawkowo68","na wysokości spojenia łonowego70")


// numerical data describing image in first study
val imageNumbersFirst  = List("SUV (max) w miejscu zapalenia44","SUV (max) tła45") 
//
val imageNumbersSecond  = List("SUV (max) tła45", "SUV (max) w miejscu zapalenia71","SUV (max) tła72") 


//numeric clinical data
val clinicalNumericData = List("CRP(6 mcy) pierwsze badanie","CRP(6 mcy) drugie badanie","WBC(6 mcy) pierwsze badanie","WBC(6 mcy) drugie badanie")
//some technical values related to study
val technicalData =  List("Glikemia badanie wcześniejsze","Glikemia badanie późniejsze","Podana aktywność badanie wcześniejsze","Podana aktywność badanie późniejsze")
//basic categories like type of vascular prosthesis ..
val basicCategories = List("Lokalizacja Graftu","Rodzaj protezy starsze","Rodzaj protezy nowsze","Hussman")
// storing microbiologic data
val microbiologicData =  List("posiewy dodatnie pierwsze badanie","posiewy dodatnie drugie badanie","posiewy ujemne pierwsze badanie","posiewy ujemne drugie badanie")


// describing presence of lack of presence of given  radiologic sign in study 1
val imageCharacteristicStudyOne = List("skala5StopnieStudy1","skala3StopnieStudy1","Nieregularne zarysy48","Ogniskowe gromadzenie znacznika49","Pęcherzyki gazu50","Skrzeplina w okolicy miejsca podejrzanego o zapalenie51", "Obszar płynowy w okolicy52","wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej53", "Naciek zapalny w okolicy54", "przetoka ropna55", "tętniak rzekomy56", "aktywne metabolicznie węzły chłonne w okolicy58"  )
// describing presence of lack of presence of given  radiologic sign in study 2
val imageCharacteristicStudyTwo = List("skala5StopnieStudy2","skala3StopnieStudy2","Nieregularne zarysy76","Ogniskowe gromadzenie znacznika77","Pęcherzyki gazu78","Skrzeplina w okolicy miejsca podejrzanego o zapalenie79", "Obszar płynowy w okolicy80","wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej81", "Naciek zapalny w okolicy82", "przetoka ropna83", "tętniak rzekomy84", "aktywne metabolicznie węzły chłonne w okolicy86" )


display(df.select(imageCharacteristicStudyTwo.map(it=> col(it)):_* ))


// COMMAND ----------

// MAGIC %md
// MAGIC #saving tables
// MAGIC we are saving here to the data lake 

// COMMAND ----------

OneOrZeroCasted.coalesce(1).write.mode("overwrite").option("delimiter", "\t").format("com.databricks.spark.csv").option("header", "true").save("/FileStore/tables/twoPoinStudyrScalaA.csv")


// COMMAND ----------

val data = Seq(("categoricalValuesCols", categoricalValuesCols),("metricData", metricData),("clinicalData", clinicalData),("locOfFocFirst", locOfFocFirst),("locOfSecond", locOfSecond)
              ,("imageNumbersFirst", imageNumbersFirst),("imageNumbersSecond", imageNumbersSecond),("clinicalNumericData", clinicalNumericData),("technicalData", technicalData),("basicCategories", basicCategories)
              ,("microbiologicData", microbiologicData),("imageCharacteristicStudyOne", imageCharacteristicStudyOne),("imageCharacteristicStudyTwo", imageCharacteristicStudyTwo))

val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF("divisionName", "listOfColumnNames")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("twoPointStudiesDivisions") 

