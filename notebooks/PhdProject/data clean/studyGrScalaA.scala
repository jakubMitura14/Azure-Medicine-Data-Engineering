// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

// MAGIC %md
// MAGIC reading table into spark dataframe with scala api

// COMMAND ----------

val df = myImportFile("studyGroupCsv.csv").where("`Płeć` is not null")

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC In order to make the dataframe work we need to change the names of some columns so we can than filter all that has not more than one not null values

// COMMAND ----------


val df2 = df.withColumnRenamed("Opis interesującego miejsca w innych badaniach obrazowych np. w CT", "opicCT")
.withColumnRenamed(df.columns(92), "skala5Stopnie")
.withColumnRenamed("ocena wzrokowa wg. 3 stopniowej (Bowles , Ambrosioni 2018; ProstheticVascularGraftKeidar ) (dodatnie - 1 i 2)", "skala3Stopnie")
//.withColumnRenamed("Wcześniej operowany w danym miejsu", "WczesniejOperowany")
.withColumnRenamed("Podana aktywność", "Podana Aktywnosc")
.withColumnRenamed("Powód operacji", "PowodOperacji")
.withColumnRenamed("obecność przetoki pachwinowej", "przetokaPachwinowa")
.withColumnRenamed("infekcje w okresie okołooperacyjnym", "infekcjeOkolooperacyjnie")
.withColumnRenamed("Pęcherzyki gazu ", "PecherzykiGazu")
.withColumnRenamed("Materiał", "Material")
.withColumnRenamed("Powód operacji - tętniak", "tetniakPowodOper")
.withColumnRenamed("Powód operacji - miażdżyca - głównie zespół Leriche'a", "lerichPowodOper")
.withColumnRenamed("Obszar płynowy w okolicy", "Obszar plynowy w okolicy")
.withColumnRenamed("wysięk, obrzęk, zatarcie zarysów tkanki tłuszczowej", "wysiekZatarcieTluszczu")
.withColumnRenamed("tętniak rzekomy102", "tetniakRzekomyObraz")
.withColumnRenamed("tętniak rzekomy57", "tetniakRzekomyCT")
.withColumnRenamed("blaszka miażdżycowa w okolicy zapalenia", "Atherosclerosis")
.withColumnRenamed("aktywne metabolicznie węzły chłonne w okolicy", "activeLymphNodes")
.withColumnRenamed("lewe ramię", "lewe ramie")
.withColumnRenamed("prawe ramię", "prawe ramie")
.withColumnRenamed("aorta wstępująca - cała", "wholeAscendingAorta")
//.withColumnRenamed("Płeć", "Plec")
.withColumnRenamed("zakażenie poprzedniej protezy", "infectionOfPrevious")
.withColumnRenamed("typ obrazu 1 tylko ogniskowo; 2 rozlany  i ogniskowy; 3 tragedia", "imageTypeOurClassification")




val cols = df2.columns
//val nullcolsToDrop = cols.filter( cl=>df2.select(cl).rdd.map(r => r(0)).collect.toList.filter(it=>it!=null).length<2 )
//futher we can drop some of the columns that are unnecessary
val unnecessaryColumnsToDrop= List("Grupa kontrolna","wiek w momencie badania","wiek w momencie operacji","czas od operacji do obrazowania w latach","Lokalizacja Graftu","Rodzaj protezy - opis dodatkowy","_c0","_c1","_c2","_c3","wynik posiewu z protezy36","_c50","Alkoholizm","chromanie przestankowe")
val df3 = df2.drop((unnecessaryColumnsToDrop):_*)

 



// COMMAND ----------

// MAGIC %md now as we filtered not necessary rows we need to cast the columns correctly - it is mainly about dates and numbers as numbers sometimes are written with dot and sometimes with comma, but also some should be boolean values
// MAGIC first I will manually choose columns that date type should be changed on the basis of  visual inspection
// MAGIC secondly we will parse it into correct format and check weather it was parsed correctly

// COMMAND ----------


//column names that should be parsed into numbers 
val shouldBeNumbers = List("Podana Aktywnosc","CRP(6 mcy)","WBC(6 mcy)","SUV (max) w miejscu zapalenia","SUV (max) tła","tumor to background ratio")

val numbsCasted = advancedCasting(df3,shouldBeNumbers,"double", (c=>regexp_replace(col(c), ",", ".") ))



// COMMAND ----------

//column names that should be parsed to dates  https://sparkbyexamples.com/spark/spark-sql-how-to-convert-date-to-string-format/
val shouldBeDates = List("Rok urodzenia","Data badania","Data operacji")
val datesCasted = advancedCasting(numbsCasted,shouldBeDates,"date", (c=>to_date(col(c)) ))


datesCasted.createOrReplaceTempView("datesCasted") // in order to enable sql queries if needed



// COMMAND ----------

/////////////should be boolean we will store data about those as a map of sequences of sequences where each list will represent diffrent way how the boolean values are stored generally null means lack of measurement
//true = "Prawda" false = ""
val prawdaFalsz = List("Wcześniej operowany w danym miejsu","przetokaPachwinowa","zgon","infekcjeOkolooperacyjnie","cukrzyca","Nikotynizm",
                       "Nieregularne zarysy","Ogniskowe gromadzenie znacznika","Skrzeplina w okolicy miejsca podejrzanego o zapalenie","Obszar plynowy w okolicy","wysiekZatarcieTluszczu","Naciek zapalny w okolicy","przetoka ropna","tetniakRzekomyObraz","Atherosclerosis","activeLymphNodes","PecherzykiGazu")



val prawdaFalszCasted = booleanCastingIfString (datesCasted,"datesCasted" , "prawda" ,prawdaFalsz ) 

//true = 1 false = 0 
// val OneOrZero = List("tetniakPowodOper","lerichPowodOper","infectionOfPrevious","nieznany","proteza dodatni","proteza ujemny","rana +","przetoka +","krew +","krew -","reoperacje - 1 jeśli tak  ?","znane protezy udowo podkolanowe","obecność skrzepliny","wzmożenie densyjności tkanek w okolicy protezy","CT bez zmian","lok - aorta brzuszna","okolica rozwidlenia","lewe ramie","prawe ramie","aorta wstępująca - cała","aorta wstępująca przyzastawkowo","tetniakRzekomyCT","Atherosclerosis","activeLymphNodes")

// val OneOrZeroCasted = advancedCasting(prawdaFalszCasted,OneOrZero,"boolean", (c=>col(c) ))

//true = "tak"  false = "nie"
prawdaFalszCasted.createOrReplaceTempView("prawdaFalszCasted") // in order to enable sql query in booleanCastingIfString
val takNie = List("Gorączka","bolesność okolicy protezy","widoczny zewnętrzny obrzęk","tętniak","pęcherzyki powietrza","niedrożność jednego z ramion","Otyłość")

val takNieCasted = booleanCastingIfString (prawdaFalszCasted,"prawdaFalszCasted" , "tak" ,takNie ) 




// COMMAND ----------



// COMMAND ----------

display(getFrameWithCols(takNieCasted, List("Wcześniej operowany w danym miejsu","przetokaPachwinowa","zgon","infekcjeOkolooperacyjnie","cukrzyca","Nikotynizm")))

// COMMAND ----------

//takNieCasted.write.mode("overwrite").format("csv").option("delimiter", "\t").save("/FileStore/tables/studyGrScalaA.csv") 

takNieCasted.coalesce(1).write.mode("overwrite").option("delimiter", "\t").format("com.databricks.spark.csv").option("header", "true").save("/FileStore/tables/studyGrScalaA.csv")



// COMMAND ----------

// MAGIC %md
// MAGIC #Defining divisions
// MAGIC For convinience we also defined some divisions that will make working with the wide table easier

// COMMAND ----------

//list of columns with categorical values 
val categoricalValuesCols = List("Płeć","uproszczona klasyfikacja","Material","Rodzaj protezy","skala5Stopnie","skala3Stopnie","imageTypeOurClassification")



// COMMAND ----------

//here we will define some column names that will create some logical subdivision of data making the analysis of such a wide table easier we will store names of columns for each subdivision in a sepatate list
// metric data
val metricData = List("Płeć","Rok urodzenia","Data badania","Data operacji")
// table summarizing the couses of surgery
val surgeryCouses = List("PowodOperacji","tetniakPowodOper","lerichPowodOper","infectionOfPrevious","nieznany")
//some technical values related to study
val technicalData =  List("Glikemia","Podana Aktywnosc")
//basic categories like type of vascular prosthesis ..  
val basicCategories = List("uproszczona klasyfikacja","Material","Rodzaj protezy","imageTypeOurClassification")
// storing some clinical data
val clinicalData = List("Gorączka","bolesność okolicy protezy","widoczny zewnętrzny obrzęk","ucieplenie","CRP(6 mcy)","WBC(6 mcy)","atybiotyki ","atybiotyki jeśli tak od kiedy i co","reoperacje - 1 jeśli tak  ?","znane protezy udowo podkolanowe","Reoperacje (co)49","Wcześniej operowany w danym miejsu","przetokaPachwinowa","zgon","Reoperacje (ilość)","infekcjeOkolooperacyjnie","cukrzyca","choroby przebyte i przewlekłe", "nowotwory (jeśli tak to jaki)","Nikotynizm","Otyłość", "inne choroby", "inne informacje" )
//microbiologic data mainly what types of bacterias were found in given localisation
val microbiologicData = List("wynik posiewu z protezy37","inne posiewy38","proteza dodatni","proteza ujemny","rana +","przetoka +","krew +","krew -")
// computer tomography data
val ctData = List("opicCT","obecność skrzepliny","wzmożenie densyjności tkanek w okolicy protezy","CT bez zmian")
//data about localisation of the focally increased activity
val locOfFoc = List("lokalizacja ogniska podwyższonego gromadzenia","lok - aorta brzuszna","okolica rozwidlenia","lewe ramie","prawe ramie","wholeAscendingAorta","aorta wstępująca przyzastawkowo", "łuk aorty")
// numerical data describing image
val imageNumbers  = List("SUV (max) w miejscu zapalenia","SUV (max) tła","tumor to background ratio")
// describing presence of lack of presence of given  
val imageCharacteristic = List("skala5Stopnie","skala3Stopnie","Nieregularne zarysy","Ogniskowe gromadzenie znacznika","PecherzykiGazu","Skrzeplina w okolicy miejsca podejrzanego o zapalenie","Obszar plynowy w okolicy","wysiekZatarcieTluszczu","Naciek zapalny w okolicy","przetoka ropna","activeLymphNodes")

                      



// COMMAND ----------

// MAGIC %md
// MAGIC We defined divisions now we want to persist them to make them available for futher transformations
// MAGIC **contrGroupDivisions** is its name

// COMMAND ----------

val data = Seq(("categoricalValuesCols", categoricalValuesCols), ("metricData", metricData), ("surgeryCouses", surgeryCouses)
              , ("technicalData", technicalData), ("basicCategories", basicCategories), ("clinicalData", clinicalData), ("microbiologicData", microbiologicData)
              , ("ctData", ctData), ("locOfFoc", locOfFoc), ("imageNumbers", imageNumbers), ("imageCharacteristic", imageCharacteristic))

val rdd = spark.sparkContext.parallelize(data)
val dfFromRDD1 = rdd.toDF("divisionName", "listOfColumnNames")
dfFromRDD1.write.mode("overwrite").format("delta").saveAsTable("studyGroupDivisions") 

