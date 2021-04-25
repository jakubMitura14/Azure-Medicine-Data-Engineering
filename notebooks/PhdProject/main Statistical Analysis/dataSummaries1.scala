// Databricks notebook source
// MAGIC %run /PhdProject/utils

// COMMAND ----------

// MAGIC %md 
// MAGIC #uploading data
// MAGIC Here we will upload all of the data yet prepared - it is possible becouse size of the tables is small; becouse the csv do not alway save well dates we need to keep it updated and becouse of polish signs in the original tables we are stuck in their case with csv

// COMMAND ----------

//study group
val dfStudy = advancedCasting(myImportFileLocalDataBricks("/FileStore/tables/studyGrScalaA.csv"),List("Rok urodzenia","Data badania","Data operacji"),"date", (c=>to_date(col(c)) ))
val divisionsStudy = spark.read.table("studyGroupDivisions")

//control group
val divisionsContr = spark.read.table("contrGroupDivisions")
val dfContr = advancedCasting(myImportFileLocalDataBricks("/FileStore/tables/contrGrScalaA.csv"),List("data badania 1","data wszczepienia stentgraftu","ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy"),"date", (c=>to_date(col(c)) ))

// two time points data
val divisionsTwoPoints = spark.read.table("twoPointStudiesDivisions")
val dfTwoPoints = advancedCasting(myImportFileLocalDataBricks("/FileStore/tables/twoPoinStudyrScalaA.csv"),List("Data badania wcześniejsze","Data badania późniejsze","Data operacji"),"date", (c=>to_date(col(c)) ))



// COMMAND ----------

dfStudy.createOrReplaceTempView("dfStudy")
divisionsContr.createOrReplaceTempView("divisionsContr")
divisionsTwoPoints.createOrReplaceTempView("divisionsTwoPoints")

// COMMAND ----------

// MAGIC %md
// MAGIC here we will combine the data aboout data quality and outliers

// COMMAND ----------

val listOfDataQAndOutliersData =  List(
  spark.read.table("reportDataQualitystudyGrB").withColumn("tableName", lit("study group")),
  spark.read.table("reportDataQualitycontrGrB").withColumn("tableName", lit("control group")),
  spark.read.table("reportDataQualitytwoPointB").withColumn("tableName", lit("two point study"))
).reduce((a,b)=> a.union(b))



// COMMAND ----------

display(listOfDataQAndOutliersData)

// COMMAND ----------

   spark.sparkContext.parallelize( Seq( ("DataQAndOutliers", "Describing quality of data and noting presence of outliers in numerical columns if such are present") //first we add data about the frame itself
  )).toDF("tableName", "tableDescription")
  .withColumn("time_stamp", current_timestamp())// we also add the timestamp of uploading the data
  .write.mode("overwrite").format("delta").saveAsTable("myPhdStatisticsMetaData") // ovewriting old table as the data is not lost 


// COMMAND ----------

createTablesWithMeta ("DataQAndOutliers", "Describing quality of data and noting presence of outliers in numerical columns if such are present", listOfDataQAndOutliersData)

// COMMAND ----------

// MAGIC %md 
// MAGIC #Age analysis
// MAGIC anlyzing age of the patients including time between surgery and study

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temporary View DatesSummary as
// MAGIC with a as(
// MAGIC select  
// MAGIC `Rok urodzenia`,`Data badania` ,
// MAGIC months_between(`Data badania` , `Rok urodzenia`)/12 as ageinYearsWhenStudy,
// MAGIC `Data operacji`,
// MAGIC months_between(`Data operacji` , `Rok urodzenia`)/12 as ageInYearsWhenSurgery, 
// MAGIC months_between(`Data badania` , `Data operacji`) as timeInMonthsBetweenSurgeryAndStudy
// MAGIC from dfStudy)
// MAGIC select 
// MAGIC 'age In Years When Surgery' as `time period` ,mean(ageInYearsWhenSurgery) as `mean`,percentile_approx(ageInYearsWhenSurgery,0.5) as `median` , min(ageInYearsWhenSurgery) as `min`, max(ageInYearsWhenSurgery) as `max` 
// MAGIC from a
// MAGIC Union
// MAGIC select 
// MAGIC 'age in years When Study' as `time period` ,mean(ageinYearsWhenStudy) as `mean`,percentile_approx(ageinYearsWhenStudy,0.5) as `median`,  min(ageinYearsWhenStudy) as `min`, max(ageinYearsWhenStudy) as `max` 
// MAGIC from a
// MAGIC Union
// MAGIC select 
// MAGIC 'time In Months Between Surgery And Study' as `time period` ,mean(timeInMonthsBetweenSurgeryAndStudy) as `mean`,percentile_approx(timeInMonthsBetweenSurgeryAndStudy,0.5) as `median`, min(timeInMonthsBetweenSurgeryAndStudy) as `min`, max(timeInMonthsBetweenSurgeryAndStudy) as `max` 
// MAGIC from a;
// MAGIC select * from DatesSummary

// COMMAND ----------

createTableCategorized(tableName = "DatesSummary", 
                       tableDescription= "summary of data in study group", 
                      listOfAggr = List((myColumnMedian _, "median"), (myColumnMin _, "min"), (myColumnMax _ , "max")),
                       frameWithData= dfStudy 
                      .withColumn("ageinYearsWhenStudy", months_between(col("Data badania"),col("Rok urodzenia") )/12)
                       .withColumn("ageInYearsWhenSurgery",months_between(col("Data operacji"),col("Rok urodzenia")  )/12)
                       .withColumn("timeInMonthsBetweenSurgeryAndStudy",months_between(col("Data badania" ),col("Data operacji")))
                       ,analyzedColumnNames= List(
                                                  ("ageinYearsWhenStudy","ageinYearsWhenStudy")
                                                  ,("ageInYearsWhenSurgery" , "ageInYearsWhenSurgery")
                                                  , ("timeInMonthsBetweenSurgeryAndStudy","timeInMonthsBetweenSurgeryAndStudy" ))
                       , categoriesColumnNames = List())

// COMMAND ----------

// MAGIC %md 
// MAGIC #couses of surgery in study group

// COMMAND ----------



val listOfAggr = List((mySum _, "count"))
val analyzedColumnNames= List(
  ("tetniakPowodOper","AorticAneurysm")
  ,("lerichPowodOper" , "VascularInsuficiency")
  , ("infectionOfPrevious","infectionOfPrevious" )
,  ("nieznany", "uknown")
)
val categoriesColumnNames = List( ("uproszczona klasyfikacja", "simplifiedClassification" ) , ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification")
                                )

createTableCategorized("surgeryCouses", "couses Of surgery divided by categories", dfStudy, listOfAggr,analyzedColumnNames, categoriesColumnNames)



// COMMAND ----------

// MAGIC %md
// MAGIC #Types and material of prosthesis divided by gender

// COMMAND ----------

display(dfStudy.select("uproszczona klasyfikacja"))

// COMMAND ----------


val innerDfStudy = dfStudy
.withColumn("Females" , when(col("Płeć")==="Kobieta", "Female").otherwise(null)  )
.withColumn("Males" , when(col("Płeć")==="Mężczyzna", "Male").otherwise(null)   )

.withColumn("StentGraft" , when(col("Rodzaj protezy")==="StentGraft", "StentGraft").otherwise(null)  )
.withColumn("Proteza" , when(col("Rodzaj protezy")==="Proteza", "Proteza").otherwise(null)   )

.withColumn("ThoracicAorta"  , when(col("uproszczona klasyfikacja")==="aorty piersiowej", "ThoracicAorta").otherwise(null)  )
.withColumn("Y" , when(col("uproszczona klasyfikacja")==="ob. nacz. biodrowe", "Y").otherwise(null)   )
       

val listOfAggr = List((myCount _, "count"))
val analyzedColumnNames= List(
  ("Females","Females")
  ,("Males" , "Males")
  , ("StentGraft","StentGraft" )
    , ("Płeć","All" )
    , ("Proteza","Proteza" )
    , ("ThoracicAorta","ThoracicAorta" )
    , ("Y","Y" )
)
val categoriesColumnNames = List( ("uproszczona klasyfikacja", "simplifiedClassification" ),  ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification"),("Material","Material"))

createTableCategorized("MaterialEtcPerGender", "Types and material of prosthesis divided by gender", 
                       innerDfStudy
                                  
                       
                       , listOfAggr,analyzedColumnNames, categoriesColumnNames)




// COMMAND ----------

// MAGIC %md
// MAGIC #Other Risk factors
// MAGIC Like other invasive procedures diabetes...

// COMMAND ----------

display(getFrameWithCols(dfStudy, List("Wcześniej operowany w danym miejsu","przetokaPachwinowa","zgon","infekcjeOkolooperacyjnie","cukrzyca","Nikotynizm")))

// COMMAND ----------

val listOfAggr = List((myCountTrues _, "countTrue"))
val analyzedColumnNames= List(
  ( "znane protezy udowo podkolanowe","FemoroPoplitealProsthesis")
  ,("Wcześniej operowany w danym miejsu" , "PreviousSurgeriesThisLocation")
  ,("cukrzyca","Diabetes" ),("zgon", "death"),("Gorączka", "fever")
)
val categoriesColumnNames = List( ("uproszczona klasyfikacja", "simplifiedClassification" ),  ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification") )

createTableCategorized("OtherRiskFactors", "Other Risk factors Like other invasive procedures diabetes...", dfStudy, listOfAggr,analyzedColumnNames, categoriesColumnNames)


// COMMAND ----------

// MAGIC %md
// MAGIC #Labolatory inflammatory Studies

// COMMAND ----------


val listOfAggr = List((myColumnMedian _, "median"), (myColumnMin _, "min"), (myColumnMax _ , "max"))
val analyzedColumnNames= List(
  ( "CRP(6 mcy)","CRP")
  ,("WBC(6 mcy)" , "WBC"))

val categoriesColumnNames = List( ("uproszczona klasyfikacja", "simplifiedClassification" ),  ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification"))

createTableCategorized("LabolatoryInflammation", "Labolatory inflammatory Studies", dfStudy, listOfAggr,analyzedColumnNames, categoriesColumnNames)


// COMMAND ----------

// MAGIC %md
// MAGIC #microbiologic Studies study Group

// COMMAND ----------

createTableCategorized(tableName = "MicrobiologicDataStudyGroup", 
                       tableDescription= "Microbiologic data about two study group", 
                       frameWithData= dfStudy                 
                       , listOfAggr = List((mySum _ , "myCountTrues"))
                       ,analyzedColumnNames= List(
                                                  ("proteza dodatni","ProsthesisPlus"),
                                                 ( "proteza ujemny","ProsthesisMinus"),
                                                  ( "rana +","WoundPlus"),
                                                  ( "przetoka +","FistulaPlus"),
                                                 (  "krew +","BloodPlus"),
                                                  (  "krew -","BloodMinus"),
                                             )
                       , categoriesColumnNames = List())


// COMMAND ----------

// MAGIC %md 
// MAGIC #Basic Data about Two Point Study

// COMMAND ----------

createTableCategorized(tableName = "BasicDataTwoPointStudy", 
                       tableDescription= "Basic data about two point study", 
                       frameWithData= dfTwoPoints
                                        .withColumn("Females" , when(col("Płeć")==="Kobieta", "Female").otherwise(null)  )
                                          .withColumn("Males" , when(col("Płeć")==="Mężczyzna", "Male").otherwise(null)   )    
                                          .withColumn("Diabetes" , when(col("cukrzyca")===1, 1).otherwise(null)   )                    

                       , listOfAggr = List((myCount _, "count"))
                       ,analyzedColumnNames= List(
                                                  ("Females","Females")
                                                  ,("Males" , "Males")
                                                 ,("Diabetes","Diabetes")
                                              
                                                  , ("Płeć","All" ))
                       , categoriesColumnNames = List())


// COMMAND ----------

// MAGIC %md 
// MAGIC # Basic Characteristic Control Group

// COMMAND ----------

createTableCategorized(tableName = "BasicInControlGroup", 
                       tableDescription= "Basic Characteristic of control group - Genders", 
                       frameWithData= dfContr 
                      .withColumn("Females" , when(col("Płeć")==="Kobieta", 1).otherwise(null)  )
                                          .withColumn("Males" , when(col("Płeć")==="Mężczyzna", 1).otherwise(null)   )
                                           .withColumn("Diabetes" , when(col("cukrzyca")===true, 1).otherwise(null)   )        

                                         .withColumn("Alll", lit(1))
                       , listOfAggr = List((mySum _, "count"))
                       ,analyzedColumnNames= List(
                                                  ("Females","Females")
                                                  ,("Males" , "Males")
                                                  ,("Diabetes", "Diabetes")
                                                  , ("Alll","Alll" ))
                       , categoriesColumnNames = List(("stentgraft czy proteza", "StengraftOrProsthesis"),("typ","prosthesisLoc"),("skierowany","Indication")))


// COMMAND ----------

createTableCategorized(tableName = "ControlGroupDates", 
                       tableDescription= "summarizing time periods between events in controll group", 
                       frameWithData= dfContr
                                          .withColumn("yearsTillSurgery",year(col("data wszczepienia stentgraftu"))- col("Rok z peselu"))
                                          .withColumn("yearsTillStudy", year(col("data badania 1"))- col("Rok z peselu") )
                                          .withColumn("monthsFromSurgeryToStudy", months_between(col("data badania 1"), col("data wszczepienia stentgraftu") ))
                                          .withColumn("daysFromStudyToLastObservation", datediff(col( "ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy"),col("data badania 1") ))         
                       , listOfAggr = List((myColumnMedian _, "myColumnMedian"), (myColumnMin _, "min"), (myColumnMax _ , "max"))
                       ,analyzedColumnNames= List(("yearsTillSurgery","yearsTillSurgery")
                                                  ,("yearsTillStudy" , "yearsTillStudy")
                                                  , ("monthsFromSurgeryToStudy","monthsFromSurgeryToStudy" )
                                                  , ("daysFromStudyToLastObservation","daysFromStudyToLastObservation" )
                                                 )
                       , categoriesColumnNames = List(("stentgraft czy proteza", "StengraftOrProsthesis"),("typ","prosthesisLoc"))
                       )



// COMMAND ----------

// display(dfContr.select(col( "ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy"),col("data badania 1")))

val ff = dfContr
                                          .withColumn("yearsTillSurgery",year(col("data wszczepienia stentgraftu"))- col("Rok z peselu"))
                                          .withColumn("yearsTillStudy", year(col("data badania 1"))- col("Rok z peselu") )
                                          .withColumn("monthsFromSurgeryToStudy", months_between(col("data badania 1"), col("data wszczepienia stentgraftu") ))
                                          .withColumn("daysFromStudyToLastObservation", datediff(col( "ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy"),col("data badania 1") ))
display(ff.select(col( "ostatnia wizyta pacjenta bez stwierdzonego zakażenia protezy"),col("data badania 1"), col("yearsTillSurgery") , col("yearsTillStudy"),col("monthsFromSurgeryToStudy") ,col("daysFromStudyToLastObservation") ))

// COMMAND ----------

// MAGIC %md
// MAGIC # Localisation of focus of increased uptake

// COMMAND ----------

dfStudy.printSchema

// COMMAND ----------

createTableCategorized(tableName = "StudyGroupLoc", 
                       tableDescription= " Localisation of focus of increased uptake", 
                       frameWithData= dfStudy
                       , listOfAggr = List((mySum _, "sum"))
                       ,analyzedColumnNames= List(("lok - aorta brzuszna","AbdominaAorta" ),
                                                  ("okolica rozwidlenia","Bifurcation" ),
                                                  ("lewe ramie","leftArm" ), 
                                                  ("prawe ramie","rightArm" ),
                                                  ("wholeAscendingAorta","wholeAscendingAorta" ),
                                                  ("łuk aorty", "aorticArch"),
                                                  ("aorta wstępująca przyzastawkowo","ascendingAortaNextToValve" ),("na wysokości spojenia łonowego", "supraPubic")
                                                 )
                       , categoriesColumnNames = List(("uproszczona klasyfikacja", "simplifiedClassification" ),  ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification"))
                       )






// COMMAND ----------

// MAGIC %md
// MAGIC #Analysis Of Suv Study Group

// COMMAND ----------

display(dfStudy.select("tetniakRzekomyObraz"))

// COMMAND ----------

createTableCategorized(tableName = "StudyGroupSuv", 
                       tableDescription= "Analysis Of Suv Study Group", 
                       frameWithData= dfStudy.withColumn("AllPatients",lit(1))
                       , listOfAggr = List( (mySum _ ,"sum"),(myColumnMedian _, "median"), (myColumnMin _ , "min"), (myColumnMax _ , "max")  )
                       ,analyzedColumnNames= List(("SUV (max) w miejscu zapalenia", "SuvInFocus"), 
                                                  ("SUV (max) tła","SuvInBackground"), 
                                                  ("tumor to background ratio", "TBR"), ("AllPatients","AllPatients")
                                                 )
                       , categoriesColumnNames = List(("uproszczona klasyfikacja", "simplifiedClassification" ), ("Rodzaj protezy", "prosthesisType"))
                       )


// COMMAND ----------

// MAGIC %md
// MAGIC #studyGroup image characteristic

// COMMAND ----------


createTableCategorized(tableName = "StudyGroupImageCharacteristic", 
                       tableDescription= "studyGroup image characteristic", 
                       frameWithData= advancedCasting(dfStudy, 
                                                      List("Nieregularne zarysy", "Ogniskowe gromadzenie znacznika", "Pęcherzyki gazu", "Skrzeplina w okolicy miejsca podejrzanego o zapalenie", "Obszar plynowy w okolicy", "wysiekZatarcieTluszczu", "Naciek zapalny w okolicy", "tetniakRzekomyObraz","przetoka ropna", "activeLymphNodes","PecherzykiGazu"), "int", col _  ) .withColumn("Alll", lit(1))
                       
                       , listOfAggr = List((mySum _, "sum"))
                       ,analyzedColumnNames= List(
                         ("Alll", "Evrybody"),
                         ("Nieregularne zarysy","IrregularBorders"),
                         ("Ogniskowe gromadzenie znacznika","FocalAccumulation"),
                         ("PecherzykiGazu","GasBubbles"),
                         ("Skrzeplina w okolicy miejsca podejrzanego o zapalenie","Thrombus"),
                         ("Obszar plynowy w okolicy","Fluid"),
                         ("wysiekZatarcieTluszczu","FluidFatAttenuation"),
                         ("Naciek zapalny w okolicy","InflammatoryMass"),
                         ("przetoka ropna","puruletFistula"),
                         ("activeLymphNodes","activeLymphNodes"),
                         ("tetniakRzekomyObraz", "pseudoAneurysm")
                                                 )
                       , categoriesColumnNames = List(("uproszczona klasyfikacja", "simplifiedClassification" ),  ("Rodzaj protezy", "prosthesisType"),("imageTypeOurClassification","imageTypeOurClassification"))
                       )



// COMMAND ----------

// MAGIC %md
// MAGIC #visual scales study group and Suv

// COMMAND ----------

createTableCategorized(tableName = "SuvVsVisualScales", 
                       tableDescription= "Analysis Of Suv in Study Group categorised on visual scales", 
                       frameWithData= dfStudy.withColumn("Evrybody", lit(1))
                       , listOfAggr = List((mySum _ , "mySum"), (myColumnMedian _, "median") )
                       ,analyzedColumnNames= List(("SUV (max) w miejscu zapalenia", "SuvInFocus"), 
                                                  ("SUV (max) tła","SuvInBackground"), ("Evrybody","Evrybody"),
                                                  ("tumor to background ratio", "TBR")
                                                 )
                       , categoriesColumnNames = List(("skala5Stopnie","FivePointScale"), ("skala3Stopnie","ThreePointScale"),("imageTypeOurClassification","imageTypeOurClassification"),("Material","Material"))
                       )


  



// COMMAND ----------

// MAGIC %md
// MAGIC #Suv max analysis in 2 points study

// COMMAND ----------

createTableCategorized(tableName = "SuvTwoPointStudy", 
                       tableDescription= "Suv max analysis in 2 points study", 
                       frameWithData= dfTwoPoints
                       , listOfAggr = List((myColumnMedian _, "median"), (myColumnMin _ , "min"), (myColumnMax _ , "max")  )
                       ,analyzedColumnNames= List(
                         ("SUV (max) w miejscu zapalenia44" ,"SUVfocusOne"),
                         ("SUV (max) tła45","SuvBackgroundOne"),
                         ("SUV (max) w miejscu zapalenia71","SuvFocusTwo"),
                         ("SUV (max) tła72","SuvBackgroundTwo")
                                                 )
                       , categoriesColumnNames = List()
                       )

// COMMAND ----------

// MAGIC %md
// MAGIC #Visual Scores vs Suv Control Group

// COMMAND ----------

display(dfContr.select("skala5Stopnie"))

// COMMAND ----------

createTableCategorized(tableName = "SuvVsVisualScalesControlGroup", 
                       tableDescription= "Analysis Of Suv in Study Group categorised on visual scales in control group", 
                       frameWithData= dfContr
                       .withColumn("TBR", col("SUV protezy")/col("tło"))
                       .withColumn("Evrybody", lit(1))
                       , listOfAggr = List((mySum _ , "sum"),(myColumnMedian _, "median") )
                       ,analyzedColumnNames= List(("SUV protezy", "SuvInFocus"), 
                                                  ("tło","SuvInBackground"), ("Evrybody","Evrybody"),
                                                  ("TBR", "TBR")
                                                 )
                       , categoriesColumnNames = List(("skala5Stopnie","FivePointScale"), ("skala3Stopnie","ThreePointScale"))
                       )

// COMMAND ----------

// MAGIC %md
// MAGIC #Comparing values of SUVs and TBR in study and control group

// COMMAND ----------

createTableCategorized(tableName = "SuvStudyVsCrontrol", 
                       tableDescription= "comparison of Suv in  control and study group", 
                       frameWithData= dfContr.join(dfStudy)
                       .withColumn("TBR", col("SUV protezy")/col("tło"))
                       , listOfAggr = List((myColumnMedian _, "median"), (myColumnMin _ , "min"), (myColumnMax _ , "max")  )
                       ,analyzedColumnNames= List(("SUV protezy", "SuvInFocusControl"), 
                                                  ("tło","SuvInBackgroundControl"), 
                                                  ("TBR", "TBRcontrol"),
                                                  ("SUV (max) w miejscu zapalenia", "SuvInFocusStudy"), 
                                                  ("SUV (max) tła","SuvInBackgroundStudy"), 
                                                  ("tumor to background ratio", "TBRStudy")
                                                 )
                       , categoriesColumnNames = List()
                       )



// COMMAND ----------

// MAGIC %md
// MAGIC # ct data from study group
// MAGIC In many cases Ct was performed separately to Pet/ct

// COMMAND ----------

display(dfStudy.select("obecność skrzepliny", "tetniakRzekomyCT", "pogrubienie ściany aorty", "poszerzenie w obrębie zespolenia", "naciek zapalny", "wzmożenie densyjności tkanek w okolicy protezy","przetoka", "płyn wokół protezy", "CT bez zmian","pęcherzyki powietrza") )

// COMMAND ----------

createTableCategorized(tableName = "CtDoneBefore", 
                       tableDescription= "ct data from study group In many cases Ct was performed separately to Pet/ct", 
                       frameWithData= dfStudy
                       .withColumn("Alll", 
                                 when(col("obecność skrzepliny").equalTo(1), 1)
                                 .when(col("tetniakRzekomyCT").equalTo(1), 1) 
                                   .when(col("pogrubienie ściany aorty").equalTo(1), 1) 
                                   .when(col("poszerzenie w obrębie zespolenia").equalTo(1), 1) 
                                   .when(col("naciek zapalny").equalTo(1), 1) 
                                   .when(col("wzmożenie densyjności tkanek w okolicy protezy").equalTo(1), 1) 
                                   .when(col("przetoka").equalTo(1), 1) 
                                   .when(col("płyn wokół protezy").equalTo(1), 1) 
                                   .when(col("CT bez zmian").equalTo(1), 1))
                                   .withColumn ("gasBubbles", when(col("pęcherzyki powietrza").equalTo(true),1)
                                    )
                       , listOfAggr = List((mySum _, "sum")) 
                       ,analyzedColumnNames= List(
                         ("Alll", "Alll"),
                         ("gasBubbles","gasBubbles"),
                       ("obecność skrzepliny","thrombus"),
                        ( "tetniakRzekomyCT","pseudoAneurysm"),
                         ("pogrubienie ściany aorty","thickelingOfAortaWall"),
                         ("poszerzenie w obrębie zespolenia","WideningAnastomosis"),
                         ("naciek zapalny","inflammatoryMass"),
                          ("wzmożenie densyjności tkanek w okolicy protezy","increasingDensity"),
                         ("przetoka","fistula"),
                         ("płyn wokół protezy","fluid"),
                         ("CT bez zmian"  ,"noChanges")  
                       )
                       , categoriesColumnNames = List()
                       )


// COMMAND ----------

// MAGIC %md
// MAGIC #technical data of both control and study group

// COMMAND ----------

createTableCategorized(tableName = "TechnicalDataInStudyAndControlGroup", 
                       tableDescription= "giving some technical details to study and control group like given tracer activity and glucose levels", 
                       frameWithData= dfContr.join(dfStudy)
                       , listOfAggr = List((myColumnMedian _, "median"), (myColumnMin _ , "min"), (myColumnMax _ , "max")  )
                       ,analyzedColumnNames= List(("Glikemia", "GlocoseStudyGroup"), 
                                                  ("Podana Aktywnosc","ActivityStudyGroup"), 
                                                  ("glukoza w dniu podania [mg/dl]", "GlocoseControlGroup"), 
                                                  ("aktywnosc w dniu podania [MBq]", "ActivityControlGroup")
                                              
                                                 )
                       , categoriesColumnNames = List()
                       )

