// Databricks notebook source
// MAGIC %run /PhdProject/utils

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



// COMMAND ----------

// MAGIC %md 
// MAGIC We will try to develop here cople hypothesis Tests using first permanova if permanova will show us that there is some indication that in some group we have rejected null hypothesis we will do the permutation or bootstrap tests with structured benjamini hohenberg correction (if we will do just a single test we will resolve just to to permutation or bootstrap  test)

// COMMAND ----------

// MAGIC %md
// MAGIC From https://towardsdatascience.com/bootstrapping-vs-permutation-testing-a30237795970
// MAGIC Hypothesis testing for the presence (or absence) of effects (e.g. whether any effect of a certain kind is present at all, or whether some positive effect is present, or whether some negative effect is present).
// MAGIC Bootstrapping should be used for:
// MAGIC Quantitative hypothesis testing for specific known/expected effects (e.g. was the average life span of the car batteries actually improved by a year or more?).
// MAGIC Determining confidence intervals non-parametrically.

// COMMAND ----------

// MAGIC %md
// MAGIC We need also to check weather dispersions are equal before doing permanova

// COMMAND ----------

// MAGIC %md
// MAGIC # Imaging Characteristics
// MAGIC so first we will look weather SUV values are correlated with the imagiing characteristics

// COMMAND ----------

//first selecting relevant columns and renaming them for convinience and to be able to later save them in delta tables
val columnsImageChars = List( ("Nieregularne zarysy","IrregularBorders"),
                         ("Ogniskowe gromadzenie znacznika","FocalAccumulation"),
                         ("PecherzykiGazu","GasBubbles"),
                         ("Skrzeplina w okolicy miejsca podejrzanego o zapalenie","Thrombus"),
                         ("Obszar plynowy w okolicy","Fluid"),
                         ("wysiekZatarcieTluszczu","FluidFatAttenuation"),
                         ("Naciek zapalny w okolicy","InflammatoryMass"),
                         ("przetoka ropna","puruletFistula"),
                         ("activeLymphNodes","activeLymphNodes"),
                         ("tetniakRzekomyObraz", "pseudoAneurysm"),
                         ("SUV (max) w miejscu zapalenia", "SuvInFocus"), 
                         ("tumor to background ratio", "TBR")).map(it=> col(it._1).as(it._2))
val imagingFrame = dfStudy.select(columnsImageChars:_*)   
imagingFrame.write.mode("overwrite").format("delta").saveAsTable("imagingFrame")
display(imagingFrame)
                                                                        

// COMMAND ----------

// MAGIC %md
// MAGIC # CutOff Age, prosthesis types, basic location ... 
// MAGIC Does age of patient during surgery , type of prosthesis  or its localisation is significantly diffrent between study and controll group
// MAGIC Here We will  also collect SUV and TBR values in controll and study group so we will check the 

// COMMAND ----------

//selecting relevant columns and renaming them for easir reference

val colsInControl = List(                         ("SUV protezy", "SuvInFocus"), 
                                                  ("TBR", "TBR"),    
                                                 ("typ", "simplifiedClassification"  ),
                                                   ("stentgraft czy proteza" , "prosthesisType"),
                         ("ageInYearsWhenSurgery","ageInYearsWhenSurgery"), ("Płeć", "gender")
                                                 ).map(it=> col(it._1).as(it._2))

                                      
                         





val colsInStudy = List(                           ("SUV (max) w miejscu zapalenia", "SuvInFocus"), 
                                                  ("tumor to background ratio", "TBR"),
                                                  ("simplifiedClassification", "simplifiedClassification"  ),
                                                   ("prosthesisType" , "prosthesisType"),
                         ("ageInYearsWhenSurgery","ageInYearsWhenSurgery"), ("Płeć", "gender")
                                                 ).map(it=> col(it._1).as(it._2))

val controlFram = dfContr.withColumn("TBR", col("SUV protezy")/col("tło"))
                         .withColumn("isStudy", lit(0))
                         .withColumn("ageInYearsWhenSurgery",year(col("data wszczepienia stentgraftu"))- col("Rok z peselu"))
                          .select( (List(col("isStudy")) ++ colsInControl ):_* )// adding column for reference wheather it is study or controll group
                          
                          
val studyFrame = dfStudy.withColumn("isStudy", lit(1)) // adding column for reference wheather it is study or controll group
                       .withColumn("ageInYearsWhenSurgery",months_between(col("Data operacji"),col("Rok urodzenia")  )/12)
   .withColumn( "prosthesisType",           regexp_replace(
                        regexp_replace(  col("Rodzaj protezy"),   "StentGraft" ,"stentgraft" )    ,
                      "Proteza" ,"proteza" ) 
                         )                   

.withColumn(  "simplifiedClassification",          regexp_replace(
                        regexp_replace(  col("uproszczona klasyfikacja"),   "ob. nacz. biodrowe" ,"Y" )  ,  
                      "aorty piersiowej" ,"B" ) 
                        )    

.select(  (List(col("isStudy")) ++ colsInStudy ):_* )


val numbsFrame = (studyFrame.union(controlFram)).withColumn("isMale" , when(col("Gender")==="Mężczyzna", 1).otherwise(0)  )

createTablesWithMeta ("contrAndStudyNumbsFrame", "Does age of patient during surgery , type of prosthesis  or its localisation is significantly diffrent between study and controll group Here We will  also collect SUV and TBR values in controll and study group so we will check the", numbsFrame)                         




// COMMAND ----------

display(numbsFrame)

// COMMAND ----------

createTablesWithMeta ("contrAndStudyAgeFrame", "age during surgery in study and control group", numbsFrame.select("isStudy","ageInYearsWhenSurgery").where("ageInYearsWhenSurgery is not null"))  

// COMMAND ----------

// MAGIC %md
// MAGIC # Authors scale
// MAGIC Here We will collect data needed to check weather there is correlation between SUV max  and labolatory data ... 

// COMMAND ----------

val aScale = dfStudy.select( col("SUV (max) w miejscu zapalenia").as("SuvInFocus"), 
                             col("tumor to background ratio").as("TBR"),
                             col( "CRP(6 mcy)").as("CRP"),
                            col("WBC(6 mcy)").as("WBC"),
                            col("imageTypeOurClassification")
                           )

aScale.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("aScale")
display(aScale)