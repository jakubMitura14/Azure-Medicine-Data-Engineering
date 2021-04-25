# Work in progress

# Azure-Medicine-Data-Engineering
## basic description 
Data science project based on data about patients with infected vascular grafts, and Azure tools (including DataBricks))
## Azure Synapse Analytics graph

![image](https://user-images.githubusercontent.com/53857487/115989538-2a2f1f80-a5bf-11eb-9ca8-8f0a6a84d815.png)

As we see on the graph we first copy data from input (excel) file to a format that can be consumed by the databricks, databricks notebook first clean data than check its quality and look for any outliers,, to create data summaries and perform statistical hypothesis testing. It is also shown how the ML pipelinee can be connected, actual code of the jupiter notebook with model training will be described below.




# Key Vault
In order to keep safely all keys all keys and certificates are stored in Azure Key vault 

## Access policies
First step is to provide Access policies  for all required services like Synapse Analytics , Azure MAchine learning etc.

![image](https://user-images.githubusercontent.com/53857487/115953879-59c22700-a4ee-11eb-9866-74976330c04d.png)


## Secrets
most important secrets are to DataBricks and storage

##krowa

## DataBricks integration
In order to enable integration of Databricks with azure keyvoult we need a premium account and also access the create scope in the Databricks via pasting appropriate url as shown below

![image](https://user-images.githubusercontent.com/53857487/115957830-07d8cb80-a505-11eb-9a0f-eb05fe62fc5b.png)

after passing appropriate vault Uri and resource id we get information confirming success

![image](https://user-images.githubusercontent.com/53857487/115958081-255a6500-a506-11eb-9ed1-8252302e4585.png)

## Azure ML integration
Data needed for integration of synapse analytics and azure machine learning can be accessed via cloud shell
![image](https://user-images.githubusercontent.com/53857487/115988989-48475080-a5bc-11eb-89a1-646191a40e2f.png)

![image](https://user-images.githubusercontent.com/53857487/115988993-4e3d3180-a5bc-11eb-9604-dc49413a6d08.png)

## all integrations
all integrations needed in the azure synapse analytics are shown and summarized on the printscreen below

![image](https://user-images.githubusercontent.com/53857487/115989792-5eefa680-a5c0-11eb-8a93-c6e6a05e16fc.png)

# Information Flow
## storage
Information is stored in blob container in a storage Account, as data is in clinical setting written into excel files this is a data that is uploaded 
![image](https://user-images.githubusercontent.com/53857487/115954365-ea016b80-a4f0-11eb-902c-83b09aeeb703.png)

## integration datasets
Data from sheets that are intresting us is uploaded to the integration datasets  (we have 3 sheets that we have intrest in  sheet with data baout study group , with data about controll group, and about studies that were performed ) below shown example how such integration dataset is configured

![image](https://user-images.githubusercontent.com/53857487/115960398-76bc2180-a511-11eb-874b-859a33a9fce8.png)


## copy data
Next we need to copy data from datasets to appropriate CSV files (databricks are configured to load csv). In order to achieve this we use copy data activity for each sheet, below example of configuration

![image](https://user-images.githubusercontent.com/53857487/115960486-f944e100-a511-11eb-98f6-d68abdcadd9b.png)


![image](https://user-images.githubusercontent.com/53857487/115960489-ff3ac200-a511-11eb-9aed-ee36e6ea68fd.png)

### outputData from copy data activity

In order to  parse corretly the data that we have we formatted the output datasets (output from copy data activity)


![image](https://user-images.githubusercontent.com/53857487/115965234-7d09c800-a528-11eb-9adb-10d464dccbe6.png)


The effect of this operation is to put into linked blob storage the proper files 

![image](https://user-images.githubusercontent.com/53857487/115965356-251f9100-a529-11eb-9ebb-c281c879fb7a.png)

##  Databricks blob integration

Now we nheed to connect the Databricks into our blob storage the access to the azure keyvoult was already established hence we will use it here
First we need to define all necessary constance
```
val containerName = "mainblob"
val storageAccountName = "vascpathstorage"
val key = dbutils.secrets.get(scope= "myblobkv", key = "stor2")

val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.account.key." + storageAccountName + ".blob.core.windows.net"

```
Then If storage is not yet mounted we mount it

```
val myMountPoint = "/mnt/myblob"

if(!dbutils.fs.mounts().map(it=>it.mountPoint).contains(myMountPoint)){
dbutils.fs.mount(
  mountPoint =myMountPoint,
  source = url,
  extraConfigs = Map(config -> key))

}
display(dbutils.fs.ls("/mnt/myblob"))

```


In the end to access the necessary file we can use the function from utils notebook 

```
def myImportFile(fileName : String) : DataFrame =  {
return spark.read.
    format("csv").
    option("header", "true").
    option("delimiter", "\t").
    option("header", "true").
    option("inferSchema", "true").load("dbfs:/mnt/myblob/" + fileName)
}

```

#  Data Cleaning

In order to deal with irregularities of data we need to properly parse the columns as shown below (functions can be found also in utils files)

```
  /*
*@description it takes dataframe some column list that should be modified and casts it after some preparation to new type
*@param frame source frame that we want to change
*@param columnList list of column names we want to modify
*@param castTo string representing the type to which we want to cast given functions
*@param prepareFunction what we need to do with the values that we already have in order to be able to cast it to target values we assume that the value that we work on is always String
* A - type to which we want modify the values in given columns
*@return new dataframe where the values in given columns have proper class
*/
def advancedCasting(frame: DataFrame, columnList: List[String], castTo:String, prepareFunction : (String)=>Column) : DataFrame = {
  return frame.select(frame.columns.map{c => if(columnList.contains(c)){prepareFunction(c).cast(castTo).as(c)} else {col(c)}}    :_*)
}
/**
*@description given old dataframe it applies transformation changes te column to boolean with boolean true if the value of a column equals trueString when value is null it stays null
*@param frameName name of frame which we want to modify
*@param frame source frame that we want to change
*@param trueString if the column value is equal to this it will be true
*@param listOfCols list of columns names that we want to modify
*/
def booleanCastingIfString (frame : DataFrame, frameName : String, trueString : String,listOfCols : List[String] ) : DataFrame = {
val primListt =  listOfCols.map(colName=> s""" lower(string(`$colName`)) = "${trueString}" as  `$colName` """)
return spark.sql("select "+ (primListt ++ frame.columns.filterNot(it=>listOfCols.contains(it)).map(it=>s"`${it}`") ).mkString(",") + s"from $frameName")
  
}
```


this will be applied to manually defined inside the databricks notebooks steps pointed out on the picture below

![image](https://user-images.githubusercontent.com/53857487/115966448-e50edd00-a52d-11eb-8ca8-99ccb7297946.png)


# Data quality and outliers
One more important step to complete before applying statistical analysis is to properly asses the quality of data  
1) the amount of null values in columns where it should not be present
2) presence or absence of outliers in this case measured with z score 
3) wheathe numerical data is within manually defined bounds (that are set on the basis of domain knowledge)

For obvius reasons the columns and bounds needed to be chosen manually, all of the results would be saved into the delta table. The functions that are used in order to achieved are stored in utils notebook and also presented below

```
/**
*@description check weather given value is within given numerical constraintsif not appends to the report table
*@param frame Dataframe from which we want to get data to check weather they are in given constraints
*@param columnNames list of column names that we want to test 
*@param lowerTreshold  lower treshold of acceptable values
*@param upperTreshold upper treshold of acceptable values
*@return list of tripples where first entry is specyfing what is wrong in this function hardcoded as "outside of the range" ; than column name and numer of records that are not in accepted range
*/
def countOutsideTheRange(frame : DataFrame ,columnNames : List[String] ,lowerTreshold : Double , upperTreshold : Double ) :List[(String,String,Long)] = {
  return columnNames.map(colName=> ("outside of the range", colName, frame.select(col(colName)).where(s""" `$colName` < $lowerTreshold OR  `$colName` > $upperTreshold  """).count() ) )}


/**
*@description check weather given value is not null
*@param frame Dataframe from which we want to get data to check weather they are in given constraints
*@param columnNames list of column names that we want to test 
*@param lowerTreshold  lower treshold of acceptable values
*@param upperTreshold upper treshold of acceptable values
*@return list of tripples where first entry is specyfing what is wrong in this function hardcoded as "number of null values" ; than column name and numer of records that are null
*/
def countHowManyNulls(frame : DataFrame ,columnNames : List[String] ) :List[(String,String,Long)] = {
  return columnNames.map(colName=> ("number of null values", colName, frame.select(col(colName)).where(isnull(col(colName))).count() ) )  
}

/**
*@description  check weather the z score of given column is above or below 3 
*@param frame Dataframe from which we want to get data to check weather we have outliers
*@param columnNames list of column names that we want to test 
*@return list of tripples where first entry is specyfing what is wrong in this function hardcoded as "oultlier number" ; than column name and numer of records that are outliers according to the z score
*/
def countOutliersOfZscore (frame : DataFrame ,columnNames : List[String] ) :List[(String,String,Long)] = {
return columnNames.map{colName=> 
  val outliersnumb = frame
  .withColumn("mean",avg(s"$colName").over())
  .withColumn("stddev",callUDF("stddev_pop",col(s"$colName")).over())
  .withColumn("z-score",(col(s"$colName")-$"mean")/$"stddev") 
  .where(" `z-score` >3 OR `z-score` < (-3)" ).count()
 ("oultlier number", colName,outliersnumb   )
}
}

```

# Data Summaries

Now when data is prepared summeries will be created  in order to achieve this goal multiple utility functions were defined

```
*table names with given desctiption will also be added to the myPhdStatistics meta data
so what is importan it will also create appropriate delta table
*@patam tableName name of table
*@param tableDescription String that will be recorded in metadata and that will represent the description of data about this table
*@param frameWithData dataframe with data we calculated
*/
def createTablesWithMeta (tableName: String, tableDescription : String, frameWithData : DataFrame) {
  //below we are creating new metadata frame as union of old data we use union to avoid duplicates
   spark.sparkContext.parallelize( Seq( (tableName, tableDescription) //first we add data about the frame itself
  )).toDF("tableName", "tableDescription")
  .withColumn("time_stamp", current_timestamp())// we also add the timestamp of uploading the data
  .union(spark.read.table("myPhdStatisticsMetaData")) // union with data that was already there
  .write.mode("overwrite").format("delta").saveAsTable("myPhdStatisticsMetaData") // ovewriting old table as the data is not lost 
  //  and we write the frame itself to delta table
  frameWithData.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(tableName)  
}

/**
*@description most of the summaries will be described after dividing it into diffrent categories like type of vascular prosthesis, its localisation and pattern of image 
so we will have as a paramater list of aggregation functions and we will apply those first over all of the data in given columns and then over partitions genereted by supplied categorical columns
*@patam tableName name of table
*@param tableDescription String that will be recorded in metadata and that will represent the description of data about this table
*@param frameWithData dataframe with data we calculated
*@param listOfAggr list of aggregation functions and added name that will be applied to all specified columns 
*@param analyzedColumnNames list of names we are intrested in to check and how we want to name appropriate columns (the first in a tuple list will be column names and second a  new name for coluimn summarizing ... )
*@param categoriesColumnNames list of column names with categorical data that we will use to generate divisons data will be stored in tuples where first part will be name of the column and second the string that will be added to the row where data about those divisions is added
*/
def createTableCategorized (tableName: String,tableDescription: String, frameWithData : DataFrame, listOfAggr : List[(Column=>Column, String)], analyzedColumnNames : List[(String, String)], categoriesColumnNames : List[(String, String)]) {

 val categorized =  (List(("All","All")) ++categoriesColumnNames).map{categoryInfo=>  
    listOfAggr.map{ locAggr=>// aggregation function and the name of this aggregation 
   frameWithData.select( setModificationToCol (categoryInfo, locAggr,analyzedColumnNames )
                       :_*).distinct()   }//end listOfAggr
    }.flatten.reduce((a,b)=> a.union(b))// we accumulate all frames
  display(categorized)
  createTablesWithMeta(tableName,tableDescription,categorized )
}


/**
helper function to createTableCategorized it will create list of columns objects that will be used in select statement
*@param categoryInfo tuple with name of the column with the category  and its description
*@param locAggr method of aggregation and its description
*@param analyzedColumnNames list of names we are intrested in to check and how we want to name appropriate columns (the first in a tuple list will be column names and second a  new name for coluimn summarizing ... )
*@return return list of column objects 
*/
def setModificationToCol (categoryInfo : (String, String), locAggr: (Column=>Column, String), analyzedColumnNames : List[(String, String)] ) : Seq[Column] = {
       //we need to return diffrent thing in case we are in All category
      if(categoryInfo._1!="All"){
       return (Seq(lit(categoryInfo._2).as("Division"),
      col(categoryInfo._1).as("DivisionCategory"),
      lit(locAggr._2).as("aggregation")) ++       
      analyzedColumnNames.map(colNameInfo=>  locAggr._1(    col(colNameInfo._1) )  //applying aggregations
                                                       .over(Window.partitionBy(categoryInfo._1) )  //defining window over which we will execute aggregation functions if it is not empty if it is we basically do nothing with window as this mean we want to get all   
                                                         .as(colNameInfo._2)))// renaming
                                } else{
      return(Seq( lit("All").as("Division"),
     lit("All").as("DivisionCategory"),
      lit(locAggr._2).as("aggregation")) ++ 
      analyzedColumnNames.map(colNameInfo=>  locAggr._1(    col(colNameInfo._1) )  //applying aggregations
                                                        .as(colNameInfo._2)))// renaming
        
      }                     
  
}

/********************  simple casted column functions   *****************************/

//percentile_approx from https://www.programmersought.com/article/34051525009/

  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr,  percentage.expr, accuracy.expr
    ).toAggregateExpression
    new Column(expr)
  }
  def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )



//the table is immaterial here
//val anyDataFrame =  spark.table("myPhdStatisticsMetaData")
def  myColumnMedian (c : Column) : Column =  percentile_approx(c, lit(0.5))
def mySum (c : Column) : Column = sum(c)
    
def  myCountTrues (c : Column) : Column = sum(regexp_replace(regexp_replace(c , lit(true), lit(1)),  lit(false), lit(0)))

def myColumnMin (c : Column) : Column = functions.min(c)
 def myColumnMax (c : Column) : Column = functions.max(c)
def  myCount (c : Column) : Column = count(c)// wrapper to get around compiler uncertainity


```
Basically depending on the context multiple features were analyzed and compared  divided ussually on the basis of some categorical data like for example visual scales described in dedical literature, below example of such aggregation. Also as can be seen in the utility functions  all data will be saved to the respective delta tables and metadata about those tables will also be aggregated in meta data delta table. Example of such aggregation below where we compare the SUV max and tumor to background ration in control and study groups. All of the analyzed  features can be found in dataSummaries1 notebook.


```
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
```

# Preparing data for Hypothesis testing and predictive statistics

Some a bit more complicated aggregations are performed in HypothesisTestingPrediction notebook for example below we are standardizing column names between study and control groups in order to be able to later efficiently compare diffrences in characteristics of those groups

```
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



```


# Hypothesis testing
Hypothesis testing was performed in R as the libraries for hypothesis testing is richer in this enviroment, all of the code is available in "Hypothesis testing R" notebook .
Multiple two sided hypotheses were tested using in most cases non parametric permutation tasts with structure‐adaptive Benjamini–Hochberg algorithm [1] (algorithm copied from authors repository). 

```
# based on the implementation from perm test https://cran.r-project.org/web/packages/perm/perm.pdf
myPermTest <- function(booleansColName, numbVectorColName, frame) {
  framee <-  frame %>% dplyr::select(booleansColName,numbVectorColName)  %>% dplyr::filter( !is.na(.[1]))%>% dplyr::filter( !is.na(.[2]))   
  numbVector <- framee[[numbVectorColName]]
  booleans<- framee[[booleansColName]]
  
trueOnes <-numbVector[booleans]
falseOnes <-numbVector[!booleans]
if(length(falseOnes)>1){
permTS(trueOnes , falseOnes)$p.values[1][["p.twosided"]]   } else{2}

}
#myPermTest("FocalAccumulation", "SuvInFocus", imagingFrame)


#Implementation of structure aware BH we supply p values and labels both vectors should be of the same length
myBH <- function (pValues,labels) {
n = length(pValues) # here important  we need to 
BH_Res = Storey_Res = SABHA_Res = rep(0,n)

############## set of parameters copied from fMRI example
alpha = 0.05 # target FDR level
tau = 0.5; eps = 0.1 # parameters for SABHA
ADMM_params = c(10^2, 10^3, 2, 15000, 1e-3) # alpha_ADMM,beta,eta,max_iters,converge_thr

# gather results
labels[SABHA_Res ==1]


qhat = Solve_q_block(pValues,tau,eps,labels,ADMM_params)

# it returns a vector with the same order as supplied at the beginning in this vecto when we did not achieved significance we get 0 when we did we get 1
SABHA_Res[SABHA_method(pValues,qhat,alpha,tau)] = 1
#selecting from labels those that are significantly significant 

labels[SABHA_Res ==1]
}

```

permanova function was also attempted yet becouse of results of beta dispersion the results were not included

```
# performing the Permanova - method detecting weather there is a relation between some value and group of other values
# We will also check First the beta dispersion  to say weather it is sufficiently small in order to be able to still in a valid way perworm permanova
# @param mainFrame {DataFrame} representin all data we are analyzing
# @param referenceColumnName {String} name of column to which we want to check weather it has a significant correlation with all other columns (so reference column holds dependent variable)
# returns {List} return p value of permanova and p value related of beta dispersion in order for the test to be valid we need 
myPermanova <- function (mainFrame,referenceColumnName) {
reducedFr <-  mainFrame %>% dplyr::select(-all_of(referenceColumnName)) # %>% as.matrix() %>% sqrt() # square root transformation in order to reduce the effect of strongest values

  parmaNov<-adonis(reducedFr ~ mainFrame[[referenceColumnName]], perm=999)
  #calculating permanova p value 
  permanovaPValue <- as.data.frame(as.data.frame(parmaNov$aov.tab)[6])[1,1]

  dist<-vegdist(reducedFr, method='jaccard')
  dispersion<-betadisper(dist, group=mainFrame[[referenceColumnName]])
  
  c(permanovaPValue, permutest(dispersion) )
  
  
}

```
In case of the binary data to perform hypothesis fisher test was used

The analysis of optimal treshold cut off value of SUV max and TBR was also performed, minimizing both false postive and false negative results in this clinical setting was considered equally important.

```
#SUV max analysis
trueOnesSUV <-controlVsStudyFrame$SuvInFocus[controlVsStudyFrame$isStudy]
falseOnesSUV <-controlVsStudyFrame$SuvInFocus[!controlVsStudyFrame$isStudy]
#TBR analysis
trueOnesTBR <-controlVsStudyFrame$TBR[controlVsStudyFrame$isStudy]
falseOnesTBR <-controlVsStudyFrame$TBR[!controlVsStudyFrame$isStudy]

tresholdSuv<- thres2(trueOnesSUV,falseOnesSUV,0.01 )[[1]]$thres
tresholdTBR<- thres2(trueOnesTBR,falseOnesTBR,0.01 )[[1]]$thres

```

Additionaly associative rules were analyzed to establish important coocurences of imaging characteristics in study group 

```
rules<- apriori(trans,parameter=list(supp=0.3,conf=.80, minlen=3,maxlen=7, target='rules')) # run a priori algorithms
arules<-rules
```

# Machine Learning
Predictive statistics was performed using decision tree model and scikit learn in azure ML. Additionally in order to make a model axplainable the feature importance was analyzed using appropriate software packages.

All of the modelling was performed in azere ML using Jupiter notebook and Python(3.6 or 3.8). 

First data was futher preapared to properly deal with categorical data and null values

```
def prepareFrame (df):
    #filling nulls with mean
    df= df.fillna(df.mean())

    # Normalize the numeric columns
    scaler = MinMaxScaler()
    num_cols = ['SuvInFocus','TBR','ageInYearsWhenSurgery']
    df[num_cols] = scaler.fit_transform(df[num_cols])
    #setting categorical columns to boolean  - as here we basically care only about two columns
    df.loc[df['prosthesisType'] == "stentgraft", 'isStentgraft'] = True 
    df.loc[df['simplifiedClassification'] == "Y", 'isY'] = True 

    df.loc[df['prosthesisType'] != "stentgraft", 'isStentgraft'] = False 
    df.loc[df['simplifiedClassification'] != "Y", 'isY'] = False 


    df["isMale"] = df["isMale"].astype(bool)
    return df


mainPdFrame= pd.DataFrame(data= prepareFrame(mainPdFrame))
mainPdFrame


```

We need aslo to define the main code that will be then reused for hyperparameter tuning and for interpreting a model. This cell will create a parametrized python script that with given hyperparameters will train decision tree classifier with hyperparameters specified in parameters of script.

```
%%writefile $experiment_folder/vascProsth_training.py
# this is important as this cell will be written to separate python file
# Import libraries


import argparse, joblib, os
from azureml.core import Run
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.preprocessing import MinMaxScaler

from sklearn.tree import DecisionTreeClassifier




def prepareFrame (df):
    #filling nulls with mean
    df= df.fillna(df.mean())

    # Normalize the numeric columns
    scaler = MinMaxScaler()
    num_cols = ['SuvInFocus','TBR','ageInYearsWhenSurgery']
    df[num_cols] = scaler.fit_transform(df[num_cols])
    #setting categorical columns to boolean  - as here we basically care only about two columns
    df.loc[df['prosthesisType'] == "stentgraft", 'isStentgraft'] = True 
    df.loc[df['simplifiedClassification'] == "Y", 'isY'] = True 

    df.loc[df['prosthesisType'] != "stentgraft", 'isStentgraft'] = False 
    df.loc[df['simplifiedClassification'] != "Y", 'isY'] = False 


    df["isMale"] = df["isMale"].astype(bool)
    return df





# Get the experiment run context
run = Run.get_context()

#learning_rate = 0.1
#n_estimators =100

################ part below to set arguments when this python file will be called

# Get script arguments
parser = argparse.ArgumentParser()

# Input dataset
parser.add_argument("--input-data", type=str, dest='input_data', help='training dataset')

############ Hyperparameters


#from https://medium.datadriveninvestor.com/decision-tree-adventures-2-explanation-of-decision-tree-classifier-parameters-84776f39a28
#max_depth: int or None, optional (default=None) [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]


#min_samples_split: int, float, optional (default=2) [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]


#min_impurity_decrease   [0.00005,0.0001,0.0002,0.0005,0.001,0.0015,0.002,0.005,0.01]


parser.add_argument('--max_depth', type=int, dest='max_depth', default=15, help='max_depth')
parser.add_argument('--min_samples_split', type=int, dest='min_samples_split', default=2, help='min_samples_split')
parser.add_argument('--min_impurity_decrease', type=float, dest='min_impurity_decrease', default=0.00005, help='min_impurity_decrease')
parser.add_argument('--isToExplain', type=bool, dest='isToExplain', default=False , help='true if it is a model we want to interpret - the best model')

# Add arguments to args collection
args = parser.parse_args()



mainPdFr = run.input_datasets['training_data'].to_pandas_dataframe() # Get the training data from the estimator input
mainPdFr= prepareFrame(mainPdFr)
#################### preapre data

# Separate features and labels
X, y = mainPdFr[
    ['SuvInFocus','TBR','isStentgraft','isY','ageInYearsWhenSurgery'
     ,'isMale']].values, mainPdFr['isStudy'].values

# Split data into training set and test set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=0)


################ model
model = DecisionTreeClassifier(max_depth =args.max_depth,
                                min_samples_split= args.min_samples_split,
                               min_impurity_decrease= args.min_impurity_decrease
                                                       ).fit(X_train, y_train)


########################## metrics


# calculate accuracy
y_hat = model.predict(X_test)
acc = np.average(y_hat == y_test)
print('Accuracy:', acc)
run.log('Accuracy', np.float(acc))

# calculate AUC
y_scores = model.predict_proba(X_test)
auc = roc_auc_score(y_test,y_scores[:,1])
print('AUC: ' + str(auc))
run.log('AUC', np.float(auc))

######################## Save the model in the run outputs
os.makedirs('outputs', exist_ok=True)
joblib.dump(value=model, filename='outputs/vascProsth_model.pkl')

########## explanations used only in best case
if(args.isToExplain):
    from interpret.ext.blackbox import TabularExplainer
    import os, shutil
    from azureml.interpret import ExplanationClient
    # Get explanation
    explainer = TabularExplainer(model, X_train, features=['SuvInFocus','TBR','isStentgraft','isY','ageInYearsWhenSurgery'
         ,'isMale'], classes=['noInfection', 'infection'])
    explanation = explainer.explain_global(X_test)

    # Get an Explanation Client and upload the explanation
    explain_client = ExplanationClient.from_run(run)
    explain_client.upload_model_explanation(explanation, comment='Tabular Explanation')



run.complete()
```

### Hyper parameter tuning

After the compute target is established We will perform baysian hyperparameter tuning using the Azure ml library. We use an AUC as a model metric we consider false posiitive and false negative results in this clinical setting as equally undesirable.

```
# Create a Python environment for the experiment
sklearn_env = Environment("sklearn-env")

# Ensure the required packages are installed (we need scikit-learn, Azure ML defaults, and Azure ML dataprep)
packages = CondaDependencies.create(conda_packages=['scikit-learn','pip'],
                                    pip_packages=['azureml-defaults','azureml-dataprep[pandas]'])
sklearn_env.python.conda_dependencies = packages

# Get the training dataset
ds = ws.datasets.get("mainMlDataSet")

# Create a script config
script_config = ScriptRunConfig(source_directory=experiment_folder,
                                script='vascProsth_training.py',
                                # Add non-hyperparameter arguments -in this case, the training dataset
                                arguments = ['--input-data', ds.as_named_input('training_data')],
                                environment=sklearn_env,
                                compute_target = training_cluster)



# Sample a range of parameter values
params = BayesianParameterSampling(
    {
        # Hyperdrive will try  combinations, adding these as script arguments
        '--max_depth': choice(1 ,2,3,4,5,6,7,8,9),
        '--min_samples_split' : choice(2,3,4,5,6,7,8,9,10,11,12,13,14,15),
        '--min_impurity_decrease' : choice(0.00005,0.0001,0.0002,0.0005,0.001,0.0015,0.002,0.005,0.01)
    }
)

# Configure hyperdrive settings
hyperdrive = HyperDriveConfig(run_config=script_config, 
                          hyperparameter_sampling=params, 
                          policy=None, # No early stopping policy
                          primary_metric_name='AUC', # Find the highest AUC metric
                          primary_metric_goal=PrimaryMetricGoal.MAXIMIZE, 
                          max_total_runs=50, # Restict the experiment to 200 iterations
                          max_concurrent_runs=2) # Run up to 2 iterations in parallel

# Run the experiment
experiment = Experiment(workspace=ws, name='mslearn-vascProsth-hyperdrive')
run = experiment.submit(config=hyperdrive)

# Show the status in the notebook as the experiment runs
RunDetails(run).show()
run.wait_for_completion()

```

![image](https://user-images.githubusercontent.com/53857487/115992254-ffe45e80-a5cc-11eb-888c-218ff6e1e0e0.png)

```
DecisionTreeClassifier(ccp_alpha=0.0, class_weight=None, criterion='gini',
                       max_depth=5, max_features=None, max_leaf_nodes=None,
                       min_impurity_decrease=Decimal('0.00005'),
                       min_impurity_split=None, min_samples_leaf=1,
                       min_samples_split=7, min_weight_fraction_leaf=0.0,
                       presort='deprecated', random_state=None,
                       splitter='best')
```
![image](https://user-images.githubusercontent.com/53857487/115992301-37eba180-a5cd-11eb-83fc-bf39d8391d89.png)


## Model interpretability

In order to make model expleinable we choose optimal hyperparameters calculated in a previous step and using the TabularExplainer we are anylizing the features importance

First we use the previously prepared script just now we flag that it needs to include Tabular Interpreter.
```
# Create a Python environment for the experiment
explain_env = Environment("explain-env")

# Create a set of package dependencies (including the azureml-interpret package)
packages = CondaDependencies.create(conda_packages=['scikit-learn','pandas','pip'],
                                    pip_packages=['azureml-defaults','azureml-interpret'])
explain_env.python.conda_dependencies = packages


ds = ws.datasets.get("mainMlDataSet")

max_depth = bestRunDetails[3]
min_samples_split = bestRunDetails[5]
min_impurity_decrease = bestRunDetails[7]

# Create a script config
script_config = ScriptRunConfig(source_directory=experiment_folder,
                      script='vascProsth_training.py'
                      ,arguments=['--input-data', ds.as_named_input('training_data')
                                 ,'--max_depth',max_depth
                                 ,'--min_samples_split',min_samples_split
                                 ,  '--min_impurity_decrease',min_impurity_decrease
                                  ,'--isToExplain',True
                                 ],
                       environment=explain_env) 

# submit the experiment
experiment_name = 'mslearn-vascProsth-explain'
experiment = Experiment(workspace=ws, name=experiment_name)
run = experiment.submit(config=script_config)
RunDetails(run).show()
run.wait_for_completion()


```
After experiment is completed we can look through feature importance
```
# Get the feature explanations
client = ExplanationClient.from_run(run)
engineered_explanations = client.download_model_explanation()
feature_importances = engineered_explanations.get_feature_importance_dict()

# Overall feature importance
print('Feature\tImportance')
for key, value in feature_importances.items():
    print(key, '\t', value)

```

```
Feature	Importance
TBR 	 0.40010683760683763
SuvInFocus 	 0.08764245014245015
isMale 	 0.0
ageInYearsWhenSurgery 	 0.0
isY 	 0.0
isStentgraft 	 0.0

```


![image](https://user-images.githubusercontent.com/53857487/115992320-5c477e00-a5cd-11eb-979e-493b31951eaf.png)



# Visualizations in power Bi
First we need to supply appropriate data for power bi to connect to data in databricks

![image](https://user-images.githubusercontent.com/53857487/115992377-bb0cf780-a5cd-11eb-8edc-9ee9ac711ef3.png)


![image](https://user-images.githubusercontent.com/53857487/115992382-c102d880-a5cd-11eb-9505-baa7d2944ae8.png)


Below some example of powe Bi visualizations

![image](https://user-images.githubusercontent.com/53857487/115993117-9fa3eb80-a5d1-11eb-9e89-6aeee838ebac.png)







1.Li, A. and Barber, R.F. (2019), Multiple testing with the structure‐adaptive Benjamini–Hochberg algorithm. J. R. Stat. Soc. B, 81: 45-74. https://doi.org/10.1111/rssb.12298

