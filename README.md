# Work in progress

# Azure-Medicine-Data-Engineering
## basic description 
Data science project based on data about patients with infected vascular grafts, and Azure tools (including DataBricks))
## Detailed plan
* Uploading Data to Data lake
  * The data Link of Azure Data Factory will be used
* Data cleaning and preparation**  (this step will be done in the Databricks notebook that will upload data from data lake)
  * upload the data into Databricks
  * Remove all columns and rows that do not contain any data (only nulls)
  * Check weather automatic casting of data type of columns was correct particularly looking into dates, and numeric data (risk that sometimes the floating point numbers can be represented with dot or comma) 
  * futher in R we will define which of the columns are representing the categorical variables and we will define the accordingly 
* Basic statistical analysis
  * collect and summarize basic patient data like gender age ...
  * anylyze how the radiologic signs are related to each other and to other analyzed variables 

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










