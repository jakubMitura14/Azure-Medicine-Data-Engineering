// Databricks notebook source
// MAGIC %md
// MAGIC # Utility functions for 

// COMMAND ----------


import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType};
import org.apache.spark.sql.{Row, SparkSession}
import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.WrappedArray
import scala.math._
import Numeric.Implicits._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.ml.feature
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile



//*************************** general ***********************************************

//importing tsv amd in the same time it will import all usefull libraries from spark ...
def myImportFile(fileName : String) : DataFrame =  {
return spark.read.
    format("csv").
    option("header", "true").
    option("delimiter", "\t").
    option("header", "true").
    option("inferSchema", "true").load("dbfs:/mnt/myblob/" + fileName)
}




//importing tsv amd in the same time it will import all usefull libraries from spark ...
def myImportFileLocalDataBricks(fileName : String) : DataFrame =  {
return spark.read.
    format("csv").
    option("header", "true").
    option("delimiter", "\t").
    option("header", "true").
    option("inferSchema", "true").load(fileName)
}



//getting name from column
def roundKeepname(c:Column) =  c.expr.asInstanceOf[NamedExpression].name
  

/**
*@description given column names and frame it return this frame only with specified columns 
*@param frame - dataframe we are filtering
*@param colNames - list of column names we are intrested in
*@return dataframe with only those columns that we are intrested in
*/
def getFrameWithCols(frame : DataFrame, colNames : List[String]) : DataFrame = {
  
    return frame.select(colNames.map(it=> col(it)):_*)
}

/**
*@description after we save the table with descriptions of divisions - each row of such table contains name of the division in first column and the list of column names as array in second column - we specify in this function about what divisions we are intrested and the function will give back all distinct column names accumulated from all of those divisions
*@param frame - dataframe representing divisions table
*@param listOfDivNames - list of names of divisions
*@return list of distinct column names related to given divisions 
*/
def getColNamesOfDivisions(frame : DataFrame, listOfDivNames : List[String]) : List[String] = {
val whereClouse = listOfDivNames.map(it=>s"""divisionName = "$it" """).mkString(" OR ") // sql where clouse
return   frame.select(col("listOfColumnNames")).where(whereClouse).collect.toList
.map(wrapped=>wrapped.get(0).asInstanceOf[WrappedArray[String]].toList).flatten.distinct// we are unpacking the spark objects
}



//*************************** data cleaning  ***********************************************

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

//*************************** testing quality of data and ouliers ***********************************************


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

/****************************  for creating data summaries  ******************************/
/**
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

  

// COMMAND ----------

//First we need to define all necessary constance

val containerName = "mainblob"
val storageAccountName = "vascpathstorage"
val key = dbutils.secrets.get(scope= "myblobkv", key = "stor2")

val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.account.key." + storageAccountName + ".blob.core.windows.net"



// COMMAND ----------

val myMountPoint = "/mnt/myblob"

if(!dbutils.fs.mounts().map(it=>it.mountPoint).contains(myMountPoint)){
dbutils.fs.mount(
  mountPoint =myMountPoint,
  source = url,
  extraConfigs = Map(config -> key))

}
display(dbutils.fs.ls("/mnt/myblob"))

