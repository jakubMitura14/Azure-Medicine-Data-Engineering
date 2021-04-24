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

For obvius reasons the columns and bounds needed to be chosen manually, all of the results would be saved into the delta table













