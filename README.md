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

![image](https://user-images.githubusercontent.com/53857487/115957822-eb3c9380-a504-11eb-944b-e73f4fea745e.png)



# Information Flow
Information is stored in blob container in a storage Account, as data is in clinical setting written into excel files this is a data that is uploaded 
![image](https://user-images.githubusercontent.com/53857487/115954365-ea016b80-a4f0-11eb-902c-83b09aeeb703.png)

