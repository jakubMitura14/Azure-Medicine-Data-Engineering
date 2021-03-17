# Azure-Medicine-Data-Engineering
## basic description 
Data science project based on data about patients with infected vascular grafts, and Azure tools (including DataBricks))
## Detailed plan
*Uploading Data to Data lake
 *The data Link of Azure Data Factory will be used
*  **Data cleaning and preparation**  (this step will be done in the Databricks notebook that will upload data from data lake)
 *upload the data into Databricks
 *Remove all columns and rows that do not contain any data (only nulls)
 *Check weather automatic casting of data type of columns was correct particularly looking into dates, and numeric data (risk that sometimes the floating point numbers can be represented with dot or comma) 
 *futher in R we will define which of the columns are representing the categorical variables and we will define the accordingly 
* Basic statistical analysis
 * collect and summarize basic patient data like gender age ...
 * anylyze how the radiologic signs are related to each other and to other analyzed variables 


