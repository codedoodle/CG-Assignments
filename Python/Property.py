# Databricks notebook source
# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4304312318995627/4174293604489617/1999489753413851/latest.html
#FileStore/tables/Property_data-2.csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, mean, max, min
spark = SparkSession.builder.appName("PropertyDataExample").getOrCreate()
pdf = spark.read.csv("/FileStore/tables/Property_data.csv", header=True, inferSchema=True)
pdf.show()

# COMMAND ----------

# DBTITLE 1,Average price for each bedroom
avg = pdf.groupBy("Bedrooms").avg("price").alias("Average_Price")
 
avg.show()


# COMMAND ----------

# DBTITLE 1,Total flats in a particular location
total_flats = pdf.groupBy("location").count()
total_flats.show()

# COMMAND ----------

# DBTITLE 1,Data of flats with (price<90k)
df1 = pdf.filter( pdf["price"] < 90000 ).select("*")
 
df1.count()

# COMMAND ----------

# DBTITLE 1,Data of flats with (price>150k)
df3 = pdf.filter( pdf["price"] > 150000 ).select("*")
df3.count()
df3.groupBy("Bedrooms").avg("Price").show()

# COMMAND ----------

# DBTITLE 1,Rename Column
pdf = pdf.withColumnRenamed("Price SQ Ft", "Price_Sq_Ft")
 
pdf.show()

# COMMAND ----------

# DBTITLE 1,Maximum and Minimum and Average price of whole data
pdf.select( max(pdf["price"]) ).show()
pdf.select( min(pdf["price"]) ).show()
pdf.select( mean(pdf["price"]) ).show()



# COMMAND ----------

# DBTITLE 1,Price per bedroom
ppbed = pdf.withColumn( "Price_Per_Bedroom", format_number(((pdf["Price_Sq_Ft"]*pdf["Size"])/(pdf["Bedrooms"])), 2) )
 
ppbed.show()

# COMMAND ----------

# DBTITLE 1,Price per bathroom
ppbath = ppbed.withColumn( "Price_Per_Bathroom", format_number( ((ppbed["Price_Sq_Ft"]*ppbed["Size"])/(ppbed["Bathrooms"])),2 ) )
 
ppbath.show()
