# Databricks notebook source
import urllib
 
ACCESS_KEY = "AKIA4WR7LEUPDH5NNOXG"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
 
# bucket name
 
AWS_BUCKET_NAME = "firedatacap"
MOUNT_NAME = "s3Diabetes"
 
dbutils.fs.mount("s3n://%s:%s@%s" %  (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" %  MOUNT_NAME )

display(dbutils.fs.ls("/mnt/s3Diabetes"))

 

# COMMAND ----------


import urllib
 
ACCESS_KEY = "AKIA4WR7LEUPDH5NNOXG"
SECRET_KEY = "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"
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY, "")
 
# bucket name
 
AWS_BUCKET_NAME = "firedataregex"
MOUNT_NAME = "newDiabetes"
dbutils.fs.mount("s3n://%s:%s@%s" %  (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" %  MOUNT_NAME )


# COMMAND ----------

dfFire=spark.read.csv("dbfs:/mnt/newDiabetes/Fire_Department_Calls_for_Service.csv",header=True,inferSchema=True)
dfFire.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField,StringType,IntegerType,BooleanType,StructType
fireSchema = StructType([ StructField('CallNumber', IntegerType(), True),
                        StructField('UnitId', StringType(),True),
                        StructField('IncidentNumber', IntegerType(),True),
                        StructField('CallType', StringType(),True),
                         StructField('CallDate', StringType(),True),
                        StructField('WatchDate', StringType(),True),
                        StructField('ReceivedDtm', StringType(),True),
                        StructField('EntryDTm', StringType(),True),
                        StructField('DispatchDTM', StringType(),True),
                        StructField('ResponseDTm', StringType(),True),
                        StructField('OnSceneDTm', StringType(),True),
                         StructField('TransportDtm', StringType(),True),
                        StructField('HospitalDTm', StringType(),True),
                        StructField('CallFinalDisposition', StringType(),True),
                        StructField('AvailableDtm', StringType(),True),
                        StructField('Address', StringType(),True),
                        StructField('City', StringType(),True),
                        StructField('ZipCodeOfIncident', StringType(),True),
                        StructField('Battalion', StringType(),True),
                        StructField('StationArea', StringType(),True),
                        StructField('Box', StringType(),True),
                        StructField('OriginalPriority', StringType(),True),
                        StructField('Priority', StringType(),True),
                        StructField('FinalPriority', IntegerType(),True),
                        StructField('ALSUnit', BooleanType(),True),
                        StructField('CallTypeGroup', StringType(),True),
                        StructField('NumberofAlarms', IntegerType(),True),
                        StructField('UnitType', StringType(),True),
                        StructField('UnitSequenceInCallDispatch', StringType(),True),
                     
                                    StructField('FirePreventionDistrict', StringType(),True),
                                    StructField('SupervisorDistrict', StringType(),True),
                                    StructField('NeighborhooodsAnalysisBoundaries', StringType(),True),
                                    StructField('Location', StringType(),True),
                                    StructField('RowID', StringType(),True),
                                    StructField('shape', StringType(),True),
                                    StructField('SupervisorDistricts', IntegerType(),True),
                                    StructField('FirePreventionDistricts', IntegerType(),True),
                                    StructField('CurrentPoliceDistricts', IntegerType(),True),
                                    StructField('NeighborhoodsAnalysisBoundaries', IntegerType(),True),
                                                           StructField('ZipCodes', IntegerType(),True),
                                                                        StructField('NeighborhoodsOld', IntegerType(),True),
                                                                         StructField('PoliceDistricts', IntegerType(),True),
                                                                  StructField('CivicCenterHarmReductionProjectBoundary', IntegerType(),True),
                                                                   StructField('HSOCZones', IntegerType(),True),
                                                                    StructField('CentralMarket', IntegerType(),True),
                                                                     StructField('Neighborhoods', IntegerType(),True),
                                                                                    StructField('SFFindNeighborhoods', IntegerType(),True),
                                                                                    StructField('CurrentPoliceDistricts ', IntegerType(),True
                                                                                      )])

dfnew=spark.read.csv("dbfs:/mnt/newDiabetes/Fire_Department_Calls_for_Service.csv",header=True,schema=fireSchema)
dfnew.groupBy("CallType").count().show()

# COMMAND ----------

display(dfnew.select("CallType").groupBy("CallType").count().show())

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp
pattern1='MM/dd/yyyy'
to_pattern1='yyyy/MM/dd'
 
#dfnew.withColumn("newCol",unix_timestamp(dfnew(["CallDate"],pattern1).cast("timestamp")))

