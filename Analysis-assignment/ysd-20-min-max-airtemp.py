from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

#Taking 2000-2018.txt as we want to find for the decade 2000-2010
df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/2000-2018.txt")


df3 = df2.withColumn('Weather_Station', df2['value'].substr(5, 6)).withColumn('WBAN', df2['value'].substr(11, 5)).withColumn('Observation_Date',to_date(df2['value'].substr(16,8),"yyyyMMdd")).withColumn('Observation_Hour', df2['value'].substr(24, 4).cast(IntegerType())).withColumn('Latitude', df2['value'].substr(29, 6).cast('float') / 1000).withColumn('Longitude', df2['value'].substr(35, 7).cast('float') / 1000).withColumn('Elevation', df2['value'].substr(47, 5).cast(IntegerType())).withColumn('Wind_Direction', df2['value'].substr(61, 3).cast(IntegerType())).withColumn('WD_Quality_Code', df2['value'].substr(64, 1).cast(IntegerType())).withColumn('Sky_Ceiling_Height', df2['value'].substr(71, 5).cast(IntegerType())).withColumn('SC_Quality_Code', df2['value'].substr(76, 1).cast(IntegerType())).withColumn('Visibility_Distance', df2['value'].substr(79, 6).cast(IntegerType())).withColumn('VD_Quality_Code', df2['value'].substr(86, 1).cast(IntegerType())).withColumn('Air_Temperature', df2['value'].substr(88, 5).cast('float')/10).withColumn('AT_Quality_Code', df2['value'].substr(93, 1).cast(IntegerType())).withColumn('Dew_Point', df2['value'].substr(94, 5).cast('float')).withColumn('DP_Quality_Code', df2['value'].substr(99, 1).cast(IntegerType())).withColumn('Atmospheric_Pressure', df2['value'].substr(100, 5).cast('float')/ 10).withColumn('AP_Quality_Code', df2['value'].substr(105, 1).cast(IntegerType())).drop('value')

#Selecting only Observation_Date,Air_Temperature,and spliting that date to year,month and date columns
df_split=df3.select(col("Air_Temperature"),col("Observation_Date"),year(col("Observation_Date")).alias("year"),month(col("Observation_Date")).alias("month"),dayofmonth(col("Observation_Date")).alias("day")).drop("Observation_Date")

#Filtering out the dataframe to pick only the decade 2000-2010
df_split=df_split.filter(df_split.year<=2010)
df_split=df_split.filter(df_split.year>=2000)

#Filtering out the bad data Air_Temperature=999.9
df_split=df_split.filter(df_split.Air_Temperature!=999.9)

#Range of temperatures on earth are: (46 C to -73 C)
df_split=df_split.filter(df_split.Air_Temperature<=4.6)
df_split=df_split.filter(df_split.Air_Temperature>= -7.3) 

#using orderby to sort dataframe according to month and day
dfsorted = df_split.orderBy("month","day")

maxtemp=dfsorted.orderBy("month").groupBy(dfsorted.month).agg({"Air_Temperature":"max"})
maxtemp.orderBy('month').show()

print(maxtemp.orderBy('month').show())

mintemp=dfsorted.orderBy("month").groupBy(dfsorted.month).agg({"Air_Temperature":"min"})

print(mintemp.orderBy('month').show())
#Using outer join to combine the the two above Dataframe to create a new dataframe with only month ,min and max columns
df_min_max=mintemp.join(maxtemp,mintemp.month==maxtemp.month,'outer')

print(df_min_max.show())


df_min_max.write.format("csv").option("header","true").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2000-2010/min-max-airtemp")