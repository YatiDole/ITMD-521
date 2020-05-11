from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth



spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

#Reading the filtered valid records-temp
df = spark.read.format("csv").option("header","true").load("hdfs://namenode/output/itmd-521/ysd/2014/valid-records-temp")


#Selecting only Observation_Date,Air_Temperature,and spliting that date to year,month and date columns
df_split=df3.select(col("Air_Temperature"),col("Observation_Date"),year(col("Observation_Date")).alias("year"),month(col("Observation_Date")).alias("month"),dayofmonth(col("Observation_Date")).alias("day")).drop("Observation_Date")

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


df_min_max.write.format("csv").option("header","true").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/min-max-airtemp")