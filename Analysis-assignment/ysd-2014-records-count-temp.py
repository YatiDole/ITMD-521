from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

#Reading 2014 year data
df = spark.read.format("csv").load("hdfs://namenode/output/itmd-521/ysd/2014/csv-file")

#Filtering bad data Air_Temperature ==999.9
df_bad_temp=df.filter(df.Air_Temperature== 999.9)

#Using .count() to find bad record count
bad_count = df_bad_temp.count() #84823263

#Total count
total_count = df.count()



# Spark-The Definitive Guide(E-book) 73 Creating DataFrames
myManualSchema = StructType([
StructField("bad_record_count", IntegerType(), True),
StructField("Total_record_count", IntegerType(), True),
StructField("Percentage", FloatType(), True)
])

myRow = Row(bad_count, total_count,float((bad_count*100.0)/total_count))


df_data = spark.createDataFrame([myRow], myManualSchema)

df_data..write.format("csv").option("header","true").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/records-count-temp")