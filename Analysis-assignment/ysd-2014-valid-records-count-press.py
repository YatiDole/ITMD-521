from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()


df = spark.read.format("csv").load("hdfs://namenode/output/itmd-521/ysd/2014/csv-file")

total_count= df.count()# 130606333 was the count in pyspark console

df_bad = df.filter(df.Atmospheric_Pressure == 9999.9)

bad_count = df_bad.count() #84823263

# Spark-The Definitive Guide(E-book) 73 Creating DataFrames
myManualSchema = StructType([
StructField("badrecordcount", IntegerType(), True),
StructField("Totalrecordcount", IntegerType(), True),
StructField("Percentage", FloatType(), True)
])

#Adding using Row
myRow = Row(bad_count, total_count,float((bad_count*100.0)/total_count))

#Dataframe is created with information(schema) added
df_data = spark.createDataFrame([myRow], myManualSchema)

df_data.write.format("csv").option("header","true").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/valid-records-count-press")

