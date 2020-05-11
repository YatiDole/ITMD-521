from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/ysd/2014/parquet-file")


df_valid = df.filter(df.Atmospheric_Pressure!= 9999.9)

print(df_valid.show(10))

df_valid.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/valid-atmospheric-pressure")

df_valid.count() #45783070



