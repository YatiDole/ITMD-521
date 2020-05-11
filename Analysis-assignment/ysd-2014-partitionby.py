from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import StringType
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, FloatType
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/ysd/2014/valid-records-temp")

df.createOrReplaceTempView("MainTable")

#Using sparks sql feature ,introducing new column months along with other columns
df_month_data =spark.sql("select month(Observation_Date) as month ,* from MainTable")


#Using partition by month column 
df_month_data.write.format("parquet").mode("overwrite").partitionBy("month").save("hdfs://namenode/output/itmd-521/ysd/2014/Partition-By-Month-Parquet")


#Writing out in parquet format and naming the folder Partition-By-Month-Parquet
df = spark.read.format("parquet").load("hdfs://namenode/output/itmd-521/ysd/2014/Partition-By-Month-Parquet")



