from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode/output/itmd-521/ysd/2014/csv-file")

# page 82 Spark definetive guide -compression lz4
df.repartition(1).write.format("csv").option("compression","lz4").mode("overwrite").option("header","true").save("hdfs://namenode/output/itmd-521/ysd/2014/repartition/1")