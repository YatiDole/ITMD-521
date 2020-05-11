from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode/output/itmd-521/ysd/2014/csv-file")

df.repartition(200).write.format("csv").option("compression","lz4").mode("overwrite").option("header","true").save("hdfs://namenode/output/itmd-521/ysd/2014/repartition/200")