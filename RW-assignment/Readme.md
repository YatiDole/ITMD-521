# ITMD-521 Read & Write Assignment


## Cluster Command

Provide the command and all of the commandline options used to generate your csv and parquet files.

I should be able to execute this command and achieve your results.

```bash
 spark-submit --verbose --name ysd-2014-read-write-csv.py --master yarn --deploy-mode cluster ysd-read-write-cv.py
```

### Your Explanation

CITING from the text of the book, explain why you chose the values on the commandline that you did and explain what they do and what occurs.


(http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html#DataFrameReader.text)
The decade file we import is a .txt file so we use read.text
```bash
 df2 = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/2000-2018.txt")
```
Adding .withcolumn to convert the data to a tablular format 
```bash
 df3 = df2.withColumn('Weather_Station', df2['value'].substr(5, 6)).withColumn('WBAN', df2['value'].substr(11, 5)).withColumn('Observation_Date',to_date(df2['value'].substr(16,8),"yyyyMMdd")).withColumn('Observation_Hour', df2['value'].substr(24, 4).cast(IntegerType())).withColumn('Latitude', df2['value'].substr(29, 6).cast('float') / 1000).withColumn('Longitude', df2['value'].substr(35, 7).cast('float') / 1000).withColumn('Elevation', df2['value'].substr(47, 5).cast(IntegerType())).withColumn('Wind_Direction', df2['value'].substr(61, 3).cast(IntegerType())).withColumn('WD_Quality_Code', df2['value'].substr(64, 1).cast(IntegerType())).withColumn('Sky_Ceiling_Height', df2['value'].substr(71, 5).cast(IntegerType())).withColumn('SC_Quality_Code', df2['value'].substr(76, 1).cast(IntegerType())).withColumn('Visibility_Distance', df2['value'].substr(79, 6).cast(IntegerType())).withColumn('VD_Quality_Code', df2['value'].substr(86, 1).cast(IntegerType())).withColumn('Air_Temperature', df2['value'].substr(88, 5).cast('float')/10).withColumn('AT_Quality_Code', df2['value'].substr(93, 1).cast(IntegerType())).withColumn('Dew_Point', df2['value'].substr(94, 5).cast('float')).withColumn('DP_Quality_Code', df2['value'].substr(99, 1).cast(IntegerType())).withColumn('Atmospheric_Pressure', df2['value'].substr(100, 5).cast('float')/ 10).withColumn('AP_Quality_Code', df2['value'].substr(105, 1).cast(IntegerType()))
```
Filter to get only rows which have year 2014 (http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/context.html#SQLContext.tables)

```bash
df3.createOrReplaceTempView("MainTable")

df_2014 = spark.sql("select * from MainTable where year(Observation_Date)= 2014")
```https://user-images.githubusercontent.com/57376468/81509903-92fc8980-92d3-11ea-8072-d211bb413555.JPG

(http://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/readwriter.html#DataFrameWriter.format)

Statement to write out the filtered data frame to output datasource . making use of overwrite mode so that every time the script is run the same file in the target output is overwriiten everytime
```bash
df_2014.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014.parquet")
```

Writing Dataframe to a csv file
```bash
df_2014.write.format("csv").mode("overwrite").option("".save("hdfs://namenode/output/itmd-521/ysd/2014.csv")
```

Output: 
![alt text](https://user-images.githubusercontent.com/57376468/81511481-63ec1500-92df-11ea-8d88-62ce6128e957.JPG "Output 20 values")

