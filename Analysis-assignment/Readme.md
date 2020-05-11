# ITMD-521 Cluster Analysis


## Phase 1

Provide the command and all of the commandline options used to generate your csv and parquet files.

I should be able to execute this command and achieve your results.

```bash
spark-submit --verbose jrh-demo-read.py --name --master yarn --deploy-mode cluster demo-read.py
```




## Question 1 - Air Temperature and Atmospheric Pressure

# Spark-The Definitive Guide(E-book) 73 Creating DataFrames


ysd-2014-valid-records-temp.py reads the Single year parquet file and uses below command to filter out any row with AirTemperature value = 999.9

```bash
df_valid_temp=df.filter(df.Air_Temperature!= 999.9)
```
Then we write df_valid(AirTemperature) to our hadoop file system in a new folder valid-records-temp


```bash
df_valid.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/valid-records-temp")
```
To write additional dataframe to a file with bad record count total record count and percentage we make use of below filter to filter out bad records

```bash
df_bad_temp=df.filter(df.Air_Temperature== 999.9)
```
Find total count by making use of .count()

```bash
total_count = df.count()
```
```bash
bad_count = df_bad_temp.count() 
```
lastly making use of create spark dataframe 

```bash
myManualSchema = StructType([
StructField("bad record count", IntegerType(), True),
StructField("Total record count", IntegerType(), True),
StructField("Percentage", FloatType(), True)
])
```
passing count values and percentage to a row 
```bash
yRow = Row(bad_count, total_count,float((bad_count*100.0)/total_count))
```
```bash
df_data = spark.createDataFrame([myRow], myManualSchema)

```
Then simply writing this out to file called records-count-temp using following command

```bash
df_data.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/valid-records-count-press")
```

#Part B
For Atmospheric pressure we make use od ysd-2014-valid-record-press.py we submit following command on bash

```bash
spark-submit --verbose ysd-2014-valid-record-press.py --name --master yarn --deploy-mode cluster ysd-2014-valid-record-press.py
```
By running the following command we filter out the dataframe of values which are 9999.9
```bash
df_valid = df.filter(df.Atmospheric_Pressure!= 9999.9)
```

we write this file to folder called valid-atmospheric-pressure in hdfs using following command

```bash
df_valid.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/ysd/2014/valid-atmospheric-pressure")
```
Valid Atmospheric Pressure:
![alt text](https://user-images.githubusercontent.com/57376468/81509936-c808dc00-92d3-11ea-802c-5011bf9cf18d.JPG "valid-atmospheric-pressure")



```bash
df_valid = df.filter(df.Atmospheric_Pressure!= 9999.9)
```
Valid Atmospheric pressure count :
![alt text](https://user-images.githubusercontent.com/57376468/81509934-c808dc00-92d3-11ea-8952-7b6a5ddbd295.JPG "valid-record-count-press")
```bash
df_valid = df.filter(df.Atmospheric_Pressure!= 9999.9)
```
Valid Air Temperature count :
![alt text](https://user-images.githubusercontent.com/57376468/81509935-c808dc00-92d3-11ea-9d1c-916283c24d0a.JPG "valid-record-count-temp")


## Question 2 - Explain Partition Effect

* Briefly explain in a paragraph with references, what happens to execution time when you reduce the shuffle partitions from the default of 200 to 20?
--> when you reduce the shuffle partitions from the default of 200 to 20, the query execution time gets reduced. This occurs because the number of shuffle partitions in spark is static. It doesn’t change with different data size. This will lead into below issues-
For less amount of data, 200 is a overkill which often leads to slower processing because of scheduling overheads.
For large amount data, 200 is small and doesn’t effectively use the all resources in the cluster. By reducing it to 20 we increase data processing time manually between the partitions for given amount of data.


=======
## Question 3 - Min Max Air Temperature


Selecting only observation_Date and Airtemperature and splitting date to year and month
```bash
df_split=df.select(col("Air_Temperature"),col("Observation_Date"),year(col("Observation_Date")).alias("year"),month(col("Observation_Date")).alias("month"),dayofmonth(col("Observation_Date")).alias("day")).drop("Observation_Date")
```
Filter range of temperatures on earth 
```bash
df_valid_temp = df_split.filter((df_split.Air_Temperature) <=4.6 )
df_valid_temp = df_valid_temp.filter((df_valid_temp.Air_Temperature) >= -7.3)
```
2014 Min max Air Temperature :
![alt text](https://user-images.githubusercontent.com/57376468/81509903-92fc8980-92d3-11ea-8072-d211bb413555.JPG "Min Max Air Temp")




## Question 4

* Show a screenshot of the execution times for your year
  * 1
  * 50  
  * 200

  ![alt text](https://user-images.githubusercontent.com/57376468/81511480-63ec1500-92df-11ea-99c0-43c500afa1db.JPG "Partition analysis")
  

* Show a screenshot of the execution times for your decade
  * 1
  * 50 
  * 200
    ![alt text](https://user-images.githubusercontent.com/57376468/81511854-ce05b980-92e1-11ea-84f5-1b7bfe0fed8c.JPG "Partition200")

* Compare the execution times and explain why or why not there are any significant differences in the first group and in the second group
-->The execution times shows there are significant differences in the first group and in the second group
-For partition 1 in the first group and in the second group, the time is less for specified year as data is less among the partitions. For the 5 year amount of data is more but with same number of partions and hence execution time is more.
-For partition 50, the time is less for specified year as data is less among the partitions. Hence per year data has less execution time. For the 5 year amount of data is more but as number of partions are increased and hence execution time is less for 5 year data compared to partition one data for same 5 year.
-For partition 200, the time is less for specified year as data is less among the partitions. Hence per year data has less execution time. For the 5 year amount of data is more but as number of partions are increased,there are more partitions for data to process resulting in data skew and hence execution time is more.


