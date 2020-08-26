# import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Data folder
data_folder = "/Users/maheswaraputha/mine/Career/tests/Data Engineer Test"

# create sparksession
spark = SparkSession.
    builder.
    appName("weather-data-process").
    getOrCreate()

df = spark.read.format('com.databricks.spark.csv').options(delimiter = ',', header ='true', nullValue ='null').load(data_folder + '/*.csv')
df = df.withColumn('ScreenTemperature',df['ScreenTemperature'].cast("float").alias('ScreenTemperature'))

max_temp_df = df.select('ObservationDate', 'ScreenTemperature', 'Region').
    where(col('ScreenTemperature') == df.groupby().max('ScreenTemperature').collect()[0]['max(ScreenTemperature)'])

max_temp_df.count()
max_temp_df.show(max_df.count(), False)

max_temp_row = max_temp_df.first()
#Row(ObservationDate='2016-03-17T00:00:00', ScreenTemperature=15.800000190734863, Region='Highland & Eilean Siar')

hottest_day = max_temp_row['ObservationDate']

hottest_day_temperature = max_temp_row['ScreenTemperature']

hottest_day_region = max_temp_row['Region']

print("hottest_day", hottest_day)
print("hottest_day_temperature", hottest_day_temperature)
print("hottest_day_region", hottest_day_region)
