#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, countDistinct, avg, hour, sum as spark_sum, when, count

spark = SparkSession.builder \
    .appName("TaxiDataCleaning") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

schema = StructType([
    StructField("medallion", StringType(), True),
    StructField("hack_license", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("trip_time_in_secs", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

df = spark.read.csv('gs://assignment3ts/taxi-data-sorted-large.csv.bz2', 
                    schema=schema, 
                    header=False)

df_cleaned = df.dropna(subset=["fare_amount", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"])
df_cleaned = df_cleaned.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

df_cleaned = df_cleaned.withColumn("trip_time_in_secs", col("trip_time_in_secs").cast(IntegerType())) \
                       .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
                       .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
                       .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
                       .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType())) \
                       .withColumn("total_amount", col("total_amount").cast(DoubleType()))

df_cleaned = df_cleaned.dropDuplicates()

df_grouped_p1 = df_cleaned.groupBy("medallion").agg(countDistinct("hack_license").alias("num_drivers"))

top_ten_taxis = df_grouped_p1.orderBy(col("num_drivers").desc()).limit(10)

# Write Task 1 Output to GCS
top_ten_taxis.coalesce(1).write.csv('gs://assignment3ts/output/task1_top_taxis.csv', header=True)

# Task 2: Calculate Average Money Per Minute for each driver
df_cleaned_p2 = df.dropna(subset=["hack_license", "trip_time_in_secs", "total_amount"])
df_cleaned_p2 = df_cleaned_p2.filter(col("trip_time_in_secs") > 0)

df_with_money_per_minute = df_cleaned_p2.withColumn("money_per_minute", col("total_amount") / (col("trip_time_in_secs") / 60))

df_avg_money_per_minute = df_with_money_per_minute.groupBy("hack_license").agg(avg("money_per_minute").alias("avg_money_per_minute"))

top_ten_drivers = df_avg_money_per_minute.orderBy(col("avg_money_per_minute").desc()).limit(10)

top_ten_drivers.coalesce(1).write.csv('gs://assignment3ts/output/task2_top_drivers.csv', header=True)

# Task 3: Calculate profit ratio by hour
df_cleaned_p3 = df.dropna(subset=["pickup_datetime", "trip_distance", "surcharge"])
df_cleaned_p3 = df_cleaned_p3.filter(col("trip_distance") > 0)

df_with_hour = df_cleaned_p3.withColumn("hour_of_day", hour(col("pickup_datetime")))

df_grouped_by_hour = df_with_hour.groupBy("hour_of_day").agg(
    spark_sum("surcharge").alias("total_surcharge"),
    spark_sum("trip_distance").alias("total_trip_distance")
)

df_with_profit_ratio = df_grouped_by_hour.withColumn(
    "profit_ratio", col("total_surcharge") / col("total_trip_distance")
)

best_hour = df_with_profit_ratio.orderBy(col("profit_ratio").desc()).limit(1)

best_hour.coalesce(1).write.csv('gs://assignment3ts/output/task3_best_hour.csv', header=True)

# Task 4: Calculate percentage of card payments by hour
df_with_hour = df.withColumn("hour_of_day", hour(col("pickup_datetime")))

df_with_payment_method = df_with_hour.withColumn(
    "is_cash", when(col("payment_type") == "CSH", 1).otherwise(0)
).withColumn(
    "is_card", when(col("payment_type") == "CRD", 1).otherwise(0)
)

total_cash_count = df_with_payment_method.filter(col("is_cash") == 1).count()
total_card_count = df_with_payment_method.filter(col("is_card") == 1).count()
total_count = df_with_payment_method.count()

total_percent_cash = (total_cash_count / total_count) * 100
total_percent_card = (total_card_count / total_count) * 100

df_grouped_by_hour = df_with_payment_method.groupBy("hour_of_day").agg(
    count("is_card").alias("total_rides"),
    spark_sum("is_card").alias("card_payments")
)

df_grouped_with_percent = df_grouped_by_hour.withColumn(
    "percent_card", (col("card_payments") / col("total_rides")) * 100
)

df_grouped_with_percent.coalesce(1).write.csv('gs://assignment3ts/output/task4_hourly_card_payment.csv', header=True)

spark.stop()

