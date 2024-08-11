import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import types, functions as F

spark = SparkSession.builder \
    .master("spark://codespaces-0918c7:7077") \
    .appName('test') \
    .getOrCreate()

data_green = 'taxi_data/green/pq/*/'
data_yellow = 'taxi_data/yellow/pq/*/'

yellow_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

green_schema = types.StructType([
    types.StructField('VendorID', types.IntegerType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('RatecodeID', types.IntegerType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('passenger_count', types.IntegerType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('ehail_fee', types.DoubleType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('payment_type', types.IntegerType(), True),
    types.StructField('trip_type', types.IntegerType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)
])

df_yellow = spark.read \
    .schema(yellow_schema) \
    .parquet(data_yellow)

df_green = spark.read \
    .schema(green_schema) \
    .parquet(data_green)

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


common_columns = [
      'VendorID'
    , 'pickup_datetime'
    , 'dropoff_datetime'
    , 'store_and_fwd_flag'
    , 'RatecodeID'
    , 'PULocationID'
    , 'DOLocationID'
    , 'passenger_count'
    , 'trip_distance'
    , 'fare_amount'
    , 'extra'
    , 'mta_tax'
    , 'tip_amount'
    , 'tolls_amount'
    , 'improvement_surcharge'
    , 'total_amount'
    , 'payment_type'
    , 'congestion_surcharge']

df_green_select = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_select = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_union = df_green_select.unionAll(df_yellow_select)

df_union.groupby('service_type').count().show()

df_union.createOrReplaceTempView('trips_data')

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    revenue_zone,revenue_month, service_type
""")

df_result.coalesce(1).write.parquet('taxi_data/report/revenue/', mode='overwrite')
