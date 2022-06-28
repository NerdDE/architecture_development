from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser(description='Dataproc Test')
parser.add_argument('--input_data_loc', required=True)
parser.add_argument('--output_data_loc', required=True)

args = parser.parse_args()

input_data = args.input_data_loc
output = args.output_data_loc

spark = SparkSession \
    .builder \
    .appName('data_nerds') \
    .getOrCreate()

df = spark.read.parquet(input_data)

df.show()

df.registerTempTable('trips')

df_processed = spark.sql( 
    '''
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', tpep_pickup_datetime) AS revenue_month, 
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
    trips
GROUP BY
    1, 2
    '''
)

df_processed.show()

df_processed.coalesce(1).write.parquet(output, mode='overwrite')

# --input_data_loc=gs://data_nerds_test123/2020/*
# --output_data_loc=gs://data_nerds_test123/test