
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
import time

def main():
    # 1. Initialize Spark (No special config needed for Dataproc)
    spark = SparkSession.builder \
        .appName("GCP_Full_Latency_Analysis") \
        .getOrCreate()
    start_time = time.time()
    print(f"--- BENCHMARK START: {time.ctime(start_time)} ---")
    print("--- STARTING ANALYSIS ON FULL GOOGLE DATASET ---")

    # 2. Define Schema for Job Events
    # (We must be precise because we are reading 500+ files)
    schema_job = StructType([
        StructField("timestamp", LongType(), True),
        StructField("missing_info", IntegerType(), True),
        StructField("job_id", LongType(), True),
        StructField("event_type", IntegerType(), True),
        StructField("user_name", StringType(), True),
        StructField("scheduling_class", IntegerType(), True),
        StructField("job_name", StringType(), True),
        StructField("logical_job_name", StringType(), True)
    ])

    # 3. Read Data DIRECTLY from Google's Public Bucket
    # Note the wildcard *: This reads ALL parts (00000 to 00500)
    gcs_path = "gs://clusterdata-2011-2/job_events/part-*.csv.gz"
    
    print(f"Reading from: {gcs_path}")
    df_jobs = spark.read.csv(gcs_path, schema=schema_job)

    # 4. Perform the Latency Analysis (Same logic as before)
    
    # Get Submit Time (Event 0)
    df_submit = df_jobs.filter("event_type = 0") \
                       .groupBy("job_id", "scheduling_class") \
                       .agg(F.min("timestamp").alias("submit_time"))

    # Get Schedule Time (Event 1)
    df_schedule = df_jobs.filter("event_type = 1") \
                         .groupBy("job_id") \
                         .agg(F.min("timestamp").alias("schedule_time"))

    # Join
    df_latency = df_submit.join(df_schedule, on="job_id", how="inner")

    # Calculate Wait Time in Seconds
    df_calc = df_latency.withColumn(
        "wait_time_seconds", 
        (F.col("schedule_time") - F.col("submit_time")) / 1000000
    )

    # Aggregate results
    result = df_calc.groupBy("scheduling_class") \
                    .agg(
                        F.count("*").alias("job_count"),
                        F.avg("wait_time_seconds").alias("avg_wait_seconds"),
                        F.max("wait_time_seconds").alias("max_wait_seconds")
                    ) \
                    .orderBy("scheduling_class")

    # 5. Show Results
    print("--- FINAL RESULTS ---")
    result.show()
    # STOP TIMER
    end_time = time.time()
    duration = end_time - start_time
    
    print("="*50)
    print(f"BENCHMARK RESULT: {duration:.2f} seconds")
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    main()
