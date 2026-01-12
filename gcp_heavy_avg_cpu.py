import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType

def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("Heavy_Benchmark_TaskUsage").getOrCreate()

    # START TIMER
    start_time = time.time()
    print(f"--- HEAVY BENCHMARK START: {time.ctime(start_time)} ---")

    # 1. Define Schema for Task Usage (We only need the columns we use to save RAM)
    # The full schema is huge, we just take what we need.
    schema_usage = StructType([
        StructField("start_time", LongType(), True),
        StructField("end_time", LongType(), True),
        StructField("job_id", LongType(), True),
        StructField("task_index", LongType(), True),
        StructField("machine_id", LongType(), True),
        StructField("cpu_rate", FloatType(), True),
        StructField("canonical_memory_usage", FloatType(), True)
    ])

    # 2. Read Data (HEAVY PART)
    # We load 100 files (part-00000 to part-00099).
    # This is approx 25GB of data.
    gcs_path = "gs://clusterdata-2011-2/task_usage/part-000*.csv.gz"
    
    print(f"Reading heavy dataset from: {gcs_path}")
    df_usage = spark.read.csv(gcs_path, schema=schema_usage)

    # 3. Heavy Aggregation
    # Group by Job ID and calculate averages. 
    # This forces a "Shuffle" which benefits greatly from more nodes.
    result = df_usage.groupBy("job_id") \
                     .agg(
                         F.count("*").alias("task_records"),
                         F.avg("cpu_rate").alias("avg_cpu"),
                         F.avg("canonical_memory_usage").alias("avg_mem")
                     )
    
    # 4. Trigger Action
    # We count the results to force the calculation to finish.
    total_jobs = result.count()
    print(f"Processed usage stats for {total_jobs} unique jobs.")

    # STOP TIMER
    end_time = time.time()
    duration = end_time - start_time
    
    print("="*50)
    print(f"BENCHMARK RESULT: {duration:.2f} seconds")
    print("="*50)
    
    spark.stop()

if __name__ == "__main__":
    main()