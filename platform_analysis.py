from load_data import DataLoader
from pyspark.sql import functions as F

def main():
    loader = DataLoader()

    # We only need Job Events for this
    # We load it, but we need to create two aliases of it to join them
    df_jobs = loader.load_job_events()

    print("\n" + "="*60)
    print("ORIGINAL Q2: Job Schedule Latency Analysis")
    print("How long do jobs wait in the queue?")
    print("="*60)

    # 1. Get Submit Times (Event 0)
    # We take the FIRST submit time per job (min timestamp)
    df_submit = df_jobs.filter("event_type = 0") \
                       .groupBy("job_id", "scheduling_class") \
                       .agg(F.min("timestamp").alias("submit_time"))

    # 2. Get Schedule Times (Event 1)
    # We take the FIRST schedule time per job (min timestamp)
    # This is when the job actually starts running on a machine
    df_schedule = df_jobs.filter("event_type = 1") \
                         .groupBy("job_id") \
                         .agg(F.min("timestamp").alias("schedule_time"))

    # 3. Join them on Job ID
    # Inner join: We only care about jobs that eventually started
    df_latency = df_submit.join(df_schedule, on="job_id", how="inner")

    # 4. Calculate Latency (Wait Time)
    # Timestamp is in microseconds. Divide by 1,000,000 to get seconds.
    df_calc = df_latency.withColumn(
        "wait_time_seconds", 
        (F.col("schedule_time") - F.col("submit_time")) / 1000000
    )

    # 5. Aggregate by Scheduling Class
    result = df_calc.groupBy("scheduling_class") \
                    .agg(
                        F.count("*").alias("job_count"),
                        F.avg("wait_time_seconds").alias("avg_wait_seconds"),
                        F.max("wait_time_seconds").alias("max_wait_seconds")
                    ) \
                    .orderBy("scheduling_class")

    result.show()

    loader.stop()

if __name__ == "__main__":
    main()