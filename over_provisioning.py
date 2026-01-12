import time
from load_data import DataLoader
from pyspark.sql import functions as F
from operator import add

def main():
    loader = DataLoader()

    print("Loading data...")
    # We load the data first so file reading overhead doesn't skew the test too much
    df_task_events = loader.load_task_events()
    df_task_usage = loader.load_task_usage()

    print("\n" + "="*60)
    print("ORIGINAL Q1: Over-provisioning Analysis (Performance Test)")
    print("="*60)

    # ---------------------------------------------------------
    # METHOD 1: DATAFRAME APPROACH
    # ---------------------------------------------------------
    print("\nStarting Method 1 (DataFrame)...")
    start_time_df = time.time()
    
    analyze_with_dataframe(df_task_events, df_task_usage)
    
    end_time_df = time.time()
    duration_df = end_time_df - start_time_df
    print(f">>> DataFrame Time: {duration_df:.4f} seconds")

    # ---------------------------------------------------------
    # METHOD 2: RDD APPROACH
    # ---------------------------------------------------------
    print("\n" + "-"*30)
    print("Starting Method 2 (RDD)...")
    start_time_rdd = time.time()
    
    analyze_with_rdd(df_task_events, df_task_usage)
    
    end_time_rdd = time.time()
    duration_rdd = end_time_rdd - start_time_rdd
    print(f">>> RDD Time:       {duration_rdd:.4f} seconds")

    # ---------------------------------------------------------
    # SUMMARY
    # ---------------------------------------------------------
    print("\n" + "="*60)
    print("PERFORMANCE COMPARISON")
    print("="*60)
    print(f"DataFrame Approach: {duration_df:.4f} s")
    print(f"RDD Approach:       {duration_rdd:.4f} s")
    
    if duration_rdd > 0:
        speedup = duration_rdd / duration_df
        print(f"DataFrames were {speedup:.2f}x faster than RDDs.")
    
    loader.stop()

def analyze_with_dataframe(df_events, df_usage):
    # 1. Prepare Task Events (Request side)
    df_req = df_events.select("job_id", "task_index", "scheduling_class", "cpu_request") \
                      .filter("cpu_request IS NOT NULL AND cpu_request > 0") \
                      .dropDuplicates(["job_id", "task_index"])

    # 2. Prepare Task Usage (Usage side)
    df_use = df_usage.select("job_id", "task_index", "cpu_rate") \
                     .groupBy("job_id", "task_index") \
                     .agg(F.avg("cpu_rate").alias("avg_cpu_usage"))

    # 3. Join
    df_joined = df_req.join(df_use, on=["job_id", "task_index"], how="inner")

    # 4. Calculate Utilization Ratio
    df_analysis = df_joined.withColumn(
        "utilization_ratio", 
        F.col("avg_cpu_usage") / F.col("cpu_request")
    )

    # 5. Aggregate
    result = df_analysis.groupBy("scheduling_class") \
                        .agg(
                            F.avg("cpu_request").alias("avg_requested"),
                            F.avg("avg_cpu_usage").alias("avg_used"),
                            F.avg("utilization_ratio").alias("avg_utilization")
                        ) \
                        .orderBy("scheduling_class")

    # Action to trigger computation
    result.show()

def analyze_with_rdd(df_events, df_usage):
    # 1. Convert DataFrames to RDDs
    # We map to ((job_id, task_index), (cpu_request, scheduling_class))
    rdd_events_raw = df_events.select("job_id", "task_index", "cpu_request", "scheduling_class") \
        .rdd \
        .filter(lambda row: row.cpu_request is not None and row.cpu_request > 0) \
        .map(lambda row: ((row.job_id, row.task_index), (float(row.cpu_request), row.scheduling_class)))

    # --- THE FIX IS HERE ---
    # We must deduplicate strictly by key to match the DataFrame's dropDuplicates().
    # Without this, tasks with many events (Submit, Schedule, Evict, Schedule) are counted multiple times.
    rdd_events = rdd_events_raw.reduceByKey(lambda a, b: a) 
    # -----------------------

    # Aggregating usage in RDD (Same as before)
    rdd_usage_raw = df_usage.select("job_id", "task_index", "cpu_rate").rdd \
        .map(lambda row: ((row.job_id, row.task_index), (float(row.cpu_rate), 1)))
    
    rdd_usage_agg = rdd_usage_raw.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                                 .mapValues(lambda v: v[0] / v[1])

    # 2. Join (Now both sides are unique per task)
    rdd_joined = rdd_events.join(rdd_usage_agg)

    # 3. Map Logic
    def map_metrics(pair):
        specs = pair[1]
        req_data = specs[0]
        usage_val = specs[1]
        
        cpu_req = req_data[0]
        sched_class = req_data[1]
        
        ratio = usage_val / cpu_req
        return (sched_class, (cpu_req, usage_val, ratio, 1))

    rdd_mapped = rdd_joined.map(map_metrics)

    # 4. Reduce Logic
    def reduce_sums(a, b):
        return (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3])

    rdd_result = rdd_mapped.reduceByKey(reduce_sums)

    # 5. Final Calculation
    final_result = rdd_result.mapValues(lambda v: (
        v[0]/v[3], v[1]/v[3], v[2]/v[3]
    )).sortByKey().collect()

    # Print Result
    print(f"{'Class':<10} {'Avg Request':<15} {'Avg Used':<15} {'Utilization'}")
    print("-" * 55)
    for row in final_result:
        print(f"{row[0]:<10} {row[1][0]:<15.4f} {row[1][1]:<15.4f} {row[1][2]:.4f}")

if __name__ == "__main__":
    main()