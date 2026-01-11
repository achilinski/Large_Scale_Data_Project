# ============================================================
# PERFORMANCE TESTING CONFIGURATION
# ============================================================
USE_CACHING = True              # Test impact of caching DataFrames
USE_BROADCAST_JOIN = True       # Test broadcast vs. sort-merge join
EARLY_COLUMN_PROJECTION = True  # Test early select() optimization
OPTIMIZE_PARTITIONING = True    # Test custom partitioning
OPTIMIZED_FILTER_ORDER = True   # Test filter ordering
# ============================================================

import sys
from pyspark import SparkContext, SparkConf
import time
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType

#### Driver program
sc = SparkContext("local[8]")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)
print("OK")

### Schema definitions
task_events_schema = StructType([
    StructField("time", IntegerType(), True),
    StructField("missing_info", IntegerType(), True),
    StructField("job_id", IntegerType(), False),
    StructField("task_index", IntegerType(), False),
    StructField("machine_id", IntegerType(), True),
    StructField("event_type", IntegerType(), False),
    StructField("user", StringType(), True),
    StructField("scheduling_class", IntegerType(), True),
    StructField("priority", IntegerType(), False),
    StructField("cpu_request", FloatType(), True),
    StructField("memory_request", FloatType(), True),
    StructField("disk_space_request", FloatType(), True),
    StructField("different_machines_restriction", BooleanType(), True)
])

task_usage_schema = StructType([
    StructField("start_time", IntegerType(), False),
    StructField("end_time", IntegerType(), False),
    StructField("job_id", IntegerType(), False),
    StructField("task_index", IntegerType(), False),
    StructField("machine_id", IntegerType(), False),
    StructField("cpu_rate", FloatType(), True),
    StructField("canonical_memory_usage", FloatType(), True),
    StructField("assigned_memory_usage", FloatType(), True),
    StructField("unmapped_page_cache", FloatType(), True),
    StructField("total_page_cache", FloatType(), True),
    StructField("maximum_memory_usage", FloatType(), True),
    StructField("disk_io_time", FloatType(), True),
    StructField("local_disk_space_usage", FloatType(), True),
])

# Load data
df_events = sqlContext.read.csv("../google-dataset/task_events/part-00265-of-00500.csv.gz",
                                schema=task_events_schema,
                                header=False)
df_usage = sqlContext.read.csv("../google-dataset/task_usage/part-00265-of-00500.csv.gz",
                               schema=task_usage_schema,
                               header=False)

# ============================================================
# OPTIMIZATION 1: Early Column Projection
# ============================================================
if EARLY_COLUMN_PROJECTION:
    # Select only needed columns early to reduce data size
    df_events = df_events.select(
        "job_id", "task_index", "event_type",
        "cpu_request", "memory_request", "disk_space_request"
    )
    df_usage = df_usage.select(
        "job_id", "task_index", "cpu_rate",
        "canonical_memory_usage", "local_disk_space_usage"
    )
    # print("Applied early column projection")


# ============================================================
# OPTIMIZATION 2: Optimized Filter Order
# Apply most selective filters first
# ============================================================
if OPTIMIZED_FILTER_ORDER:
    # More selective filter first (assuming NULL checks are selective)
    task_events = df_events.filter(
        (F.col("cpu_request").isNotNull()) |
        (F.col("memory_request").isNotNull()) |
        (F.col("disk_space_request").isNotNull())
    )

    task_usage = df_usage.filter(
        (F.col("cpu_rate").isNotNull()) |
        (F.col("canonical_memory_usage").isNotNull()) |
        (F.col("local_disk_space_usage").isNotNull())
    )
else:
    # Original filter order
    task_events = df_events.filter(
        (df_events.cpu_request.isNotNull()) |
        (df_events.memory_request.isNotNull()) |
        (df_events.disk_space_request.isNotNull())
    )

    task_usage = df_usage.filter(
        (df_usage.cpu_rate.isNotNull()) |
        (df_usage.canonical_memory_usage.isNotNull()) |
        (df_usage.local_disk_space_usage.isNotNull())
    )


# ============================================================
# OPTIMIZATION 3: Partitioning
# ============================================================
if OPTIMIZE_PARTITIONING:
    # Repartition based on join keys for better shuffle performance
    task_events = task_events.repartition(8, "job_id", "task_index")
    task_usage = task_usage.repartition(8, "job_id", "task_index")
    print("Applied custom partitioning on join keys")

print("task_events")
task_events.show()
# print("task_usage")
# task_usage.show()

# ============================================================
# OPTIMIZATION 4: Caching
# ============================================================
if USE_CACHING:
    # TODO suitable caching earlier
    task_events.cache()
    task_usage.cache()
    # Force cache materialization
    task_events.count()
    task_usage.count()
    print("Applied caching to base DataFrames")

print("events per event-type")
task_events.groupBy("event_type").count().orderBy(F.desc("event_type")).show(20)

EVICTED_CODE = 2

# Filter evicted tasks
task_eviction_flag = (
    task_events
    .withColumn("is_eviction_event", (F.col("event_type") == EVICTED_CODE).cast("integer"))
    .groupBy("job_id", "task_index")
    .agg(
        F.max("is_eviction_event").alias("was_evicted")
    )
)

# print("evicted tasks")
# task_eviction_flag.show()

# Aggregate resource usage per task
task_usage_agg = (
    task_usage
    .groupBy("job_id", "task_index")
    .agg(
        F.avg("cpu_rate").alias("avg_cpu_rate"),
        F.max("cpu_rate").alias("max_cpu_rate"),
        F.avg("canonical_memory_usage").alias("avg_canon_mem"),
        F.max("canonical_memory_usage").alias("max_canon_mem"),
        F.avg("local_disk_space_usage").alias("avg_local_disk"),
        F.max("local_disk_space_usage").alias("max_local_disk"),
        F.count("*").alias("usage_samples")
    )
)

# print("task_usage_agg")
# task_usage_agg.show()

# ============================================================
# OPTIMIZATION 5: Broadcast Join
# Check if one DataFrame is small enough for broadcast
# ============================================================
if USE_BROADCAST_JOIN:
    # If task_eviction_flag is small, broadcast it
    task_level = (
        task_usage_agg
        .join(F.broadcast(task_eviction_flag), on=["job_id", "task_index"], how="left")
        .fillna({"was_evicted": 0})
    )
    print("Applied broadcast join optimization")
else:
    # Regular join (sort-merge join)
    task_level = (
        task_usage_agg
        .join(task_eviction_flag, on=["job_id", "task_index"], how="left")
        .fillna({"was_evicted": 0})
    )
    print("Using regular sort-merge join")

# print("task_level resource usage joined with evicted tasks")
# task_level.show()

# Final caching if enabled
if USE_CACHING:
    task_level.cache()
    final_count = task_level.count()
    print(f"Final result count: {final_count}")
else:
    # Trigger final action to ensure all computations complete
    final_count = task_level.count()
    print(f"Final result count: {final_count}")
