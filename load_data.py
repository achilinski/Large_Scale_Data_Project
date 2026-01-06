from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType

class DataLoader:
    def __init__(self):
        print("Initializing Spark Session...")
        self.spark = SparkSession.builder \
            .appName("GoogleCluster_Parts265-269") \
            .getOrCreate()
        
        self._define_schemas()
        
        self.df_machine = None
        self.df_job = None
        self.df_task = None
        self.df_usage = None

    def _define_schemas(self):
        self.schema_machine = StructType([
            StructField("timestamp", LongType(), True),
            StructField("machine_id", LongType(), True),
            StructField("event_type", IntegerType(), True),
            StructField("platform_id", StringType(), True),
            StructField("cpus", FloatType(), True),
            StructField("memory", FloatType(), True)
        ])

        self.schema_job = StructType([
            StructField("timestamp", LongType(), True),
            StructField("missing_info", IntegerType(), True),
            StructField("job_id", LongType(), True),
            StructField("event_type", IntegerType(), True),
            StructField("user_name", StringType(), True),
            StructField("scheduling_class", IntegerType(), True),
            StructField("job_name", StringType(), True),
            StructField("logical_job_name", StringType(), True)
        ])

        self.schema_task = StructType([
            StructField("timestamp", LongType(), True),
            StructField("missing_info", IntegerType(), True),
            StructField("job_id", LongType(), True),
            StructField("task_index", IntegerType(), True),
            StructField("machine_id", LongType(), True),
            StructField("event_type", IntegerType(), True),
            StructField("user_name", StringType(), True),
            StructField("scheduling_class", IntegerType(), True),
            StructField("priority", IntegerType(), True),
            StructField("cpu_request", FloatType(), True),
            StructField("memory_request", FloatType(), True),
            StructField("disk_request", FloatType(), True),
            StructField("machine_constraint", IntegerType(), True)
        ])

        self.schema_usage = StructType([
            StructField("start_time", LongType(), True),
            StructField("end_time", LongType(), True),
            StructField("job_id", LongType(), True),
            StructField("task_index", IntegerType(), True),
            StructField("machine_id", LongType(), True),
            StructField("cpu_rate", FloatType(), True),
            StructField("canonical_memory_usage", FloatType(), True),
            StructField("assigned_memory_usage", FloatType(), True),
            StructField("unmapped_page_cache", FloatType(), True),
            StructField("total_page_cache", FloatType(), True),
            StructField("max_mem_usage", FloatType(), True),
            StructField("disk_io_time", FloatType(), True),
            StructField("local_disk_space_usage", FloatType(), True),
            StructField("max_cpu_rate", FloatType(), True),
            StructField("max_disk_io_time", FloatType(), True),
            StructField("cycles_per_instruction", FloatType(), True),
            StructField("mean_mpki", FloatType(), True),
            StructField("max_mpki", FloatType(), True),
            StructField("mean_llc_miss", FloatType(), True),
            StructField("max_llc_miss", FloatType(), True)
        ])

    def load_machine_events(self, path="dataset/machine_events/part-*.csv.gz"):
        self.df_machine = self.spark.read.csv(path, schema=self.schema_machine)
        return self.df_machine

    def load_job_events(self, path="dataset/job_events/part-*.csv.gz"):
        self.df_job = self.spark.read.csv(path, schema=self.schema_job)
        return self.df_job

    def load_task_events(self, path="dataset/task_events/part-*.csv.gz"):
        self.df_task = self.spark.read.csv(path, schema=self.schema_task)
        return self.df_task

    def load_task_usage(self, path="dataset/task_usage/part-*.csv.gz"):
        self.df_usage = self.spark.read.csv(path, schema=self.schema_usage)
        return self.df_usage

    def stop(self):
        self.spark.stop()