from load_data import DataLoader
from pyspark.sql.functions import col, count, sum, desc
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def main():
    loader = DataLoader()
        
    df_task_events = loader.load_task_events()
    df_job_events = loader.load_job_events()

    unique_tasks_count = df_task_events.select("job_id", "task_index").dropDuplicates().agg(
        count("*").alias("unique_task_count")
    ).collect()[0]["unique_task_count"]
    
    killed_evicted_tasks_count = df_task_events.filter(col("event_type").isin([2, 5])).select("job_id", "task_index").dropDuplicates().agg(
        count("*").alias("killed_evicted_task_count")
    ).collect()[0]["killed_evicted_task_count"]
    
    percentage_killed_evicted = (killed_evicted_tasks_count / unique_tasks_count) * 100
    
    print(percentage_killed_evicted)
    
    unique_jobs_count = df_job_events.select("job_id").dropDuplicates().agg(
        count("*").alias("unique_job_count")
    ).collect()[0]["unique_job_count"]
    
    killed_evicted_jobs_count = df_job_events.filter(col("event_type").isin([2, 5])).select("job_id").dropDuplicates().agg(
        count("*").alias("killed_evicted_job_count")
    ).collect()[0]["killed_evicted_job_count"]
    
    percentage_killed_evicted_jobs = (killed_evicted_jobs_count / unique_jobs_count) * 100
    
    print(percentage_killed_evicted_jobs)
    
    taska_killed_evicted_per_scheduling_class = df_task_events.filter(col("event_type").isin([2, 5])).select("scheduling_class", "job_id", "task_index").dropDuplicates().groupBy("scheduling_class").agg(
        count("*").alias("killed_evicted_task_count")
    ).orderBy("scheduling_class")
    taska_killed_evicted_per_scheduling_class = taska_killed_evicted_per_scheduling_class.withColumn(
        "percentage_killed_evicted",
        (col("killed_evicted_task_count") / unique_tasks_count) * 100
    )
    taska_killed_evicted_per_scheduling_class.show()
    
    jobs_killed_evicted_per_scheduling_class = df_job_events.filter(col("event_type").isin([2, 5])).select("scheduling_class", "job_id").dropDuplicates().groupBy("scheduling_class").agg(
        count("*").alias("killed_evicted_job_count")
    ).orderBy("scheduling_class")
    jobs_killed_evicted_per_scheduling_class = jobs_killed_evicted_per_scheduling_class.withColumn(
        "percentage_killed_evicted",
        (col("killed_evicted_job_count") / unique_jobs_count) * 100
    )
    jobs_killed_evicted_per_scheduling_class.show()
    # Graphical representation percentage of killed/evicted tasks per scheduling class
    taska_killed_evicted_per_scheduling_class_pd = taska_killed_evicted_per_scheduling_class.toPandas().sort_values("scheduling_class")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(taska_killed_evicted_per_scheduling_class_pd["scheduling_class"].astype(str), taska_killed_evicted_per_scheduling_class_pd["percentage_killed_evicted"], color="salmon", label="Percentage of Killed/Evicted Tasks")
    ax.set_xlabel("Scheduling Class", fontsize=12)
    ax.set_ylabel("Percentage of Killed/Evicted Tasks (%)", fontsize=12)
    ax.set_title("Percentage of Killed/Evicted Tasks per Scheduling Class", fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=0)
    fig.tight_layout()
    plt.show()
    
    loader.stop()

if __name__ == "__main__":
    main()