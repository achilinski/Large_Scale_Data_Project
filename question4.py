
from load_data import DataLoader
from pyspark.sql.functions import col, count, sum, desc
import matplotlib.pyplot as plt
import numpy as np

def main():
    loader = DataLoader()
        
    df_job = loader.load_job_events()
    
    df_task = loader.load_task_events()

    print("\n" + "="*50)
    print("STARTING ANALYSIS")
    print("="*50)

    print("\n>>> Q4: distribution of the number of jobs/tasks per scheduling class")
                          
    q4_jobs = df_job.dropDuplicates(["job_id"]).groupBy("scheduling_class").agg(
        count("job_id").alias("num_jobs")
    ).orderBy("scheduling_class")

    q4_tasks = df_task.dropDuplicates(["job_id", "task_index"]) \
    .groupBy("scheduling_class") \
    .agg(
        count("job_id").alias("num_tasks") 
    ).orderBy("scheduling_class")
    q4_total_tasks = df_task.dropDuplicates(["job_id", "task_index"]).count()
    q4_total_tasks_per_class = q4_tasks.withColumn(
        "percentage_of_total_tasks",
        (col("num_tasks") / q4_total_tasks) * 100
    ).orderBy("scheduling_class")

    q4_joined = q4_jobs.join(q4_tasks, "scheduling_class").withColumn("tasks_per_job", col("num_tasks") / col("num_jobs")).orderBy("scheduling_class")
    q4_joined.show()
    
    # Graphical representation: Total Number of Tasks per Scheduling Class
    q4_total_tasks_per_class_pandas = q4_total_tasks_per_class.toPandas().sort_values("scheduling_class")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(q4_total_tasks_per_class_pandas["scheduling_class"].astype(str), q4_total_tasks_per_class_pandas["num_tasks"], color="skyblue", label="Number of Tasks")
    ax.set_xlabel("Scheduling Class", fontsize=12)
    ax.set_ylabel("Number of Tasks", fontsize=12)
    ax.set_title("Total Number of Tasks per Scheduling Class", fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.xticks(rotation=0)
    fig.tight_layout()
    plt.show()
    
    
    # q4_tasks_pandas = q4_jobs.toPandas().sort_values("scheduling_class")
    
    # fig, ax = plt.subplots(figsize=(10, 6))
    # ax.bar(q4_tasks_pandas["scheduling_class"].astype(str), q4_tasks_pandas["num_jobs"], color="steelblue")
    # ax.set_xlabel("Scheduling Class", fontsize=12)
    # ax.set_ylabel("Number of Jobs", fontsize=12)
    # ax.set_title("Number of Jobs per Scheduling Class", fontsize=14, fontweight="bold")
    # ax.grid(axis="y", alpha=0.3)
    
    # fig.tight_layout()
    # plt.show()

    loader.stop()

if __name__ == "__main__":
    main()