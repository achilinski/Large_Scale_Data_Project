
from load_data import DataLoader
from pyspark.sql.functions import col, count, sum, desc

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

    
    q4_tasks = df_task.dropDuplicates(["job_id"]).groupBy("scheduling_class").agg(
        count("job_id").alias("num_tasks")
    ).orderBy("scheduling_class")
    
    q4_joined = q4_jobs.join(q4_tasks, "scheduling_class")
    q4_joined.show()

    loader.stop()

if __name__ == "__main__":
    main()