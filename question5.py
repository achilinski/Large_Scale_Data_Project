from load_data import DataLoader
from pyspark.sql.functions import col, count, sum, desc

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
    loader.stop()

if __name__ == "__main__":
    main()