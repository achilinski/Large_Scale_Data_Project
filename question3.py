from load_data import DataLoader
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    loader = DataLoader()
    
    df_machine = loader.load_machine_events()
    

    print("\n" + "="*50)
    print("STARTING ANALYSIS")
    print("="*50)

    print("\n>>> Q3: Percentage of computational power lost due to maintenance")
    
    machine_window = Window.partitionBy("machine_id").orderBy("timestamp")
                          
    filtered_events = df_machine.filter("event_type = 1 or event_type = 0")
    df_total_machines = df_machine.filter("cpus IS NOT NULL") \
                                  .dropDuplicates(["machine_id"]) \
                                  .groupBy("cpus") \
                                  .count() \
                                  .withColumnRenamed("count", "total_count")
                                  
    df_with_lead = filtered_events.withColumn(
        "next_timestamp",
        F.lead("timestamp", 1).over(machine_window)
    ).withColumn(
        "next_event_type",
        F.lead("event_type", 1).over(machine_window)
    )
    

    df_only_down = df_with_lead.filter("event_type = 1 AND next_event_type = 0")
    #df_only_down = df_with_lead.filter("event_type = 1")
    
    df_number_of_incidents = df_only_down.groupBy("cpus").agg(
        F.count("*").alias("num_incidents")
    )
    df_down_with_time = df_only_down.withColumn(
        "downtime",
        F.col("next_timestamp") - F.col("timestamp")
    )
    df_down_time_per_cpu = df_down_with_time.groupBy("cpus").agg(
        F.sum("downtime").alias("total_downtime")
    )

    df_analysis = df_down_time_per_cpu.join(df_total_machines, "cpus").join(df_number_of_incidents, "cpus")
    final_result = df_analysis.withColumn(
        "avg_downtime_per_machine",
        F.col("total_downtime") / F.col("total_count")
    ).withColumn("avg_incidents_per_machine", F.col("num_incidents") / F.col("total_count")).orderBy("avg_downtime_per_machine")

    final_result.show()

    loader.stop()

if __name__ == "__main__":
    main()