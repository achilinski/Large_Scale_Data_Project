from load_data import DataLoader
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    loader = DataLoader()
    
    df_machine = loader.load_machine_events()
    

    print("\n" + "="*50)
    print("STARTING ANALYSIS")
    print("="*50)

    print("\n>>> Q2: Percentage of computational power lost due to maintenance")
    
    machine_window = Window.partitionBy("machine_id").orderBy("timestamp")
                          
    filtered_events = df_machine.filter("event_type = 1 or event_type = 0")
    
    df_with_lead = filtered_events.withColumn(
        "next_timestamp",
        F.lead("timestamp", 1).over(machine_window)
    ).withColumn(
        "next_event_type",
        F.lead("event_type", 1).over(machine_window)
    )
    

    df_only_down = df_with_lead.filter("event_type = 1 AND next_event_type = 0")
    df_down_with_time = df_only_down.withColumn(
        "downtime",
        F.col("next_timestamp") - F.col("timestamp")
    )
    df_down_power = df_down_with_time.withColumn(
        "lost_power",
        F.col("downtime") * F.col("cpus")
    )
    
    total_lost_power = df_down_power.agg(
        F.sum("lost_power").alias("total_lost_power")
    ).collect()[0]["total_lost_power"]
        
    total_time = df_machine.agg(
        (F.max("timestamp") - F.min("timestamp")).alias("total_time")
    ).collect()[0]["total_time"]
    
    total_power = df_machine.dropDuplicates(["machine_id"]).select(
        F.sum(F.col("cpus") * total_time).alias("total_power")
    ).collect()[0]["total_power"]
    
    print("Total Lost Power due to Maintenance:", total_lost_power)
    print("Total Power:", total_power)
    print("Percentage of Power Lost due to Maintenance: {:.4f}%".format(
        (total_lost_power / total_power) * 100
    ))

    loader.stop()

if __name__ == "__main__":
    main()