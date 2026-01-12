
from load_data import DataLoader
from pyspark.sql.functions import col, count, sum, desc

def main():
    loader = DataLoader()
    
    df_machine = loader.load_machine_events()
    

    print("\n" + "="*50)
    print("STARTING ANALYSIS")
    print("="*50)

    print("\n>>> Q1: Machine Distribution by CPU Capacity")
                          
    q1_result = df_machine.filter("cpus IS NOT NULL") \
                      .select("machine_id", "cpus") \
                      .dropDuplicates(["machine_id"]) \
                      .groupBy("cpus") \
                      .count() \
                      .orderBy("cpus")
                      
    q11_result = df_machine.filter("cpus IS NOT NULL") \
                      .select("machine_id", "cpus") \
                      .dropDuplicates(["machine_id"]) \
                      .groupBy("cpus") \
                      .count() \
                      .orderBy("cpus")
    


    
    q1_result.show()
    q11_result.show()

    loader.stop()

if __name__ == "__main__":
    main()