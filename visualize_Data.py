from load_data import DataLoader

def main():
    loader = DataLoader()
    
    df_machine = loader.load_machine_events()
    df_events = loader.load_job_events()
    df_task = loader.load_task_events()
    df_usage = loader.load_task_usage()
    loader.inspect_df(df_machine, "Machine Events")
    
    loader.inspect_df(df_events, "Job Events")
    
    loader.inspect_df(df_task, "Task Events")
    
    loader.inspect_df(df_usage, "Task Usage")
    loader.stop()
    
    
if __name__ == "__main__":
    main()