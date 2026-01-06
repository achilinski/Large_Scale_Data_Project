  # 1. Machine Events (Download the single full file)
gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz .

# 2. Job Events (Parts 265-269)
gsutil cp "gs://clusterdata-2011-2/job_events/part-0026[5-9]-*.csv.gz" .

# 3. Task Events (Parts 265-269)
gsutil cp "gs://clusterdata-2011-2/task_events/part-0026[5-9]-*.csv.gz" .

# 4. Task Usage (Parts 265-269)
gsutil cp "gs://clusterdata-2011-2/task_usage/part-0026[5-9]-*.csv.gz" .

Move all files to their own folder like:
./dataset/machine_events
./dataset/job_Events
./dataset/task_events
./dataset/task_usage

To use DataLoader class refer to question1.py 
