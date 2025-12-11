import pandas as pd
    
csv_file_path = 'yellow_tripdata_2016-01.csv'
df = pd.read_csv(csv_file_path)

parquet_file_path = 'yellow_tripdata_2016-01.parquet'
df.to_parquet(parquet_file_path, index=False) # index=False prevents writing the DataFrame index as a column