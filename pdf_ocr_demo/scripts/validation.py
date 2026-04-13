import pandas as pd

df = pd.read_parquet("data/dwd_doc_aggregate/data.parquet")
print(df.head())
