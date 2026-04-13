import pandas as pd

INPUT = "data/dwd_page_ocr/data.parquet"
OUTPUT = "data/dwd_doc_aggregate/data.parquet"

df = pd.read_parquet(INPUT)

agg_df = df.groupby("doc_id").agg({
    "page_id": "count",
    "ocr_text": lambda x: "\n".join(x),
    "ocr_confidence": "mean"
}).reset_index()

agg_df.columns = [
    "doc_id",
    "page_cnt",
    "doc_text",
    "quality_score"
]

agg_df.to_parquet(OUTPUT, index=False)

print("✅ aggregate 完成:", len(agg_df))