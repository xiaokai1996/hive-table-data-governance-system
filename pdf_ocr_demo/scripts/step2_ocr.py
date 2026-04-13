import pandas as pd
import random
from datetime import datetime

INPUT = "data/dwd_page_render/data.parquet"
OUTPUT = "data/dwd_page_ocr/data.parquet"

df = pd.read_parquet(INPUT)

def mock_ocr(img_path):
    return f"text from {img_path}", round(random.uniform(0.9, 0.99), 3)

rows = []

for _, r in df.iterrows():
    text, conf = mock_ocr(r["image_uri"])

    rows.append({
        "doc_id": r["doc_id"],
        "page_id": r["page_id"],
        "ocr_text": text,
        "ocr_confidence": conf,
        "ocr_version": "v1",
        "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

out_df = pd.DataFrame(rows)
out_df.to_parquet(OUTPUT, index=False)

print("✅ OCR 完成:", len(out_df))