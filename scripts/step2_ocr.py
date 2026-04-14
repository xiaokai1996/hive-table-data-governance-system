import argparse
import os
import random
import sys

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.io_utils import current_ts, filter_md5_doc_rows, read_parquet, save_parquet, upsert_dataframe
from utils.path_config import DWD_PAGE_OCR_PATH, DWD_PAGE_RENDER_PATH, DWD_REQUEST_DOC_TASK_PATH

TASK_PATH = DWD_REQUEST_DOC_TASK_PATH
INPUT_PATH = DWD_PAGE_RENDER_PATH
OUTPUT_PATH = DWD_PAGE_OCR_PATH


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    return parser.parse_args()


def mock_ocr(image_path):
    return f"text from {image_path}", round(random.uniform(0.9, 0.99), 3)


def main():
    args = parse_args()

    task_df = read_parquet(
        TASK_PATH,
        default_columns=[
            "request_id",
            "owner",
            "pdf_url",
            "doc_id",
            "file_name",
            "submit_time",
            "parse_needed",
        ],
    )
    target_doc_ids = set(
        task_df[
            (task_df["request_id"] == args.request_id) & (task_df["parse_needed"] == True)
        ]["doc_id"].dropna().tolist()
    )

    render_df = read_parquet(
        INPUT_PATH,
        default_columns=[
            "doc_id",
            "pdf_url",
            "page_id",
            "page_no",
            "image_uri",
            "render_version",
            "process_time",
            "source_request_id",
        ],
    )
    render_df = filter_md5_doc_rows(render_df)
    target_render_df = render_df[render_df["doc_id"].isin(target_doc_ids)].copy()

    existing_df = read_parquet(
        OUTPUT_PATH,
        default_columns=[
            "doc_id",
            "page_id",
            "ocr_text",
            "ocr_confidence",
            "ocr_version",
            "process_time",
            "source_request_id",
        ],
    )
    existing_df = filter_md5_doc_rows(existing_df)

    if not target_doc_ids:
        save_parquet(existing_df, OUTPUT_PATH)
        print(f"Request {args.request_id} ocr skipped: no new documents")
        return

    processed_page_ids = set(existing_df["page_id"].dropna().tolist()) if "page_id" in existing_df.columns else set()

    rows = []
    for row in target_render_df.to_dict("records"):
        if row["page_id"] in processed_page_ids:
            continue
        text, confidence = mock_ocr(row["image_uri"])
        rows.append(
            {
                "doc_id": row["doc_id"],
                "page_id": row["page_id"],
                "ocr_text": text,
                "ocr_confidence": confidence,
                "ocr_version": "v2",
                "process_time": current_ts(),
                "source_request_id": args.request_id,
            }
        )

    new_df = pd.DataFrame(rows)
    output_df = upsert_dataframe(existing_df, new_df, ["page_id"])
    save_parquet(output_df, OUTPUT_PATH)

    print(f"Request {args.request_id} ocr finished: {len(new_df)} pages")


if __name__ == "__main__":
    main()
