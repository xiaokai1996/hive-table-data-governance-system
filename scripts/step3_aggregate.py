import argparse
import os
import sys

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.io_utils import current_ts, filter_md5_doc_rows, read_parquet, save_parquet, upsert_dataframe
from utils.path_config import DWD_DOC_AGGREGATE_PATH, DWD_DOC_PARSE_STATUS_PATH, DWD_PAGE_OCR_PATH, DWD_PAGE_RENDER_PATH, DWD_REQUEST_DOC_TASK_PATH

TASK_PATH = DWD_REQUEST_DOC_TASK_PATH
RENDER_PATH = DWD_PAGE_RENDER_PATH
OCR_PATH = DWD_PAGE_OCR_PATH
OUTPUT_PATH = DWD_DOC_AGGREGATE_PATH
DOC_STATUS_PATH = DWD_DOC_PARSE_STATUS_PATH


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    return parser.parse_args()


def update_doc_status(doc_status_df, target_doc_ids, request_id, process_time):
    output_columns = [
        "doc_id",
        "parse_status",
        "parsed_time",
        "last_request_id",
        "last_seen_time",
        "last_update_time",
    ]
    if doc_status_df.empty:
        doc_status_df = pd.DataFrame(columns=output_columns)

    doc_status_df = doc_status_df.copy()
    existing_doc_ids = set(doc_status_df["doc_id"].dropna().tolist()) if "doc_id" in doc_status_df.columns else set()

    for doc_id in target_doc_ids:
        if doc_id not in existing_doc_ids:
            doc_status_df = pd.concat(
                [
                    doc_status_df,
                    pd.DataFrame(
                        [
                            {
                                "doc_id": doc_id,
                                "parse_status": "parsed",
                                "parsed_time": process_time,
                                "last_request_id": request_id,
                                "last_seen_time": process_time,
                                "last_update_time": process_time,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    mask = doc_status_df["doc_id"].isin(target_doc_ids)
    doc_status_df.loc[mask, "parse_status"] = "parsed"
    doc_status_df.loc[mask, "parsed_time"] = process_time
    doc_status_df.loc[mask, "last_request_id"] = request_id
    doc_status_df.loc[mask, "last_update_time"] = process_time
    return doc_status_df[output_columns].sort_values("doc_id").reset_index(drop=True)


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
    target_tasks_df = task_df[
        (task_df["request_id"] == args.request_id) & (task_df["parse_needed"] == True)
    ][["doc_id", "pdf_url"]].drop_duplicates(subset=["doc_id"])
    target_doc_ids = set(target_tasks_df["doc_id"].dropna().tolist())

    render_df = read_parquet(
        RENDER_PATH,
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
    ocr_df = read_parquet(
        OCR_PATH,
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
    ocr_df = filter_md5_doc_rows(ocr_df)

    render_target_df = render_df[render_df["doc_id"].isin(target_doc_ids)][["doc_id", "page_id", "page_no", "pdf_url"]]
    ocr_target_df = ocr_df[ocr_df["doc_id"].isin(target_doc_ids)][["doc_id", "page_id", "ocr_text", "ocr_confidence"]]
    merged_df = render_target_df.merge(ocr_target_df, on=["doc_id", "page_id"], how="inner")
    merged_df = merged_df.sort_values(["doc_id", "page_no"])

    grouped_df = (
        merged_df.groupby("doc_id")
        .agg(
            page_cnt=("page_id", "count"),
            doc_text=("ocr_text", lambda values: "\n".join(values)),
            quality_score=("ocr_confidence", "mean"),
            pdf_url=("pdf_url", "first"),
        )
        .reset_index()
    )
    grouped_df["aggregate_version"] = "v2"
    grouped_df["process_time"] = current_ts()
    grouped_df["source_request_id"] = args.request_id

    existing_output_df = read_parquet(
        OUTPUT_PATH,
        default_columns=[
            "doc_id",
            "pdf_url",
            "page_cnt",
            "doc_text",
            "quality_score",
            "aggregate_version",
            "process_time",
            "source_request_id",
        ],
    )
    existing_output_df = filter_md5_doc_rows(existing_output_df)

    doc_status_df = read_parquet(
        DOC_STATUS_PATH,
        default_columns=[
            "doc_id",
            "parse_status",
            "parsed_time",
            "last_request_id",
            "last_seen_time",
            "last_update_time",
        ],
    )
    doc_status_df = filter_md5_doc_rows(doc_status_df)

    if target_tasks_df.empty:
        save_parquet(existing_output_df, OUTPUT_PATH)
        save_parquet(doc_status_df, DOC_STATUS_PATH)
        print(f"Request {args.request_id} aggregate skipped: no new documents")
        return

    output_df = upsert_dataframe(existing_output_df, grouped_df, ["doc_id"])
    save_parquet(output_df, OUTPUT_PATH)

    doc_status_df = update_doc_status(doc_status_df, target_doc_ids, args.request_id, current_ts())
    save_parquet(doc_status_df, DOC_STATUS_PATH)

    print(f"Request {args.request_id} aggregate finished: {len(grouped_df)} docs")


if __name__ == "__main__":
    main()
