import argparse
import os
import sys

import pandas as pd

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.io_utils import build_doc_id, current_ts, filter_md5_doc_rows, list_files, normalize_pdf_url, read_parquet, resolve_local_pdf_path, save_parquet, upsert_dataframe
from utils.path_config import DIM_PDF_DOCUMENT_PATH, DWD_DOC_AGGREGATE_PATH, DWD_DOC_PARSE_STATUS_PATH, DWD_REQUEST_DOC_TASK_PATH, ODS_PDF_REQUEST_ITEM_PATH, RAW_PDF_DIR

INPUT_DIR = RAW_PDF_DIR
REQUEST_ITEM_PATH = ODS_PDF_REQUEST_ITEM_PATH
TASK_PATH = DWD_REQUEST_DOC_TASK_PATH
DIM_DOC_PATH = DIM_PDF_DOCUMENT_PATH
DOC_STATUS_PATH = DWD_DOC_PARSE_STATUS_PATH
DOC_AGG_PATH = DWD_DOC_AGGREGATE_PATH


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--owner", default="demo_user")
    return parser.parse_args()


def build_request_items(request_id, owner, submit_time):
    pdf_files = list_files(INPUT_DIR)
    if not pdf_files:
        raise ValueError(f"No PDF files found under {INPUT_DIR}")

    rows = []
    for pdf_file in pdf_files:
        pdf_url = normalize_pdf_url(pdf_file)
        pdf_path = resolve_local_pdf_path(pdf_url)
        rows.append(
            {
                "request_id": request_id,
                "owner": owner,
                "pdf_url": pdf_url,
                "doc_id": build_doc_id(pdf_url),
                "file_name": os.path.basename(pdf_path),
                "submit_time": submit_time,
            }
        )

    return pd.DataFrame(rows).drop_duplicates(subset=["request_id", "doc_id"], keep="last")


def build_doc_dimension(existing_dim_df, request_items_df, submit_time):
    output_columns = ["doc_id", "pdf_url", "file_name", "file_size_bytes", "first_seen_time"]
    dim_map = {}

    if not existing_dim_df.empty:
        for row in existing_dim_df.to_dict("records"):
            dim_map[row["doc_id"]] = row

    for row in request_items_df.to_dict("records"):
        doc_id = row["doc_id"]
        pdf_path = resolve_local_pdf_path(row["pdf_url"])
        current = dim_map.get(
            doc_id,
            {
                "doc_id": doc_id,
                "pdf_url": row["pdf_url"],
                "file_name": row["file_name"],
                "file_size_bytes": os.path.getsize(pdf_path),
                "first_seen_time": submit_time,
            },
        )
        current["pdf_url"] = row["pdf_url"]
        current["file_name"] = row["file_name"]
        current["file_size_bytes"] = os.path.getsize(pdf_path)
        dim_map[doc_id] = current

    return pd.DataFrame(dim_map.values())[output_columns].sort_values("doc_id").reset_index(drop=True)


def build_doc_status(existing_status_df, request_items_df, parsed_doc_ids, request_id, submit_time):
    output_columns = [
        "doc_id",
        "parse_status",
        "parsed_time",
        "last_request_id",
        "last_seen_time",
        "last_update_time",
    ]
    status_map = {}

    if not existing_status_df.empty:
        for row in existing_status_df.to_dict("records"):
            status_map[row["doc_id"]] = row

    for row in request_items_df.to_dict("records"):
        doc_id = row["doc_id"]
        current = status_map.get(
            doc_id,
            {
                "doc_id": doc_id,
                "parse_status": "parsed" if doc_id in parsed_doc_ids else "pending",
                "parsed_time": submit_time if doc_id in parsed_doc_ids else None,
                "last_request_id": request_id,
                "last_seen_time": submit_time,
                "last_update_time": submit_time,
            },
        )
        current["last_request_id"] = request_id
        current["last_seen_time"] = submit_time
        current["last_update_time"] = submit_time
        if doc_id in parsed_doc_ids:
            current["parse_status"] = "parsed"
            current["parsed_time"] = current.get("parsed_time") or submit_time
        status_map[doc_id] = current

    return pd.DataFrame(status_map.values())[output_columns].sort_values("doc_id").reset_index(drop=True)


def main():
    args = parse_args()
    submit_time = current_ts()

    request_items_df = build_request_items(args.request_id, args.owner, submit_time)

    existing_request_df = read_parquet(
        REQUEST_ITEM_PATH,
        default_columns=["request_id", "owner", "pdf_url", "doc_id", "file_name", "submit_time"],
    )
    existing_request_df = filter_md5_doc_rows(existing_request_df)
    existing_request_df = existing_request_df[existing_request_df["request_id"] != args.request_id]
    request_output_df = upsert_dataframe(existing_request_df, request_items_df, ["request_id", "doc_id"])
    request_output_df = request_output_df[
        ["request_id", "owner", "pdf_url", "doc_id", "file_name", "submit_time"]
    ]
    save_parquet(request_output_df, REQUEST_ITEM_PATH)

    existing_status_df = read_parquet(
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
    existing_status_df = filter_md5_doc_rows(existing_status_df)
    doc_agg_df = read_parquet(
        DOC_AGG_PATH,
        default_columns=["doc_id", "page_cnt", "doc_text", "quality_score"],
    )
    doc_agg_df = filter_md5_doc_rows(doc_agg_df)
    parsed_doc_ids = set(doc_agg_df["doc_id"].dropna().tolist()) if "doc_id" in doc_agg_df.columns else set()
    if not existing_status_df.empty and "parse_status" in existing_status_df.columns:
        parsed_doc_ids.update(
            existing_status_df[existing_status_df["parse_status"] == "parsed"]["doc_id"].dropna().tolist()
        )

    task_df = request_items_df.copy()
    task_df["parse_needed"] = ~task_df["doc_id"].isin(parsed_doc_ids)

    existing_task_df = read_parquet(
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
    existing_task_df = filter_md5_doc_rows(existing_task_df)
    existing_task_df = existing_task_df[existing_task_df["request_id"] != args.request_id]
    task_output_df = upsert_dataframe(existing_task_df, task_df, ["request_id", "doc_id"])
    task_output_df = task_output_df[
        ["request_id", "owner", "pdf_url", "doc_id", "file_name", "submit_time", "parse_needed"]
    ]
    save_parquet(task_output_df, TASK_PATH)

    existing_dim_df = read_parquet(
        DIM_DOC_PATH,
        default_columns=[
            "doc_id",
            "pdf_url",
            "file_name",
            "file_size_bytes",
            "first_seen_time",
        ],
    )
    existing_dim_df = filter_md5_doc_rows(existing_dim_df)
    dim_output_df = build_doc_dimension(
        existing_dim_df=existing_dim_df,
        request_items_df=request_items_df,
        submit_time=submit_time,
    )
    save_parquet(dim_output_df, DIM_DOC_PATH)

    status_output_df = build_doc_status(
        existing_status_df=existing_status_df,
        request_items_df=request_items_df,
        parsed_doc_ids=parsed_doc_ids,
        request_id=args.request_id,
        submit_time=submit_time,
    )
    save_parquet(status_output_df, DOC_STATUS_PATH)

    reuse_count = int((~task_df["parse_needed"]).sum())
    parse_count = int(task_df["parse_needed"].sum())
    print(
        f"Request {args.request_id} ingested: total={len(task_df)}, "
        f"reuse={reuse_count}, need_parse={parse_count}"
    )


if __name__ == "__main__":
    main()
