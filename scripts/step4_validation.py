import argparse
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.io_utils import current_ts, filter_md5_doc_rows, read_parquet, save_parquet, upsert_dataframe
from utils.path_config import ADS_REQUEST_DELIVERY_ITEM_PATH, ADS_REQUEST_DELIVERY_STATUS_PATH, DWD_DOC_AGGREGATE_PATH, DWD_REQUEST_DOC_TASK_PATH

TASK_PATH = DWD_REQUEST_DOC_TASK_PATH
DOC_PATH = DWD_DOC_AGGREGATE_PATH
DELIVERY_ITEM_PATH = ADS_REQUEST_DELIVERY_ITEM_PATH
DELIVERY_STATUS_PATH = ADS_REQUEST_DELIVERY_STATUS_PATH


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    return parser.parse_args()


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
    task_df = filter_md5_doc_rows(task_df)
    request_task_df = task_df[task_df["request_id"] == args.request_id].copy()
    if request_task_df.empty:
        raise ValueError(f"Request {args.request_id} not found in {TASK_PATH}")

    doc_df = read_parquet(
        DOC_PATH,
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
    doc_df = filter_md5_doc_rows(doc_df)

    delivery_df = request_task_df.merge(
        doc_df[["doc_id", "page_cnt", "doc_text", "quality_score"]],
        on="doc_id",
        how="left",
    )
    missing_doc_ids = delivery_df[delivery_df["doc_text"].isna()]["doc_id"].dropna().tolist()
    if missing_doc_ids:
        raise ValueError(f"Missing aggregate result for docs: {missing_doc_ids}")

    delivery_df["delivery_source"] = delivery_df["parse_needed"].map(
        {True: "newly_parsed", False: "cached"}
    )
    delivery_df["delivery_time"] = current_ts()

    delivery_item_df = delivery_df[
        [
            "request_id",
            "owner",
            "doc_id",
            "pdf_url",
            "delivery_source",
            "page_cnt",
            "doc_text",
            "quality_score",
            "delivery_time",
        ]
    ].copy()

    delivery_status_df = delivery_df[["request_id", "doc_id"]].copy()
    delivery_status_df["delivery_status"] = "delivered"
    delivery_status_df["status_time"] = delivery_df["delivery_time"]

    existing_item_df = read_parquet(
        DELIVERY_ITEM_PATH,
        default_columns=[
            "request_id",
            "owner",
            "doc_id",
            "pdf_url",
            "delivery_source",
            "page_cnt",
            "doc_text",
            "quality_score",
            "delivery_time",
        ],
    )
    existing_item_df = existing_item_df[existing_item_df["request_id"] != args.request_id]
    delivery_item_df = upsert_dataframe(existing_item_df, delivery_item_df, ["request_id", "doc_id"])
    save_parquet(delivery_item_df, DELIVERY_ITEM_PATH)

    existing_status_df = read_parquet(
        DELIVERY_STATUS_PATH,
        default_columns=["request_id", "doc_id", "delivery_status", "status_time"],
    )
    existing_status_df = existing_status_df[existing_status_df["request_id"] != args.request_id]
    delivery_status_df = upsert_dataframe(existing_status_df, delivery_status_df, ["request_id", "doc_id"])
    save_parquet(delivery_status_df, DELIVERY_STATUS_PATH)

    cached_count = int((delivery_df["delivery_source"] == "cached").sum())
    parsed_count = int((delivery_df["delivery_source"] == "newly_parsed").sum())
    print(
        f"Request {args.request_id} delivery finished: total={len(delivery_df)}, "
        f"cached={cached_count}, newly_parsed={parsed_count}"
    )


if __name__ == "__main__":
    main()
