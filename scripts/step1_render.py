import argparse
import os
import sys

import pandas as pd
import pypdfium2 as pdfium

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from utils.io_utils import current_ts, ensure_dir, filter_md5_doc_rows, read_parquet, resolve_local_pdf_path, save_parquet, upsert_dataframe
from utils.path_config import DWD_PAGE_RENDER_PATH, DWD_REQUEST_DOC_TASK_PATH, PAGE_IMAGE_DIR

TASK_PATH = DWD_REQUEST_DOC_TASK_PATH
OUTPUT_PATH = DWD_PAGE_RENDER_PATH
IMG_DIR = PAGE_IMAGE_DIR


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_dir(IMG_DIR)

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
    target_docs_df = task_df[
        (task_df["request_id"] == args.request_id) & (task_df["parse_needed"] == True)
    ][["request_id", "pdf_url", "doc_id"]].drop_duplicates(subset=["doc_id"])

    existing_df = read_parquet(
        OUTPUT_PATH,
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
    existing_df = filter_md5_doc_rows(existing_df)

    if target_docs_df.empty:
        save_parquet(existing_df, OUTPUT_PATH)
        print(f"Request {args.request_id} render skipped: no new documents")
        return

    rendered_doc_ids = set(existing_df["doc_id"].dropna().tolist()) if "doc_id" in existing_df.columns else set()

    rows = []
    for row in target_docs_df.to_dict("records"):
        if row["doc_id"] in rendered_doc_ids:
            continue

        pdf_path = resolve_local_pdf_path(row["pdf_url"])
        pdf = pdfium.PdfDocument(pdf_path)

        for page_index in range(len(pdf)):
            page_no = page_index + 1
            page_id = f"{row['doc_id']}_{page_no}"
            image_uri = os.path.join(IMG_DIR, f"{page_id}.png")

            page = pdf[page_index]
            pil_img = page.render(scale=2).to_pil()
            pil_img.save(image_uri)

            rows.append(
                {
                    "doc_id": row["doc_id"],
                    "pdf_url": row["pdf_url"],
                    "page_id": page_id,
                    "page_no": page_no,
                    "image_uri": image_uri,
                    "render_version": "v2",
                    "process_time": current_ts(),
                    "source_request_id": args.request_id,
                }
            )

    new_df = pd.DataFrame(rows)
    output_df = upsert_dataframe(existing_df, new_df, ["page_id"])
    save_parquet(output_df, OUTPUT_PATH)

    print(f"Request {args.request_id} render finished: {len(new_df)} pages")


if __name__ == "__main__":
    main()
