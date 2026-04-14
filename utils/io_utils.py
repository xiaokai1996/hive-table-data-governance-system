import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd


def ensure_dir(dir_path):
    Path(dir_path).mkdir(parents=True, exist_ok=True)


def list_files(dir_path, ext=".pdf"):
    if not os.path.exists(dir_path):
        return []
    return sorted(
        os.path.join(dir_path, file_name)
        for file_name in os.listdir(dir_path)
        if file_name.endswith(ext)
    )


def save_parquet(df, path):
    ensure_dir(os.path.dirname(path))
    df.to_parquet(path, index=False)


def read_parquet(path, default_columns=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=default_columns or [])
    return pd.read_parquet(path)


def upsert_dataframe(existing_df, new_df, subset):
    if new_df is None or new_df.empty:
        return existing_df.copy() if existing_df is not None else pd.DataFrame()
    if existing_df is None or existing_df.empty:
        return new_df.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)

    combined = pd.concat([existing_df, new_df], ignore_index=True, sort=False)
    return combined.drop_duplicates(subset=subset, keep="last").reset_index(drop=True)


def current_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_pdf_url(pdf_url):
    parsed = urlparse(str(pdf_url))
    if parsed.scheme in ("http", "https"):
        return str(pdf_url)
    if parsed.scheme == "file":
        return str(Path(parsed.path).resolve())
    return str(Path(str(pdf_url)).resolve())


def resolve_local_pdf_path(pdf_url):
    parsed = urlparse(str(pdf_url))
    if parsed.scheme in ("", "file"):
        path = parsed.path if parsed.scheme == "file" else str(pdf_url)
        return str(Path(path).resolve())
    raise ValueError(f"Only local file paths are supported in this demo: {pdf_url}")


def file_md5(file_path, chunk_size=1024 * 1024):
    digest = hashlib.md5()
    with open(file_path, "rb") as file_obj:
        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def build_doc_id(pdf_url):
    pdf_path = resolve_local_pdf_path(pdf_url)
    return file_md5(pdf_path)


def is_md5_doc_id(value):
    return bool(re.fullmatch(r"[0-9a-f]{32}", str(value or "")))


def filter_md5_doc_rows(df, doc_id_col="doc_id"):
    if df is None or df.empty or doc_id_col not in df.columns:
        return df
    return df[df[doc_id_col].map(is_md5_doc_id)].reset_index(drop=True)
