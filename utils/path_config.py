DATA_ROOT = "data"

OBJECT_STORAGE_ROOT = f"{DATA_ROOT}/object_storage"
RAW_PDF_DIR = f"{OBJECT_STORAGE_ROOT}/raw_pdfs"
PAGE_IMAGE_DIR = f"{OBJECT_STORAGE_ROOT}/page_images"

HIVE_WAREHOUSE_ROOT = f"{DATA_ROOT}/hive_warehouse"

ODS_PDF_REQUEST_ITEM_PATH = f"{HIVE_WAREHOUSE_ROOT}/ods_pdf_request_item/data.parquet"
DIM_PDF_DOCUMENT_PATH = f"{HIVE_WAREHOUSE_ROOT}/dim_pdf_document/data.parquet"
DWD_DOC_PARSE_STATUS_PATH = f"{HIVE_WAREHOUSE_ROOT}/dwd_doc_parse_status/data.parquet"
DWD_REQUEST_DOC_TASK_PATH = f"{HIVE_WAREHOUSE_ROOT}/dwd_request_doc_task/data.parquet"
DWD_PAGE_RENDER_PATH = f"{HIVE_WAREHOUSE_ROOT}/dwd_page_render/data.parquet"
DWD_PAGE_OCR_PATH = f"{HIVE_WAREHOUSE_ROOT}/dwd_page_ocr/data.parquet"
DWD_DOC_AGGREGATE_PATH = f"{HIVE_WAREHOUSE_ROOT}/dwd_doc_aggregate/data.parquet"
ADS_REQUEST_DELIVERY_ITEM_PATH = f"{HIVE_WAREHOUSE_ROOT}/ads_request_delivery_item/data.parquet"
ADS_REQUEST_DELIVERY_STATUS_PATH = f"{HIVE_WAREHOUSE_ROOT}/ads_request_delivery_status/data.parquet"

# Historical legacy table kept only for compatibility and inspection.
LEGACY_ADS_REQUEST_DELIVERY_PATH = f"{HIVE_WAREHOUSE_ROOT}/ads_request_delivery/data.parquet"
