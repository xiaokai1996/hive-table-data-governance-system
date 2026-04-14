# ods_pdf_request_item

- 分层: `ODS`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录业务方提交的原始 request 输入快照，是整个解析链路的入口表。

## 粒度
一行表示某个 request 中的一个 PDF。

## 上下游
- 上游: 由 `scripts/step0_ingest_request.py` 生成。
- 下游: 下游用于生成 `dwd_request_doc_task`、`dim_pdf_document` 和 `dwd_doc_parse_status`。

## 字段
- `request_id`
- `owner`
- `pdf_url`
- `doc_id`
- `file_name`
- `submit_time`

## 备注
- `doc_id` 基于 PDF 文件 bytes 的 MD5 计算。
- 该表保留 request 原始输入语义，不包含解析结果字段。
