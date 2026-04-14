# dim_pdf_document

- 分层: `DIM`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
维护去重后的文档主数据，只存文档事实字段。

## 粒度
一行表示一个逻辑文档。

## 上下游
- 上游: 由 `scripts/step0_ingest_request.py` 生成和维护。
- 下游: 供全链路进行文档主数据查询和解释。

## 字段
- `doc_id`
- `pdf_url`
- `file_name`
- `file_size_bytes`
- `first_seen_time`

## 备注
- 不包含 `page_cnt`、`doc_text`、`delivery_status` 等结果或状态字段。
- `doc_id` 是全局唯一的文档主键。
