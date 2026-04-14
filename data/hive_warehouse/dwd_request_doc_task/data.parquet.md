# dwd_request_doc_task

- 分层: `DWD`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
将 request 输入转换为可执行任务，并标记是否需要新解析。

## 粒度
一行表示某个 request 中某个文档的一条任务。

## 上下游
- 上游: 由 `scripts/step0_ingest_request.py` 生成。
- 下游: 供 `step1_render.py`、`step2_ocr.py`、`step3_aggregate.py`、`step4_validation.py` 使用。

## 字段
- `request_id`
- `owner`
- `pdf_url`
- `doc_id`
- `file_name`
- `submit_time`
- `parse_needed`

## 备注
- `parse_needed=true` 表示文档需要进入解析主链。
- `parse_needed=false` 表示该文档可直接复用历史结果。
