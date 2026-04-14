# dwd_doc_parse_status

- 分层: `DWD`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
维护文档解析状态，与文档事实表分离。

## 粒度
一行表示一个文档的当前解析状态。

## 上下游
- 上游: 由 `scripts/step0_ingest_request.py` 初始化，由 `scripts/step3_aggregate.py` 更新。
- 下游: 用于判断文档是否需要进入 render/OCR/aggregate 主链。

## 字段
- `doc_id`
- `parse_status`
- `parsed_time`
- `last_request_id`
- `last_seen_time`
- `last_update_time`

## 备注
- `parse_status` 当前主要使用 `pending` 和 `parsed`。
- 该表是状态表，不承担解析结果交付。
