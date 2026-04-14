# dwd_doc_aggregate

- 分层: `DWD`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录文档级聚合结果，是历史结果复用的核心底座。

## 粒度
一行表示一个文档的聚合结果。

## 上下游
- 上游: 由 `scripts/step3_aggregate.py` 生成。
- 下游: 供 `scripts/step0_ingest_request.py` 判定复用，供 `scripts/step4_validation.py` 生成交付结果。

## 字段
- `doc_id`
- `page_cnt`
- `doc_text`
- `quality_score`
- `pdf_url`
- `aggregate_version`
- `process_time`
- `source_request_id`

## 备注
- 命中相同 `doc_id` 时，后续 request 可直接复用该表结果。
- `doc_text` 是按页聚合后的全文结果。
