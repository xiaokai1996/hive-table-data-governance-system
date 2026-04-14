# dwd_page_ocr

- 分层: `DWD`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录页面级 OCR 结果。

## 粒度
一行表示一个页面的 OCR 输出。

## 上下游
- 上游: 由 `scripts/step2_ocr.py` 生成。
- 下游: 供 `scripts/step3_aggregate.py` 聚合为文档级结果。

## 字段
- `doc_id`
- `page_id`
- `ocr_text`
- `ocr_confidence`
- `ocr_version`
- `process_time`
- `source_request_id`

## 备注
- `ocr_text` 和 `ocr_confidence` 是页面级字段。
- 当前 demo 中 OCR 逻辑为 mock 实现。
