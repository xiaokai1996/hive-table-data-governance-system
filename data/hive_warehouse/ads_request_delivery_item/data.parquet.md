# ads_request_delivery_item

- 分层: `ADS`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录按 request 交付给业务方的事实结果。

## 粒度
一行表示某个 request 下某个文档的一条交付事实。

## 上下游
- 上游: 由 `scripts/step4_validation.py` 生成。
- 下游: 供业务方查看交付内容和结果明细。

## 字段
- `request_id`
- `owner`
- `doc_id`
- `pdf_url`
- `delivery_source`
- `page_cnt`
- `doc_text`
- `quality_score`
- `delivery_time`

## 备注
- `delivery_source` 用于区分结果来自缓存复用还是本次新解析。
- 该表不再存储 `delivery_status`，状态量已拆到独立状态表。
