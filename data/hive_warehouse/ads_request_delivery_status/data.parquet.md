# ads_request_delivery_status

- 分层: `ADS`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录按 request 交付的状态信息。

## 粒度
一行表示某个 request 下某个文档的一条交付状态。

## 上下游
- 上游: 由 `scripts/step4_validation.py` 生成。
- 下游: 供业务方或运维侧跟踪交付状态。

## 字段
- `request_id`
- `doc_id`
- `delivery_status`
- `status_time`

## 备注
- 当前 demo 中状态值主要为 `delivered`。
- 该表只保留状态字段，不混入交付事实字段。
