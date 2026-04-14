# hive_warehouse 目录说明

该目录用于模拟 Hive 数仓表存储。

- `ods_*`：原始接入层
- `dim_*`：维度层
- `dwd_*`：明细事实处理层
- `ads_*`：对外交付层

每个表目录下：

- `data.parquet`：表数据
- `data.parquet.md`：单表说明文档

说明：

- 下文 sample 基于当前仓库中的真实 parquet 数据摘取并适当截断
- 部分历史样例中的 `pdf_url`、`image_uri` 仍保留旧路径字符串，这是目录迁移前写入的数据，不影响表结构和流转逻辑理解

## 总体流转顺序

当前 OCR pipeline 的表流转顺序如下：

1. 业务方提交一个 request，请求中包含若干 PDF
2. `ods_pdf_request_item` 记录本次 request 的原始输入快照
3. `dim_pdf_document` 维护文档主数据，`dwd_doc_parse_status` 维护文档解析状态
4. `dwd_request_doc_task` 根据是否已有历史结果，标记 `parse_needed=true/false`
5. 对需要新解析的文档进入页面处理链路：
   `dwd_page_render -> dwd_page_ocr -> dwd_doc_aggregate`
6. 对不需要新解析的文档，直接复用 `dwd_doc_aggregate` 中的历史结果
7. `ads_request_delivery_item` 按 request 输出全量交付事实
8. `ads_request_delivery_status` 记录交付状态

可以简化理解为：

```text
对象存储中的 raw_pdfs
    -> ods_pdf_request_item
    -> dim_pdf_document
    -> dwd_doc_parse_status
    -> dwd_request_doc_task
    -> dwd_page_render
    -> dwd_page_ocr
    -> dwd_doc_aggregate
    -> ads_request_delivery_item
    -> ads_request_delivery_status
```

## 流转过程详解

### 1. ODS 接入层

#### `ods_pdf_request_item`

作用：

- 保存 request 原始输入快照
- 让后续所有计算都能按 `request_id` 回溯

关键动作：

- 从 `data/object_storage/raw_pdfs` 扫描 PDF
- 读取 PDF bytes，计算 MD5，生成 `doc_id`
- 把 request 中的每个 PDF 落成一行

sample：

```json
{
  "request_id": "req_md5_001",
  "owner": "team_md5_a",
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/input/2507.pdf",
  "doc_id": "b3447a9a2a2eacf751501b0c3238ee91",
  "file_name": "2507.pdf",
  "submit_time": "2026-04-13 14:21:49"
}
```

说明：

- 一条记录表示“某个 request 提交了某个 PDF”
- 此时还没有 `page_cnt`、`doc_text` 等解析结果字段

### 2. DIM 与状态层

#### `dim_pdf_document`

作用：

- 维护去重后的文档主数据
- 只保留文档事实，不混入状态和解析结果

sample：

```json
{
  "doc_id": "67570b4a86169457633a8c880a460ffd",
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/object_storage/raw_pdfs/2602.pdf",
  "file_name": "2602.pdf",
  "file_size_bytes": 1517596,
  "first_seen_time": "2026-04-13 14:21:49"
}
```

说明：

- 一条记录表示“系统认知中的一个逻辑文档”
- `doc_id` 是全局文档主键

#### `dwd_doc_parse_status`

作用：

- 维护文档是否已经完成解析
- 将状态信息从 `dim_pdf_document` 中拆出来

sample：

```json
{
  "doc_id": "67570b4a86169457633a8c880a460ffd",
  "parse_status": "parsed",
  "parsed_time": "2026-04-13 14:21:55",
  "last_request_id": "req_layout_001",
  "last_seen_time": "2026-04-14 00:45:26",
  "last_update_time": "2026-04-14 00:45:26"
}
```

说明：

- `parse_status=parsed` 表示文档已有可复用解析结果
- 新 request 到来时，会优先参考该表和聚合结果表做增量判断

### 3. request 任务拆解层

#### `dwd_request_doc_task`

作用：

- 将 request 输入转换为可执行任务
- 明确哪些文档需要新解析，哪些文档可以直接复用

sample：

```json
{
  "request_id": "req_md5_001",
  "owner": "team_md5_a",
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/input/2602.pdf",
  "doc_id": "67570b4a86169457633a8c880a460ffd",
  "file_name": "2602.pdf",
  "submit_time": "2026-04-13 14:21:49",
  "parse_needed": true
}
```

说明：

- `parse_needed=true`：进入 render/OCR/aggregate 主链
- `parse_needed=false`：直接复用已有文档结果

### 4. 页面级处理链路

#### `dwd_page_render`

作用：

- 记录 PDF 拆页和页面渲染结果
- 每页形成一个 `page_id`

sample：

```json
{
  "doc_id": "b3447a9a2a2eacf751501b0c3238ee91",
  "page_id": "b3447a9a2a2eacf751501b0c3238ee91_1",
  "page_no": 1,
  "image_uri": "data/images/b3447a9a2a2eacf751501b0c3238ee91_1.png",
  "render_version": "v2",
  "process_time": "2026-04-13 14:21:51",
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/input/2507.pdf",
  "source_request_id": "req_md5_001"
}
```

说明：

- 一条记录表示一个页面
- 当前仓库中部分历史数据的 `image_uri`、`pdf_url` 仍保留旧路径字符串，这是历史产物遗留，不影响当前逻辑理解

#### `dwd_page_ocr`

作用：

- 保存页面级 OCR 结果
- 每页输出一段 `ocr_text` 和一个置信度

sample：

```json
{
  "doc_id": "b3447a9a2a2eacf751501b0c3238ee91",
  "page_id": "b3447a9a2a2eacf751501b0c3238ee91_1",
  "ocr_text": "text from data/images/b3447a9a2a2eacf751501b0c3238ee91_1.png",
  "ocr_confidence": 0.945,
  "ocr_version": "v2",
  "process_time": "2026-04-13 14:21:55",
  "source_request_id": "req_md5_001"
}
```

说明：

- 一条记录表示一个页面的 OCR 输出
- 当前 demo 中 OCR 是 mock 逻辑，因此 `ocr_text` 是示意性文本

### 5. 文档级聚合层

#### `dwd_doc_aggregate`

作用：

- 把页面级 OCR 结果聚合成文档级结果
- 作为后续 request 复用历史解析结果的核心表

sample：

```json
{
  "doc_id": "41403ee743cd5c9ebc1683e163f6b7b1",
  "page_cnt": 21,
  "doc_text": "text from data/images/41403ee743cd5c9ebc1683e163f6b7b1_1.png\\ntext from data/images/41403ee743cd5c9ebc1683e163f6b7b1_2.png\\n...",
  "quality_score": 0.9512380952,
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/input/2603.13201.pdf",
  "aggregate_version": "v2",
  "process_time": "2026-04-13 14:21:55",
  "source_request_id": "req_md5_001"
}
```

说明：

- 一条记录表示一个完整文档的解析结果
- `page_cnt`、`doc_text`、`quality_score` 都是在这一层才真正生成
- 后续如果新 request 命中同一个 `doc_id`，就可以直接复用这里的结果

### 6. ADS 交付层

#### `ads_request_delivery_item`

作用：

- 记录按 request 对业务方交付的事实结果
- 同一个 request 下，既可能包含新解析文档，也可能包含缓存复用文档

sample：

```json
{
  "request_id": "req_md5_001",
  "owner": "team_md5_a",
  "doc_id": "b3447a9a2a2eacf751501b0c3238ee91",
  "pdf_url": "/Users/bytedance/codes/hive-table-data-governance-system/data/input/2507.pdf",
  "delivery_source": "newly_parsed",
  "page_cnt": 9,
  "doc_text": "text from data/images/b3447a9a2a2eacf751501b0c3238ee91_1.png\\ntext from data/images/b3447a9a2a2eacf751501b0c3238ee91_2.png\\n...",
  "quality_score": 0.9457777778,
  "delivery_time": "2026-04-13 14:21:56"
}
```

说明：

- 这是最终面向业务方的交付事实表
- `delivery_source` 用来标记是本次新解析还是历史缓存复用

#### `ads_request_delivery_status`

作用：

- 记录交付状态
- 与交付事实表分开存储，避免状态字段污染事实表

sample：

```json
{
  "request_id": "req_md5_001",
  "doc_id": "b3447a9a2a2eacf751501b0c3238ee91",
  "delivery_status": "delivered",
  "status_time": "2026-04-13 14:21:56"
}
```

说明：

- 一条记录表示某个 request 下某个文档的交付状态
- 当前 demo 中主要状态值为 `delivered`

## 一次完整流转示例

以 `2507.pdf` 为例，数据大致会经历如下过程：

1. 在 `ods_pdf_request_item` 中记录：
   - `request_id=req_md5_001`
   - `doc_id=b3447a9a2a2eacf751501b0c3238ee91`
2. 在 `dim_pdf_document` 中注册该文档主数据
3. 在 `dwd_request_doc_task` 中标记 `parse_needed=true`
4. 在 `dwd_page_render` 中拆成 9 页
5. 在 `dwd_page_ocr` 中产出 9 条页面 OCR 结果
6. 在 `dwd_doc_aggregate` 中聚合为 1 条文档结果，得到：
   - `page_cnt=9`
   - `quality_score=0.9457777778`
7. 在 `ads_request_delivery_item` 中生成最终交付记录
8. 在 `ads_request_delivery_status` 中标记 `delivery_status=delivered`

如果后续再来一个新的 request，且包含同一个 PDF：

1. 仍会先进入 `ods_pdf_request_item`
2. 在 `dwd_request_doc_task` 中会被标记为 `parse_needed=false`
3. 不再进入 `dwd_page_render`、`dwd_page_ocr`
4. 直接复用 `dwd_doc_aggregate` 中的历史结果
5. 最后仍然在 `ads_request_delivery_item` 中形成该 request 的一条完整交付记录

## 目录约定

- 本目录只放 Hive 表
- 原始 PDF 和页面图片不在这里，位于 `data/object_storage`
- 查看单张表的更精细说明时，可进入对应目录阅读 `data.parquet.md`
