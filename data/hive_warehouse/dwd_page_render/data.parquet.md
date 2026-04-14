# dwd_page_render

- 分层: `DWD`
- 状态: 启用中
- 文件: `data.parquet`

## 作用
记录 PDF 拆页和渲染后的页面级结果。

## 粒度
一行表示一个文档页面。

## 上下游
- 上游: 由 `scripts/step1_render.py` 生成。
- 下游: 供 `scripts/step2_ocr.py` 进行页面 OCR。

## 字段
- `doc_id`
- `page_id`
- `page_no`
- `image_uri`
- `render_version`
- `process_time`
- `pdf_url`
- `source_request_id`

## 备注
- 只会处理 `parse_needed=true` 的文档。
- `image_uri` 指向当前 demo 生成的页面图片文件。
