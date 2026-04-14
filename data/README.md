# data 目录说明

`data` 目录按存储方式拆分为两类：

- `object_storage/`：模拟对象存储，保存原始 PDF 和拆页渲染图片
- `hive_warehouse/`：模拟 Hive 数仓，保存各层 parquet 表

这样可以避免把“文件对象”和“表数据”混放在同一级目录下。
