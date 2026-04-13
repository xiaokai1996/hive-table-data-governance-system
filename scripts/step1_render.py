import os
import pandas as pd
import pypdfium2 as pdfium
from datetime import datetime

# 定义输入输出目录
INPUT_DIR = "data/input"
IMG_DIR = "data/images"
OUTPUT_DIR = "data/dwd_page_render"

# 确保输出目录存在
os.makedirs(IMG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 用于存储每页渲染结果的元数据
rows = []

# 遍历输入目录下的所有 PDF 文件
for file in os.listdir(INPUT_DIR):
    if not file.endswith(".pdf"):
        continue

    pdf_path = os.path.join(INPUT_DIR, file)
    doc_id = file.replace(".pdf", "") # 使用文件名作为文档 ID

    # 加载 PDF 文档
    pdf = pdfium.PdfDocument(pdf_path)

    # 逐页渲染 PDF 为图像
    for i in range(len(pdf)):
        page = pdf[i]
        # 渲染页面，scale=2 表示 2 倍分辨率，提高 OCR 识别率
        pil_img = page.render(scale=2).to_pil()

        page_no = i + 1
        page_id = f"{doc_id}_{page_no}"

        # 保存渲染后的图像文件
        img_path = f"{IMG_DIR}/{page_id}.png"
        pil_img.save(img_path)

        # 收集页面级的元数据
        rows.append({
            "doc_id": doc_id,
            "page_id": page_id,
            "page_no": page_no,
            "image_uri": img_path,
            "render_version": "v1",
            "process_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

# 将所有页面元数据转换为 DataFrame
df = pd.DataFrame(rows)

# 将结果保存为 Parquet 格式，以便后续 OCR 步骤读取
df.to_parquet(f"{OUTPUT_DIR}/data.parquet", index=False)

print("✅ render 完成:", len(df), "pages")