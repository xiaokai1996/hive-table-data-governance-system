import os
import pandas as pd

def list_files(dir_path, ext=".pdf"):
    """列出目录下指定后缀的文件"""
    return [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith(ext)]

def save_parquet(df, path):
    """保存 DataFrame 为 Parquet 格式"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)

def read_parquet(path):
    """读取 Parquet 格式"""
    return pd.read_parquet(path)

def ensure_dir(dir_path):
    """确保目录存在"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
