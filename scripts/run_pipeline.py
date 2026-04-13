import subprocess
import os
import sys

# 获取 scripts 目录路径
SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))

def run_script(script_name):
    """运行指定的 Python 脚本"""
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    print(f"🚀 开始运行: {script_name}...")
    
    try:
        # 使用当前解释器运行脚本
        result = subprocess.run([sys.executable, script_path], check=True, capture_output=True, text=True)
        print(result.stdout)
        print(f"✅ {script_name} 运行成功！")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_name} 运行出错！")
        print(e.stderr)
        return False

def main():
    # 定义需要按顺序运行的脚本列表
    pipeline = [
        "step1_render.py",
        "step2_ocr.py",
        "step3_aggregate.py",
        "step4_validation.py"
    ]
    
    print("--- 📋 开始执行项目流水线 ---")
    
    for script in pipeline:
        if not run_script(script):
            print(f"🛑 流水线在 {script} 处中断。")
            sys.exit(1)
            
    print("--- 🎉 所有步骤执行完毕！ ---")

if __name__ == "__main__":
    main()
