"""
增强版农药数据探索脚本
自动检测各种可能的文件格式和名称
"""

import os
import pandas as pd
from pathlib import Path
import json

def find_data_files():
    """查找所有可能的数据文件"""
    data_dir = Path("data")
    
    if not data_dir.exists():
        print("❌ data/ 目录不存在！")
        print("请确保数据文件在 pesticide-analysis/data/ 目录下")
        return []
    
    # 查找所有文件
    all_files = list(data_dir.glob("*"))
    print(f"📁 在 data/ 目录中找到 {len(all_files)} 个文件:")
    
    for file in all_files:
        size_kb = file.stat().st_size / 1024
        print(f"  - {file.name} ({size_kb:.1f} KB)")
    
    return all_files

def try_read_file(file_path):
    """尝试用多种方法读取文件"""
    print(f"\n🔄 尝试读取: {file_path.name}")
    
    # 方法1：尝试读取Parquet文件
    try:
        df = pd.read_parquet(file_path)
        print(f"  ✅ 成功读取为 Parquet 文件")
        return df, "parquet"
    except Exception as e1:
        print(f"  ❌ 不是标准Parquet文件: {e1}")
    
    # 方法2：尝试读取CSV文件
    try:
        df = pd.read_csv(file_path)
        print(f"  ✅ 成功读取为 CSV 文件")
        return df, "csv"
    except Exception as e2:
        print(f"  ❌ 不是CSV文件: {e2}")
    
    # 方法3：尝试读取Excel文件
    try:
        df = pd.read_excel(file_path)
        print(f"  ✅ 成功读取为 Excel 文件")
        return df, "excel"
    except Exception as e3:
        print(f"  ❌ 不是Excel文件: {e3}")
    
    # 方法4：如果是gzip压缩的Parquet文件
    if str(file_path).endswith('.gzip'):
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            print(f"  ✅ 成功读取为 gzip压缩的Parquet文件")
            return df, "parquet.gzip"
        except Exception as e4:
            print(f"  ❌ 读取gzip压缩文件失败: {e4}")
    
    return None, None

def analyze_dataframe(df, file_format):
    """分析数据框内容"""
    print("\n" + "="*60)
    print("📊 数据详细分析")
    print("="*60)
    
    print(f"文件格式: {file_format}")
    print(f"数据形状: {df.shape[0]} 行 × {df.shape[1]} 列")
    
    print("\n📋 所有列名:")
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)
        non_null = df[col].count()
        print(f"  {i:2d}. {col:<30} {dtype:<15} 非空值: {non_null}/{df.shape[0]}")
    
    print("\n👀 前3行数据预览:")
    print(df.head(3).to_string())
    
    # 查找文本列
    print("\n🔍 寻找文本列:")
    text_columns = []
    for col in df.columns:
        if df[col].dtype == 'object':
            samples = df[col].dropna().head(2)
            if len(samples) > 0:
                text_columns.append(col)
                print(f"\n  📄 列 '{col}':")
                for j, sample in enumerate(samples, 1):
                    sample_str = str(sample)
                    preview = sample_str[:100] + "..." if len(sample_str) > 100 else sample_str
                    print(f"     样本{j}: {preview}")
    
    return text_columns

def save_report(df, text_columns, file_path, file_format):
    """保存分析报告"""
    report = {
        "file_name": str(file_path.name),
        "file_format": file_format,
        "data_shape": {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1])
        },
        "columns": list(df.columns),
        "dtypes": {col: str(df[col].dtype) for col in df.columns},
        "text_columns": text_columns,
        "sample_data": {}
    }
    
    # 为每个文本列保存一些样本
    for col in text_columns[:5]:  # 只取前5个文本列
        samples = df[col].dropna().head(3).tolist()
        report["sample_data"][col] = samples
    
    # 保存为JSON
    with open("data_analysis_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # 保存为Markdown
    with open("data_analysis_report.md", "w", encoding="utf-8") as f:
        f.write(f"# 农药数据分析报告\n\n")
        f.write(f"## 文件信息\n")
        f.write(f"- 文件名: `{file_path.name}`\n")
        f.write(f"- 格式: {file_format}\n")
        f.write(f"- 数据规模: {df.shape[0]:,} 行 × {df.shape[1]} 列\n\n")
        
        f.write(f"## 数据列总览\n")
        f.write(f"共 {df.shape[1]} 列:\n\n")
        for i, col in enumerate(df.columns, 1):
            dtype = str(df[col].dtype)
            non_null = df[col].count()
            f.write(f"{i}. **{col}** - 类型: `{dtype}`, 非空值: {non_null}\n")
        
        f.write(f"\n## 文本列详情\n")
        f.write(f"找到 {len(text_columns)} 个文本列:\n\n")
        for col in text_columns:
            f.write(f"### {col}\n")
            samples = df[col].dropna().head(2).tolist()
            for j, sample in enumerate(samples, 1):
                f.write(f"样本{j}: `{str(sample)[:200]}`\n\n")
    
    print(f"\n📄 报告已保存:")
    print(f"  - data_analysis_report.json (机器可读)")
    print(f"  - data_analysis_report.md (人可读)")

def main():
    print("🔍 增强版农药数据探索")
    print("="*60)
    
    # 1. 查找所有数据文件
    files = find_data_files()
    
    if not files:
        print("\n💡 建议操作:")
        print("1. 确保数据文件已下载到 Mac")
        print("2. 在终端运行: mkdir -p ~/Desktop/pesticide-analysis/data")
        print("3. 将数据文件拖放到 data/ 文件夹中")
        print("4. 重新运行本脚本")
        return
    
    # 2. 尝试读取每个文件
    for file_path in files:
        df, file_format = try_read_file(file_path)
        
        if df is not None:
            print(f"\n✅ 成功读取文件: {file_path.name}")
            
            # 3. 分析数据
            text_columns = analyze_dataframe(df, file_format)
            
            # 4. 保存报告
            save_report(df, text_columns, file_path, file_format)
            
            # 5. 保存样本数据为CSV
            sample_file = "pesticide_data_sample.csv"
            df.head(100).to_csv(sample_file, index=False, encoding='utf-8')
            print(f"\n💾 样本数据已保存到: {sample_file} (前100行)")
            
            print("\n" + "="*60)
            print("🎯 下一步建议:")
            print("1. 查看 data_analysis_report.md 了解数据结构")
            print("2. 查看 pesticide_data_sample.csv 查看具体数据")
            print("3. 根据找到的文本列设计DeepSeek API提问")
            break
    else:
        print("\n❌ 无法读取任何文件")
        print("\n💡 可能原因:")
        print("1. 文件已损坏 - 请重新下载")
        print("2. 需要特殊解码 - 请联系教授确认文件格式")
        print("3. 需要安装额外库:")
        print("   运行: pip install pandas pyarrow fastparquet openpyxl")

if __name__ == "__main__":
    main()
    
    
    
    