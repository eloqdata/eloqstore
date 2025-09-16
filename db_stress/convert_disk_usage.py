#!/usr/bin/env python3
import re
import csv
import sys

def parse_disk_usage_log(input_file, output_file):
    """
    将磁盘使用日志转换为CSV格式
    输入格式: 本地磁盘GB/云端存储GB,本地磁盘GB/云端存储GB,...
    输出格式: CSV文件，包含序号、本地磁盘和云端存储三列
    """
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 使用正则表达式匹配所有的磁盘使用数据
    # 格式: 数字GB/数字GB
    pattern = r'(\d+\.\d+)GB/(\d+\.\d+)GB'
    matches = re.findall(pattern, content)
    
    if not matches:
        print("未找到匹配的磁盘使用数据")
        return
    
    # 写入CSV文件
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # 写入表头（添加序号列）
        writer.writerow(['序号', '本地磁盘(GB)', '云端存储(GB)'])
        
        # 写入数据（添加序号）
        for index, (local_disk, cloud_storage) in enumerate(matches, start=1):
            writer.writerow([index, local_disk, cloud_storage])
    
    print(f"成功转换 {len(matches)} 条记录到 {output_file}")

def main():
    if len(sys.argv) != 3:
        print("使用方法: python convert_disk_usage_to_csv.py <输入文件> <输出文件>")
        print("示例: python convert_disk_usage_to_csv.py disk_usage.log disk_usage.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    try:
        parse_disk_usage_log(input_file, output_file)
    except FileNotFoundError:
        print(f"错误: 找不到输入文件 {input_file}")
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    main()