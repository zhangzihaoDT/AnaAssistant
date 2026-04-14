"""
仅打印输出（不发送飞书）进行测试：
python LS8predict_to_feishu.py --dry-run
--
真实发送给飞书（需确保 .env 中的 FS_WEBHOOK_URL 已正确配置）：
python LS8predict_to_feishu.py

"""
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK_URL = os.getenv("FS_WEBHOOK_URL")

def main():
    parser = argparse.ArgumentParser(description="运行分析脚本并发送LS8推演指标到飞书")
    parser.add_argument("--dry-run", action="store_true", help="只打印不发送飞书")
    args = parser.parse_args()

    script_path = Path(__file__).resolve().parent / "analyze_retained_intention_orders.py"

    if not script_path.exists():
        print(f"❌ 找不到分析脚本: {script_path}")
        return 1

    print(f"正在运行分析脚本: {script_path.name} ...")
    result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ 分析脚本执行失败!")
        print(result.stderr)
        return 1

    output = result.stdout

    # 解析需要的字段
    base_top3 = re.search(r"当前已知前3日累计:\s*(\d+)", output)
    
    if not base_top3:
        print("❌ 无法从分析脚本输出中解析出基本推演数据。")
        return 1

    lines = output.split('\n')
    
    # 提取表1：LS8 最终留存小订数推演
    table1_start = -1
    for i, line in enumerate(lines):
        if "参考历史车型" in line and "推演末尾Day2" in line and "综合推演最终值" in line:
            table1_start = i
            break
            
    if table1_start == -1:
        print("❌ 无法找到第一个推演表。")
        return 1
        
    table1_data = {}
    for line in lines[table1_start+1:]:
        line = line.strip()
        if not line:
            break
        parts = line.split()
        if len(parts) >= 6:
            model = parts[0]
            inc_last3 = parts[-2]
            final_val = parts[-1]
            table1_data[model] = {"末尾3日增量": inc_last3, "最终值": final_val}

    # 提取表2：LS8 上市后30日锁单数推演
    table2_start = -1
    for i, line in enumerate(lines):
        if "参考历史车型" in line and "推演30日锁单" in line and "推演转化率" in line:
            table2_start = i
            break
            
    if table2_start == -1:
        print("❌ 无法找到第二个推演表。")
        return 1
        
    table2_data = {}
    for line in lines[table2_start+1:]:
        line = line.strip()
        if not line:
            break
        parts = line.split()
        if len(parts) >= 10:
            model = parts[0]
            lock_30d = parts[2]
            conv_rate = parts[3]
            table2_data[model] = {"推演30日锁单": lock_30d, "推演转化率": conv_rate}

    # 组合飞书卡片文本
    md_lines = []
    md_lines.append(f"--- LS8 最终留存小订数推演 (结合已知数据 + 动态推演) ---")
    md_lines.append(f"当前已知前3日累计: {base_top3.group(1)}")
    
    # 因为输出内容改变了，我们改用正则从原始 output 里直接抓取新的“中间期”和“末尾3日”摘要
    middle_match = re.search(r"中间期:\s*(.+)", output)
    if middle_match:
        md_lines.append(f"中间期: {middle_match.group(1)}")
        
    last3_match = re.search(r"末尾3日当前实际值\s*-\s*(.+)", output)
    if last3_match:
        md_lines.append(f"末尾3日实际值: {last3_match.group(1)}")

    md_lines.append("参考历史车型｜综合推演末尾3日增量 ｜综合推演最终值｜推演30日锁单｜推演转化率")
    
    for model in table1_data:
        if model in table2_data:
            d1 = table1_data[model]
            d2 = table2_data[model]
            md_lines.append(f"{model}｜{d1['末尾3日增量']} ｜{d1['最终值']}｜{d2['推演30日锁单']}｜{d2['推演转化率']}")

    body_md = "\n".join(md_lines)
    
    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": "📊 LS8 留存小订及锁单推演报告"},
                "template": "blue",
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body_md}},
                {
                    "tag": "note",
                    "elements": [{"tag": "plain_text", "content": f"统计时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}],
                },
            ],
        },
    }

    if args.dry_run or not WEBHOOK_URL:
        if not WEBHOOK_URL:
            print("⚠️ 未设置 FS_WEBHOOK_URL，跳过发送")
        print(json.dumps(card, ensure_ascii=False, indent=2))
        return 0

    try:
        resp = requests.post(WEBHOOK_URL, json=card, timeout=10)
        resp.raise_for_status()
        result_json = resp.json()
        code = result_json.get("StatusCode", result_json.get("code"))
        if code == 0:
            print("✅ 飞书消息发送成功")
            return 0
        print(f"❌ 飞书返回异常: {result_json}")
        return 1
    except Exception as e:
        print(f"❌ 发送飞书消息失败: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
