---
name: order-sample-feature-compare
description: 订单抽样特征对比（中文别名：订单抽样特征对比 / 抽样对比 / 订单特征对比）。基于订单号清单或自然语言条件筛选订单（A/B），从 order_data.parquet 计算 series_group_logic（按 schema/business_definition.json 的 product_name LIKE 规则），并与 config_attribute.parquet 以 Order Number↔order_number 关联打通为宽表；输出 A 组画像或 A vs B 核心差异摘要（占比分布/均值差/SMD/缺失率）。适用于抽样复盘、门店/城市/配置结构差异定位、选配（如“地暖=是”）与锁单用户对比。
---

# order-sample-feature-compare

中文别名：订单抽样特征对比

用仓库自带数据源把订单筛选结果打通成特征宽表，并输出画像或 A vs B 差异。

## 快速使用

自然语言筛选（只做 A 组画像）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --a-nl "LS8 用户性别为男的订单特征" \
  --md-out out/LS8_男_画像.md
```

自然语言筛选（A/B 对比）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --a-nl "LS8 用户性别为男" \
  --b-nl "LS8 用户性别为女" \
  --md-out out/LS8_男_vs_女.md
```

自然语言筛选（选配条件，要求 Attribute + value）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --a-nl "LS8 Attribute 的 IM 智控地暖系统，value=是" \
  --md-out out/LS8_地暖_是_画像.md
```

自然语言筛选（选配条件 + 锁单条件）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --a-nl "LS8 Attribute 的 IM 智控地暖系统，value=是" \
  --b-nl "LS8 Attribute 的 IM 智控地暖系统，value=是 且锁单的用户" \
  --md-out out/LS8_地暖_是_锁单对比.md
```

对比输出中剔除某些列（例如剔除筛选条件本身对应的列）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --a-nl "LS8 Attribute 的 IM 智控地暖系统，value=是 且锁单的用户" \
  --b-nl "LS8 锁单的用户" \
  --exclude-features "IM 智控地暖系统" \
  --md-out out/LS8_地暖锁单_vs_LS8整体锁单.md
```

直接输入两组订单号（逗号/空格分隔）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --orders-a-list 1001,1002,1003 \
  --orders-b-list 2001,2002,2003 \
  --wide-out out/order_wide.parquet \
  --md-out out/order_group_diff.md
```

订单号来自文件（csv/xlsx/txt/json）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --orders-a path/to/orders_a.xlsx \
  --orders-b path/to/orders_b.csv \
  --wide-out out/order_wide.csv \
  --md-out out/order_group_diff.md
```

不加载选配表（只对比订单表字段）：

```bash
python3 scripts/order_sample_feature_compare.py \
  --orders-a-list 1001,1002 \
  --orders-b-list 2001,2002 \
  --no-config-attribute
```

## 输入约定

- A/B 订单清单文件支持：txt / csv / xlsx / json
- 识别订单号列优先级：order_number / Order Number / 订单号
- json 支持两种格式：
  - list[str]
  - {"order_numbers": list[str]}

## 口径

- series_group_logic：按 schema/business_definition.json 的 series_group_logic 逐条规则匹配，默认“其他”。
- 选配信息：从 config_attribute.parquet 读取 Order Number/Attribute/value/is_staff，按（订单号, 选配项）去重后 pivot 成宽列；is_staff 汇总为每单布尔值。
- 差异摘要：
  - 类别特征：按 A/B 占比差（A-B）排序
  - 数值特征：展示均值差与 SMD（标准化均值差），同时展示缺失率

## 自然语言解析约定

- 车系识别：支持 LS8/LS9/LS7/L7/CM0/CM1/CM2/DM0/DM1（用于过滤 series_group_logic）
- 性别默认字段：默认按车主性别 owner_gender（可用 --gender-default=order 切到购车人 order_gender）；文本包含“购车人/车主”会覆盖默认
- 选配条件：识别 “Attribute/选配/配置 + value/取值”，按包含匹配（contains）过滤（匹配时忽略空格）
- 锁单条件：识别“锁单/已锁单/锁单的用户”过滤 lock_time 非空；识别“未锁单/未锁/没锁单”过滤 lock_time 为空

