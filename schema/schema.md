# 数据集指标与维度定义

本文档定义了数据集的业务逻辑、指标计算规则及可用维度，用于指导 Planning Agent 生成准确的分析计划。

## 1. 时间维度 (Time Dimensions)

用于按时间段（日、周、月、年）进行趋势分析和筛选。

- `order_create_time`: 订单创建时间
- `order_create_date`: 订单创建日期
- `store_create_date`: 门店创建日期
- `lock_time`: 锁单时间
- `invoice_upload_time`: 发票上传时间
- `delivery_date`: 交付日期
- `intention_payment_time`: 意向金支付时间
- `intention_refund_time`: 意向金退款时间
- `deposit_payment_time`: 大定支付时间
- `deposit_refund_time`: 大定退款时间
- `apply_refund_time`: 申请退款时间
- `approve_refund_time`: 审批退款时间
- `first_touch_time`: 首次接触时间
- `first_test_drive_time`: 首次试驾时间
- `lead_assign_time_max`: 线索最大下发时间
- `first_assign_time`: 首次下发时间
- `Assign Time 年/月/日`: 外部线索下发日期 (仅限 assign_data)

## 2. 可用指标 (Metrics)

用于计算总和、平均值、计数等数值指标。

### 核心业务指标

- **锁单量**: `order_number` 计数 (条件: `lock_time` 非空)
- **交付数**: `order_number` 计数 (条件: `delivery_date` 非空)
- **开票数**: `order_number` 计数 (条件: `invoice_upload_time` 非空)
- **小订数**: `order_number` 计数 (条件: `intention_payment_time` 非空)
- **开票金额**: `invoice_amount` (求和/平均)
- **订单计数**: `order_number` 计数

### 客户与行为指标

- **年龄**: `age` (平均/分布)
- **试驾次数**: `td_countd` (求和/平均)

### 外部线索指标 (仅限 assign_data)

- `下发线索数`: 下发线索总数
- `下发线索当日试驾数`: 下发当日完成试驾的数量
- `下发线索 7 日试驾数`: 下发 7 日内完成试驾的数量
- `下发线索 7 日锁单数`: 下发 7 日内完成锁单的数量
- `下发线索 30日试驾数`: 下发 30 日内完成试驾的数量
- `下发线索 30 日锁单数`: 下发 30 日内完成锁单的数量
- `下发门店数`: 接收线索的门店总数
- `下发线索数 (门店)`: 门店渠道收到的线索总数
- `下发线索当日锁单数 (门店)`: 门店渠道线索当天即锁单的数量

## 3. 可用维度 (Dimensions)

用于分组、筛选和拆解分析。

### 产品与车型

- `product_name`: 产品名称 (如: 全新智己L6)
- `series`: 车型系列 (如: L6, LS6)
- `belong_intent_series`: 意向系列
- `drive_series_cn`: 驱动系列中文名
- `product_type`: 燃料类型 / 动力形式 (对应 query 中的 "燃料"、"动力"、"燃油")

### 地理位置

- `store_city`: 门店城市
- `store_name`: 门店名称
- `parent_region_name`: 大区名称
- `license_province`: 上牌省份
- `license_city`: 上牌城市
- `license_city_level`: 上牌城市等级

### 渠道与客户

- `first_middle_channel_name`: 首次中间渠道名称
- `gender`: 性别
- `is_staff`: 是否员工 (Y/N)
- `is_hold`: 是否保留 (Y/N)

### 其他

- `order_type`: 订单类型
- `finance_product`: 金融产品
- `final_payment_way`: 尾款支付方式
- `main_lead_id`: 关联试驾表的主线索 ID

---

## 附录：原始字段 Schema 映射

### order_full_data.parquet (Total Rows: 420373)

| Column Name               | Data Type      | Description           |
| :------------------------ | :------------- | :-------------------- |
| store_create_date         | datetime64[ns] | 门店创建日期          |
| age                       | float64        | 年龄                  |
| invoice_upload_time       | datetime64[ns] | 发票上传时间          |
| belong_intent_series      | str            | 意向系列              |
| drive_series_cn           | category       | 驱动系列（中文）      |
| order_type                | str            | 订单类型              |
| order_create_time         | datetime64[ns] | 订单创建时间          |
| order_create_date         | datetime64[ns] | 订单创建日期          |
| lead_assign_time_max      | str            | 线索最大下发时间      |
| order_number              | string         | 订单号                |
| store_name                | str            | 门店名称              |
| deposit_payment_time      | datetime64[ns] | 大定支付时间          |
| intention_payment_time    | datetime64[ns] | 意向金支付时间        |
| is_staff                  | category       | 是否员工              |
| first_test_drive_time     | datetime64[ns] | 首次试驾时间          |
| deposit_refund_time       | datetime64[ns] | 大定退款时间          |
| delivery_date             | datetime64[ns] | 交付日期              |
| intention_refund_time     | datetime64[ns] | 意向金退款时间        |
| finance_product           | str            | 金融产品              |
| lock_time                 | datetime64[ns] | 锁单时间              |
| series                    | str            | 车型系列              |
| license_city              | str            | 上牌城市              |
| parent_region_name        | category       | 大区名称              |
| product_name              | str            | 产品名称              |
| store_city                | str            | 门店城市              |
| first_touch_time          | datetime64[ns] | 首次接触时间          |
| first_middle_channel_name | str            | 首次中间渠道名称      |
| is_hold                   | category       | 是否保留              |
| gender                    | category       | 性别                  |
| td_countd                 | float64        | 试驾次数              |
| main_lead_id              | str            | 关联试驾表的主线索 ID |
| final_payment_way         | category       | 尾款支付方式          |
| license_city_level        | category       | 上牌城市等级          |
| license_province          | str            | 上牌省份              |
| invoice_amount            | float64        | 开票金额              |
| apply_refund_time         | datetime64[ns] | 申请退款时间          |
| first_assign_time         | str            | 首次下发时间          |
| approve_refund_time       | datetime64[ns] | 审批退款时间          |

### assign_data.csv (Total Rows: 1164)

| Column Name               | Data Type | Description                      |
| :------------------------ | :-------- | :------------------------------- |
| Assign Time 年/月/日      | str       | 下发时间                         |
| 下发线索 30 日锁单数      | int64     | 下发线索30日内锁单数量           |
| 下发线索 30日试驾数       | int64     | 下发线索30日内试驾数量           |
| 下发线索 7 日试驾数       | int64     | 下发线索7日内试驾数量            |
| 下发线索 7 日锁单数       | int64     | 下发线索7日内锁单数量            |
| 下发线索当日试驾数        | int64     | 下发线索当日试驾数量             |
| 下发线索当日锁单数 (门店) | int64     | 当日门店渠道线索当天即锁单的数量 |
| 下发线索数 (门店)         | int64     | 当日门店渠道收到的线索总数       |
| 下发线索数                | int64     | 下发线索总数                     |
| 下发门店数                | int64     | 下发门店数量                     |
