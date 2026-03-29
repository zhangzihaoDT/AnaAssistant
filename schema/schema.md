# 数据集指标与维度定义

本文档定义了数据集的业务逻辑、指标计算规则及可用维度，用于指导 Planning Agent 生成准确的分析计划。

## 0. 澄清规则 (Clarifications)

在生成规划 DSL 前，如果用户问题存在口径歧义，必须先澄清，确认后再生成 plans。

### 0.1 口语“销量”澄清

用户如果问“销量/卖了多少/成交量”但未明确口径，必须先澄清后再规划：

- 澄清选项仅限：**锁单量**（lock_time） / **交付数**（delivery_date） / **开票数**（invoice_upload_time）
- 不允许默认选择其中一个口径
- 澄清后再生成对应 metric 与时间字段，并补齐对应“时间字段非空”过滤条件

### 0.2 城市口径澄清

用户如果在问题里提到“南京/南京市”等城市，但未明确是按门店口径还是上牌口径，必须先澄清后再规划：

- 门店城市：`store_city`
- 上牌城市：`license_city`

澄清后再生成对应 filters。若数据中存在“南京/南京市”这类值别名差异，filters 建议使用 `in` 操作符携带多值以保证命中。

**负样本（不要误判为城市）**

- 句首动词/意图词不是城市：如“查询/统计/汇总/查看/分析/对比”，不要因为后面跟着“去年/本月/昨天”等时间词就把它当作城市。
  - 例如：“查询去年的下发线索数,试驾数,锁单数”中的“查询”不是城市，不应触发城市口径澄清。
  - 例如：“统计今年锁单量”中的“统计”不是城市，不应触发城市口径澄清。

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

- **锁单量**: `order_number` 计数 (必须添加过滤条件: `lock_time` 非空)。注意：时间筛选应基于 `lock_time`。
- **交付数**: `order_number` 计数 (必须添加过滤条件: `delivery_date` 非空)。注意：时间筛选应基于 `delivery_date`。
- **开票数**: `order_number` 计数 (必须添加过滤条件: `invoice_upload_time` 非空)。注意：时间筛选应基于 `invoice_upload_time`，而不是 `order_create_date`。
- **小订数**: `order_number` 计数 (必须添加过滤条件: `intention_payment_time` 非空)。注意：时间筛选应基于 `intention_payment_time`。
- **开票金额**: `invoice_amount` (求和/平均)
- **订单计数**: `order_number` 计数
- **在营门店数**: 以目标日 `d` 统计，口径为“最近 30 天内有活动且在 `d` 当天已开店的门店数”。
  - 该指标由算子层统一计算：`operators/active_store.py`，优先走固定算子而不是通用 DSL 聚合。
  - 活动日字段优先取 `order_create_date`，缺失时回退 `order_create_time`（按天截断）。
  - 仅保留 `store_name` 与活动日非空记录。
  - 每个门店开店日取 `store_create_date` 的最小值。
  - 活跃门店集合为活动日落在 `[d-29, d]` 的门店。
  - 在营判定为 `open_date <= d`，最终结果为门店 `store_name` 去重计数。
  - 不要把 `store_create_date` 直接当作统计时间字段做简单 count。

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
- `product_type`: 燃料类型 / 动力形式。**注意：数据集中无此字段，需通过 product_name 模糊匹配生成。**
  - **增程**: `product_name` 包含 "52" 或 "66"。请使用正则匹配: `filters: [{"field": "product_name", "op": "matches", "value": "52|66"}]`。
  - **纯电**: `product_name` **不**包含 "52" 且 **不**包含 "66"。请使用正则匹配: `filters: [{"field": "product_name", "op": "not matches", "value": "52|66"}]`。
  - **Planning Agent 请注意**: 对于“增程”或“纯电”查询，必须使用 `matches` 或 `not matches` 操作符，并使用正则 `52|66`。不要生成多个 `contains` 过滤器（因为它们是 AND 关系）。

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

### order_full_data.parquet (Total Rows: 420697)

| Column Name               | Data Type      | Description           |
| :------------------------ | :------------- | :-------------------- |
| series                    | str            | 车型系列              |
| final_payment_way         | category       | 尾款支付方式          |
| intention_refund_time     | datetime64[ns] | 意向金退款时间        |
| age                       | float64        | 年龄                  |
| delivery_date             | datetime64[ns] | 交付日期              |
| invoice_upload_time       | datetime64[ns] | 发票上传时间          |
| finance_product           | str            | 金融产品              |
| first_assign_time         | str            | 首次下发时间          |
| first_test_drive_time     | datetime64[ns] | 首次试驾时间          |
| first_middle_channel_name | str            | 首次中间渠道名称      |
| lock_time                 | datetime64[ns] | 锁单时间              |
| drive_series_cn           | category       | 驱动系列（中文）      |
| license_city_level        | category       | 上牌城市等级          |
| belong_intent_series      | str            | 意向系列              |
| order_create_time         | datetime64[ns] | 订单创建时间          |
| gender                    | category       | 性别                  |
| owner_cell_phone          | string         | 车主手机号            |
| approve_refund_time       | datetime64[ns] | 审批退款时间          |
| product_name              | str            | 产品名称              |
| invoice_amount            | float64        | 开票金额              |
| store_city                | str            | 门店城市              |
| store_create_date         | datetime64[ns] | 门店创建日期          |
| store_name                | str            | 门店名称              |
| order_create_date         | datetime64[ns] | 订单创建日期          |
| order_number              | string         | 订单号                |
| lead_assign_time_max      | str            | 线索最大下发时间      |
| first_touch_time          | datetime64[ns] | 首次接触时间          |
| order_type                | str            | 订单类型              |
| deposit_refund_time       | datetime64[ns] | 大定退款时间          |
| parent_region_name        | category       | 大区名称              |
| td_countd                 | float64        | 试驾次数              |
| main_lead_id              | str            | 关联试驾表的主线索 ID |
| license_province          | str            | 上牌省份              |
| apply_refund_time         | datetime64[ns] | 申请退款时间          |
| license_city              | str            | 上牌城市              |
| is_hold                   | category       | 是否保留              |
| intention_payment_time    | datetime64[ns] | 意向金支付时间        |
| is_staff                  | category       | 是否员工              |
| deposit_payment_time      | datetime64[ns] | 大定支付时间          |

### assign_data.csv (Total Rows: 1184)

| Column Name                      | Data Type | Description                         |
| :------------------------------- | :-------- | :---------------------------------- |
| Assign Time 年/月/日             | str       | 下发时间                            |
| 下发线索 30 日锁单数 (APP小程序) | int64     | 下发线索30日内锁单数量（APP小程序） |
| 下发线索 30 日锁单数 (平台)      | int64     | 下发线索30日内锁单数量（平台）      |
| 下发线索 30 日锁单数 (快慢闪)    | int64     | 下发线索30日内锁单数量（快慢闪）    |
| 下发线索 30 日锁单数 (直播)      | int64     | 下发线索30日内锁单数量（直播）      |
| 下发线索 30 日锁单数 (门店)      | int64     | 下发线索30日内锁单数量（门店）      |
| 下发线索 30 日锁单数             | int64     | 下发线索30日内锁单数量（合计）      |
| 下发线索 30日试驾数              | int64     | 下发线索30日内试驾数量              |
| 下发线索 7 日试驾数              | int64     | 下发线索7日内试驾数量               |
| 下发线索 7 日锁单数 (平台)       | int64     | 下发线索7日内锁单数量（平台）       |
| 下发线索 7 日锁单数 (直播)       | int64     | 下发线索7日内锁单数量（直播）       |
| 下发线索 7 日锁单数 (门店)       | int64     | 下发线索7日内锁单数量（门店）       |
| 下发线索 7 日锁单数              | int64     | 下发线索7日内锁单数量（合计）       |
| 下发线索当日试驾数               | int64     | 下发线索当日试驾数量                |
| 下发线索当日锁单数 (门店)        | int64     | 当日门店渠道线索当天即锁单的数量    |
| 下发线索数 (门店)                | int64     | 当日门店渠道收到的线索总数          |
| 下发线索数                       | int64     | 下发线索总数                        |
| 下发线索数（APP小程序)           | int64     | 下发线索总数（APP小程序）           |
| 下发线索数（平台)                | int64     | 下发线索总数（平台）                |
| 下发线索数（快慢闪)              | int64     | 下发线索总数（快慢闪）              |
| 下发线索数（直播）               | int64     | 下发线索总数（直播）                |
| 下发门店数                       | int64     | 下发门店数量                        |
| 主要渠道统计覆盖率               | float64   | 主要渠道统计覆盖率                  |
