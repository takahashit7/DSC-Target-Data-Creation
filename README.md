# DSC Target Data Creation

## 📌 Project Overview
This project provides workflows to extract prior year actual data from **Division Scorecard (DSC)** reports in MIRAI for Finance department target data submission.

### Purpose
- Extract prior year actual data by **Customer × Category** granularity
- Format data using **next fiscal year customer information (FN2)**
- Prepare baseline data for target data submission to Finance department

---

## 🗂 File Structure

```
DSC-Target-Data-Creation/
├── README.md                            # This file
└── SQL for DSC Target Data.ipynb        # SQL notebook for data extraction
```

---

## 📊 Data Extraction Details

### Target Period
- **FY2425 2H** (Second Half) actual data

### Extracted Dimensions
- **FN Code / FN Name** (FN2: Next fiscal year finance number)
- **DC/WS** (jp_cust_parent_group_name)
- **Category** (Category name)
- **PG Quarter** (Fiscal quarter)
- **Org Code, Org2-5** (Organization hierarchy)

### Extracted Metrics
- **Shipment GIV Value** (Shipment GIV)
- **Indirect GIV Shipments** (Indirect GIV shipments)
- **Ships in GIV Customer Sales** (Total customer sales GIV)
- **GARP Value** (GARP value)
- **GIV Outbound** (Outbound GIV)

---

## 🔧 Usage

### 1. Open the Notebook in Databricks
Open `SQL for DSC Target Data.ipynb` in MIRAI's Databricks environment.

### 2. Execute SQL Queries
The notebook contains 3 main SQL queries:

#### ① Basic Query (Cell 1)
- Simple extraction example from Shipment Fact
- Joins with Calendar and Product dimensions
- **Note**: Customer JOIN constraints prevent FN information retrieval

#### ② FY2425 2H Full Query (Cell 3)
- **Recommended**: Main query for production use
- Applies FN2 (next fiscal year customer information)
- Automatically determines RT (Retail) vs WS (Wholesale)
- Retrieves all metrics

#### ③ Diagnostic Query (Cell 4)
- For data quality checks
- Validates row counts for each CTE
- Checks key consistency with giv_dimension_dim_vw

### 3. Data Export
Export the extracted data as CSV or Excel and submit to the Finance department.

---

## 📝 Important Notes

### Customer Information Retrieval Method
- Prioritize **FN2 (next fiscal year FN)**
- For RT (Retail): `customer_japan_dim_vw.jp_local_fn_code_2`
- For WS (Wholesale): `warehouse_dim_vw.jp_local_fn_code_2`
- If FN2 is NULL, fallback to current FN

### Performance Optimization
- Pre-filtering by date range implemented
- Daily aggregation performed before Quarter aggregation (prevents row explosion)
- Broadcast hints used where applicable

### Data Sources
Primary tables used:
- `hive_metastore.japan_gold.shipments_fct_vw`
- `hive_metastore.japan_gold.calender_dim_vw`
- `hive_metastore.japan_gold.giv_dimension_dim_vw`
- `hive_metastore.japan_gold.customer_japan_dim_vw`
- `hive_metastore.japan_gold.warehouse_dim_vw`
- `hive_metastore.japan_linkedexcel.indirect_ship_day_fct_vw`

---

## 📞 Contact
For questions about data content or extraction methods, please contact the project owner.

---

## 📅 Update History
- **Created**: February 2026
- **Target Fiscal Year**: FY2425
- **Author**: Takahashi T7
