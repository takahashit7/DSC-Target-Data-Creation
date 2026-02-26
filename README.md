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
└── SQL for DSC Target Data.ipynb        # Parameterized SQL notebook for data extraction
```

---

## 📊 Data Extraction Details

### Target Period
- **Parameterized**: Specify any Fiscal Year and Half (default: FY2425 2H)
- Flexible period selection via Databricks widgets

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

### 2. Set Parameters (Cell 1)
Configure the extraction period using Databricks widgets:
- **fy_year_name**: Fiscal year (e.g., "FY2425")
- **fy_half_year_name**: Half year period (e.g., "2H" or "1H")

```python
dbutils.widgets.text("fy_year_name", "FY2425")
dbutils.widgets.text("fy_half_year_name", "2H")
```

### 3. Execute Main Query (Cell 2)
Run the parameterized SQL query to extract data:
- Automatically applies FN2 (next fiscal year customer information)
- Dynamically determines RT (Retail) vs WS (Wholesale)
- Retrieves all metrics for the specified period
- Aggregates data by Quarter for the selected Half year

### 4. Data Export
Export the extracted data as CSV or Excel and submit to the Finance department.

---

## 📝 Important Notes

### Parameters
The notebook uses Databricks widgets for flexible period selection:
- **fy_year_name**: Any fiscal year (e.g., FY2324, FY2425, FY2526)
- **fy_half_year_name**: "1H" (first half) or "2H" (second half)

### Customer Information Retrieval Method
- Prioritize **FN2 (next fiscal year FN)**
- For RT (Retail): `customer_japan_dim_vw.jp_local_fn_code_2`
- For WS (Wholesale): `warehouse_dim_vw.jp_local_fn_code_2`
- If FN2 is NULL, fallback to current FN from giv_dimension_dim_vw

### Performance Optimization
- Pre-filtering by date range with partition pruning
- Daily aggregation performed before Quarter aggregation (prevents row explosion)
- Efficient CROSS JOIN with cal_bounds for partition pruning
- Uses UNION for combining fact tables (deduplicated automatically)

### Data Sources
Primary tables used:
- `hive_metastore.japan_gold.shipments_fct_vw`
- `hive_metastore.japan_gold.calender_dim_vw`
- `hive_metastore.japan_gold.giv_dimension_dim_vw`
- `hive_metastore.japan_gold.customer_japan_dim_vw`
- `hive_metastore.japan_gold.warehouse_dim_vw`
- `hive_metastore.japan_gold.giv_garp_dim_vw`
- `hive_metastore.japan_gold.giv_outbound_vw`
- `hive_metastore.japan_linkedexcel.indirect_ship_day_fct_vw`
- `hive_metastore.japan_gold.sl_organization_dim_vw`

---

## � Quick Start Example

### Step 1: Set Parameters
```python
# Default values are already set, modify if needed
dbutils.widgets.text("fy_year_name", "FY2425")
dbutils.widgets.text("fy_half_year_name", "2H")
```

### Step 2: Run the Main Query
Execute the SQL cell to extract data. The query will:
- Read the parameters from widgets
- Filter data for the specified period
- Apply FN2 customer information
- Aggregate metrics by Quarter

### Step 3: Review Results
The output will contain columns:
- `fn_code`, `fn_name`: Finance number (FN2)
- `dc_ws`: Customer type (DC/WS)
- `category`: Product category
- `pg_quarter`: Quarter (e.g., Q3, Q4)
- `org_code`, `org2`, `org3`, `org4`, `org5`: Organization hierarchy
- `shipment_giv_value`, `indirect_giv_shipments`, `ships_in_giv_customer_sales`, `garp_value`, `giv_outbound`: Metrics

---

## 📞 Contact
For questions about data content or extraction methods, please contact the project owner.

---

## 📅 Update History
- **Created**: February 2026
- **Target Fiscal Year**: FY2425 (parameterized for any FY)
- **Author**: Takahashi T7
- **Latest Update**: Simplified notebook with parameterization (Feb 26, 2026)
