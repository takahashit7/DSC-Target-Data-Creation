# DSC Target Data Creation

## 概要
このリポジトリは、MIRAI Databricks上でDSC Target Dataを作成するためのNotebookを管理します。

- 対象Notebook: `SQL for DSC Target Data.ipynb`
- 期間指定: WidgetパラメータでFY/Halfを指定
- 出力粒度: `FN × DC/WS × Category × Quarter`

## ファイル構成

```text
DSC-Target-Data-Creation/
├── README.md
└── SQL for DSC Target Data.ipynb
```

## Notebookの実装内容（最新）

Notebookは以下の3ブロック構成です。

1. **Parameter設定セル（Python）**
   - `dbutils.widgets.text("pg_fy", "FY2425")`
   - `dbutils.widgets.text("pg_half", "2H")`

2. **Zakka + Gillette SQLセル**
   - Zakka 7カテゴリ（Laundry, Fabric Enhancer, Dish Care, Air Care, Baby Care, Feminine Care, Hair Care）
   - Gilletteカテゴリ（Shave Care, Oral Care, Appliances）
   - Zakka/Gilletteを最終的に `UNION ALL` で統合
   - 出力列: `fn_code, fn_name, dc_ws, category, z_e, quarter, org2, org3, org4, org5, ships_in_giv, ship_giv, move_giv, move_niv, garp, outbound`

3. **Electro SQLセル**
   - 対象カテゴリ: Oral Care, Appliances
   - シナリオ: Electro DC Move / Electro DC Ship / Electro NC（Ship+Move）
   - 出力列は上記と同一

## 使い方

1. MIRAI Databricksで `SQL for DSC Target Data.ipynb` を開く
2. 先頭のWidgetセルで期間を指定する

```python
dbutils.widgets.text("pg_fy", "FY2425")
dbutils.widgets.text("pg_half", "2H")
```

3. SQLセルを上から順に実行する
   - Zakka + Gillette
   - Electro
4. 結果を必要形式（CSV/Excel）でエクスポートし提出に利用する

## パラメータ仕様

- `pg_fy`: Fiscal Year（例: `FY2425`）
- `pg_half`: Half Year（`1H` または `2H`）

各SQLセルでは `params` CTEで `${pg_fy}` / `${pg_half}` を参照し、
`calender_dim_vw` から対象日付・Quarterを確定しています。

## 主なデータソース

- `hive_metastore.japan_gold.shipments_fct_vw`
- `hive_metastore.japan_linkedexcel.indirect_ship_day_fct_vw`
- `hive_metastore.japan_gold.calender_dim_vw`
- `hive_metastore.japan_gold.giv_dimension_dim_vw`
- `hive_metastore.japan_gold.giv_garp_dim_vw`
- `hive_metastore.japan_gold.giv_outbound_vw`
- `hive_metastore.japan_gold.product_dim_vw`
- `hive_metastore.japan_gold.category_dim_vw`
- `hive_metastore.japan_gold.customer_japan_dim_vw`
- `hive_metastore.japan_gold.warehouse_dim_vw`
- `hive_metastore.japan_gold.site_dim_vw`
- `hive_metastore.japan_gold.sl_organization_dim_vw`

## 更新履歴

- 2026-03-05: READMEをNotebook実装（`pg_fy`/`pg_half`、Zakka+Gillette、Electro）に合わせて更新

---

## English Version

## Overview
This repository manages a Databricks notebook used to create DSC Target Data in MIRAI.

- Target notebook: `SQL for DSC Target Data.ipynb`
- Period control: fiscal year / half year via widgets
- Output granularity: `FN × DC/WS × Category × Quarter`

## File Structure

```text
DSC-Target-Data-Creation/
├── README.md
└── SQL for DSC Target Data.ipynb
```

## Notebook Structure (Current)

The notebook has three logical blocks:

1. **Parameter Cell (Python)**
   - `dbutils.widgets.text("pg_fy", "FY2425")`
   - `dbutils.widgets.text("pg_half", "2H")`

2. **Zakka + Gillette SQL Cell**
   - Zakka categories: Laundry, Fabric Enhancer, Dish Care, Air Care, Baby Care, Feminine Care, Hair Care
   - Gillette categories: Shave Care, Oral Care, Appliances
   - Final integration by `UNION ALL`
   - Output columns: `fn_code, fn_name, dc_ws, category, z_e, quarter, org2, org3, org4, org5, ships_in_giv, ship_giv, move_giv, move_niv, garp, outbound`

3. **Electro SQL Cell**
   - Target categories: Oral Care, Appliances
   - Scenarios: Electro DC Move / Electro DC Ship / Electro NC (Ship + Move)
   - Same output schema as above

## How to Use

1. Open `SQL for DSC Target Data.ipynb` in MIRAI Databricks.
2. Set widget parameters at the top of the notebook.

```python
dbutils.widgets.text("pg_fy", "FY2425")
dbutils.widgets.text("pg_half", "2H")
```

3. Run SQL cells in order:
   - Zakka + Gillette
   - Electro
4. Export results in the required format (CSV/Excel).

## Parameter Specification

- `pg_fy`: Fiscal Year (example: `FY2425`)
- `pg_half`: Half Year (`1H` or `2H`)

Each SQL block references `${pg_fy}` and `${pg_half}` in `params` CTE and resolves date/quarter boundaries from `calender_dim_vw`.

## Main Data Sources

- `hive_metastore.japan_gold.shipments_fct_vw`
- `hive_metastore.japan_linkedexcel.indirect_ship_day_fct_vw`
- `hive_metastore.japan_gold.calender_dim_vw`
- `hive_metastore.japan_gold.giv_dimension_dim_vw`
- `hive_metastore.japan_gold.giv_garp_dim_vw`
- `hive_metastore.japan_gold.giv_outbound_vw`
- `hive_metastore.japan_gold.product_dim_vw`
- `hive_metastore.japan_gold.category_dim_vw`
- `hive_metastore.japan_gold.customer_japan_dim_vw`
- `hive_metastore.japan_gold.warehouse_dim_vw`
- `hive_metastore.japan_gold.site_dim_vw`
- `hive_metastore.japan_gold.sl_organization_dim_vw`
