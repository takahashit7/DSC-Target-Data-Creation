# DSC Target Data Creation

## 概要
このリポジトリは、MIRAI Databricks上でDSC Target Dataを作成するためのNotebookを管理します。

### このREADMEが対象とするもの
MIRAI上のDivision Scorecard (DSC) レポートから前年実績データを抽出し、
Finance向けTarget Data提出に必要な形式へ整備するワークフローを説明します。

### 目的
- Customer × Category粒度で前年実績データを抽出する
- 次年度顧客情報（FN2）を用いてデータを整形する
- Finance部門へのTarget Data提出用ベースラインを作成する

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

## Notebook処理ステップ（実装順）

### 0) 共通パラメータ設定
- Widgetで `pg_fy` と `pg_half` を設定
- 各SQLセル内の `params` CTEで `${pg_fy}` / `${pg_half}` を参照
- `calender_dim_vw` から対象日付とQuarter、`cal_bounds` で期間境界を確定

### 1) Zakka + Gillette SQL の処理フロー
1. Orgマスタを最新行（org5_key単位）に正規化
2. Zakka対象カテゴリを定義し、商品をカテゴリにマッピング
3. ZakkaのShip/Move/GARP/Outboundを mapping_key × quarter で集約
4. ZakkaのFN名・Orgを最新ルールで付与し、必要なDC補完パッチを適用
5. Gillette対象カテゴリ（Shave/Oral/Appliances）を定義
6. Gilletteシナリオ（DC/NC）ごとにShip/Moveを作成
7. GilletteにGARP/Outboundを結合し、FN×Categoryの最新Orgを付与
8. Zakka結果とGillette結果を `UNION ALL` で統合し、最終列で出力

### 2) Electro SQL の処理フロー
1. 対象カテゴリ（Oral Care / Appliances）を定義
2. 商品にカテゴリとoverlap情報（overlap_76）を付与
3. Customer/Warehouse/Siteを必要列で展開（Direct Ship対策を含む）
4. GARP/Outboundを mapping_key × category × quarter で集約
5. D-1 Electro DC Move を作成（E-DC, WS Electro条件）
6. D-2 Electro DC Ship を作成（E-DC条件）
7. D-3 Electro NC（Ship + Move）を作成（E-NC, WS Electro条件）
8. D-1〜D-3を統合し、FN×Category×Quarterで最終集約
9. FN×Category単位で最新Orgを付与して最終出力

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

### What this README covers
It describes the workflow to extract prior-year actuals from MIRAI Division Scorecard (DSC) data and prepare submission-ready target data for Finance.

### Purpose
- Extract prior-year actual data at Customer × Category granularity
- Format data using next fiscal year customer information (FN2)
- Prepare baseline data for Finance target data submission

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

## Notebook Processing Steps (Execution Order)

### 0) Common Parameter Setup
- Set `pg_fy` and `pg_half` via widgets
- Reference `${pg_fy}` and `${pg_half}` in each SQL block through the `params` CTE
- Resolve target dates and quarter from `calender_dim_vw`, then create period bounds in `cal_bounds`

### 1) Zakka + Gillette SQL flow
1. Normalize organization master to the latest row by org5_key
2. Define Zakka categories and map products to categories
3. Aggregate Zakka Ship/Move/GARP/Outbound by mapping_key × quarter
4. Attach latest FN name and org, and apply DC patch logic where needed
5. Define Gillette categories (Shave/Oral/Appliances)
6. Build Gillette scenario rows (DC/NC) for Ship/Move
7. Join GARP/Outbound and attach latest org by FN × Category
8. Merge Zakka and Gillette outputs with `UNION ALL` and return final columns

### 2) Electro SQL flow
1. Define target categories (Oral Care / Appliances)
2. Map products to categories and overlap flag (overlap_76)
3. Prepare Customer/Warehouse/Site dimensions (including Direct Ship handling)
4. Aggregate GARP/Outbound by mapping_key × category × quarter
5. Build D-1 Electro DC Move (E-DC with WS Electro conditions)
6. Build D-2 Electro DC Ship (E-DC conditions)
7. Build D-3 Electro NC Ship + Move (E-NC with WS Electro conditions)
8. Union D-1 to D-3 and aggregate by FN × Category × Quarter
9. Attach latest org by FN × Category and output final dataset

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
