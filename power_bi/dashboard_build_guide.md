# Power BI Dashboard Build Guide
## Retail Sales Analytics & Customer Insights Platform

This step-by-step guide walks you through connecting Power BI Desktop to the curated data warehouse (either live on **Amazon Redshift** or directly from **Local Parquet** files), configuring the star schema relationships, importing the theme, and building all 4 enterprise dashboard pages.

---

## 1. Prerequisites & Connection Setup

### Option A: Direct Connection to Amazon Redshift (Cloud Mode)
1. Open **Power BI Desktop**.
2. Click **Get Data** -> **More...** -> **Database** -> **Amazon Redshift** -> **Connect**.
3. In the Redshift connection dialog:
   - **Server**: `<your-workgroup-name>.<account-id>.<region>.redshift-serverless.amazonaws.com`
   - **Database**: `dev`
   - **Data Connectivity mode**: **Import** (Recommended for high DAX performance and time intelligence functions)
4. Authenticate using database credentials or AWS IAM.
5. In the Navigator, expand `retail_dw` and select:
   - `dim_date`
   - `dim_product`
   - `dim_store`
   - `dim_supplier`
   - `dim_promotion`
   - `dim_customer` (filtered to `is_current = 1` in Power Query)
   - `fact_sales`
   - `fact_returns`
   - `customer_rfm`

### Option B: Local Gold Parquet Files (Local Dev Mode)
If Redshift Serverless is not running or you are developing offline:
1. In Power BI Desktop, click **Get Data** -> **Folder** -> browse to `c:\Users\RAKSHIT\Desktop\hackathon-project\data\gold`.
2. Click **Transform Data** (Power Query Editor).
3. Connect to each parquet subfolder (`dim_date`, `dim_product`, `dim_customer`, `fact_sales`, `fact_returns`, `customer_rfm`, etc.) using the native Parquet reader:
   ```powerquery
   let
       Source = Parquet.Document(File.Contents("C:\Users\RAKSHIT\Desktop\hackathon-project\data\gold\dim_date\part-*.parquet"))
   in
       Source
   ```
4. For partitioned fact tables (`fact_sales` partitioned by `date_key`), point Power Query to the folder and combine files.

---

## 2. Data Modeling & Relationship Configuration

Navigate to the **Model View** (left navigation bar) in Power BI Desktop. Configure the following 1-to-many (`1:*`) single-directional relationships:

| Dimension Table | Primary Key | Fact Table | Foreign Key | Cardinality | Cross Filter Direction |
|---|---|---|---|---|---|
| `dim_date` | `date_key` | `fact_sales` | `date_key` | 1 to Many (`1:*`) | Single |
| `dim_date` | `date_key` | `fact_returns` | `return_date_key` | 1 to Many (`1:*`) | Single |
| `dim_product` | `product_key` | `fact_sales` | `product_key` | 1 to Many (`1:*`) | Single |
| `dim_product` | `product_key` | `fact_returns` | `product_key` | 1 to Many (`1:*`) | Single |
| `dim_customer` | `customer_key` | `fact_sales` | `customer_key` | 1 to Many (`1:*`) | Single |
| `dim_customer` | `customer_key` | `customer_rfm` | `customer_key` | 1 to 1 (`1:1`) | Both / Single |
| `dim_store` | `store_key` | `fact_sales` | `store_key` | 1 to Many (`1:*`) | Single |
| `dim_promotion` | `promo_key` | `fact_sales` | `promo_key` | 1 to Many (`1:*`) | Single |
| `dim_supplier` | `supplier_key` | `fact_sales` | `supplier_key` | 1 to Many (`1:*`) | Single |

### Important Modeling Steps:
1. **Mark as Date Table**:
   - Right-click `dim_date` in the Data pane -> **Mark as date table**.
   - Select `full_date` as the Date column.
2. **Create Measure Table**:
   - Click **Home** -> **Enter Data** -> name the table `_Measures`.
   - Copy and paste DAX measures from [`power_bi/dax_measures.txt`](file:///c:/Users/RAKSHIT/Desktop/hackathon-project/power_bi/dax_measures.txt).
   - Delete the empty `Column1` from `_Measures`.
3. **Apply Theme**:
   - Go to **View** tab -> **Themes dropdown** -> **Browse for themes** -> select [`power_bi/retail_theme.json`](file:///c:/Users/RAKSHIT/Desktop/hackathon-project/power_bi/retail_theme.json).

---

## 3. Visual-by-Visual Build Steps

---

### Page 1: Executive Overview
> **Objective**: Provide C-suite and VP Retail executives a high-level view of topline sales, transaction counts, margin indicators, and return rates with Year-over-Year (YoY) performance.

#### Visual 1.1: Top KPI Header Cards (5 Cards across the top)
- **Position**: Top banner `Y=10, Height=100`
- **Card 1**: `Total Sales` (Format: £#,##0)
- **Card 2**: `Orders` (Format: #,##0)
- **Card 3**: `Units Sold` (Format: #,##0)
- **Card 4**: `Avg Order Value` (Format: £#,##0.00)
- **Card 5**: `Return Rate %` (Format: 0.00%) with conditional color (Red if > 5%, Teal if <= 5%)

#### Visual 1.2: Monthly Sales vs Last Year (Line and Clustered Column Chart)
- **X-Axis**: `dim_date[year]`, `dim_date[month_name]`
- **Column Y-Axis**: `[Total Sales]`
- **Line Y-Axis**: `[Sales LY]`
- **Tooltip**: `[YoY Growth %]`, `[Orders]`
- **Title**: *Monthly Sales vs Previous Year Benchmark (GBP)*

#### Visual 1.3: Top 10 Revenue Generating Products (Clustered Bar Chart)
- **Y-Axis**: `dim_product[product_name]` (Top 10 filter applied in Filters pane by `[Total Sales]`)
- **X-Axis**: `[Total Sales]`
- **Data Labels**: On, currency formatted.
- **Title**: *Top 10 Products by Net Sales Revenue*

#### Visual 1.4: Sales Breakdown by Store Region / Country (Donut Chart)
- **Legend**: `dim_store[region]`
- **Values**: `[Total Sales]`
- **Title**: *Regional Sales Contribution*

#### Visual 1.5: Executive Slicers (Right or Top sidebar)
- **Date Range Slicer**: `dim_date[year]` (Tile or Dropdown) and `dim_date[month_name]`
- **Store Region Slicer**: `dim_store[region]` (Dropdown)
- **Loyalty Tier Slicer**: `dim_customer[loyalty_tier]` (Dropdown)

---

### Page 2: Product & Supplier Performance
> **Objective**: Enable Category Managers and Procurement leads to identify top-performing product categories, vendor reliability, and defect-prone product lines.

#### Visual 2.1: Category Revenue and Volume Matrix (Matrix Visual)
- **Rows**: `dim_product[category]`, `dim_product[sub_category]`
- **Values**: `[Total Sales]`, `[Units Sold]`, `[Avg Selling Price]`, `[Total Returns]`, `[Return Rate %]`
- **Format**: Enable Data Bars on `[Total Sales]` and background color formatting on `[Return Rate %]`.
- **Title**: *Category Performance & Margin Matrix*

#### Visual 2.2: Supplier Contribution vs Returns (Scatter Chart)
- **X-Axis**: `[Total Sales]`
- **Y-Axis**: `[Return Rate %]`
- **Size**: `[Units Sold]`
- **Play Axis / Legend**: `dim_supplier[supplier_name]`
- **Quadrant Lines**: Median Sales X-axis, 5% Return Rate Y-axis.
- **Title**: *Supplier Risk Matrix: Revenue vs Return Rate*

#### Visual 2.3: Supplier Rating and Performance Table
- **Columns**: `dim_supplier[supplier_name]`, `dim_supplier[country]`, `dim_supplier[rating]`, `[Total Sales]`, `[Orders]`, `[Return Rate %]`
- **Sort**: `[Total Sales]` Descending.
- **Title**: *Supplier Scorecard*

---

### Page 3: Customer Insights & RFM Segmentation
> **Objective**: Enable CRM and retention marketing teams to identify high-value Champions, loyal repeat purchasers, and churn-risk customers for targeted marketing campaigns.

#### Visual 3.1: RFM Segment Summary Cards (4 Cards)
- **Card 1**: `[Champions Count]` (Tag: "High Value & Frequent")
- **Card 2**: `[Loyal Customers Count]` (Tag: "Consistent Buyers")
- **Card 3**: `[At Risk Count]` (Tag: "Action Needed")
- **Card 4**: `[Lost Customers Count]` (Tag: "Dormant")

#### Visual 3.2: Customer RFM Segment Distribution (Treemap)
- **Category**: `customer_rfm[rfm_segment]`
- **Values**: `[Customer Count]`
- **Tooltips**: `[Total Sales]`, `[Orders Per Customer]`, `[Avg Order Value]`
- **Colors**: Champions (Deep Blue), Loyal (Teal), At Risk (Amber), Lost (Slate Gray).
- **Title**: *Customer Base by RFM Segment*

#### Visual 3.3: Recency vs Monetary Value (Scatter Plot)
- **X-Axis**: `customer_rfm[recency_days]` (Days since last purchase)
- **Y-Axis**: `customer_rfm[monetary_value]` (Total historical spend)
- **Size**: `customer_rfm[frequency]` (Order count)
- **Legend / Color**: `customer_rfm[rfm_segment]`
- **Title**: *Customer Spend vs Days Inactive (RFM Clustering)*

#### Visual 3.4: Customer Segment Detail Table
- **Columns**: `dim_customer[customer_id]`, `dim_customer[name]`, `dim_customer[segment]`, `customer_rfm[rfm_segment]`, `customer_rfm[recency_days]`, `customer_rfm[frequency]`, `customer_rfm[monetary_value]`
- **Sort**: `customer_rfm[monetary_value]` Descending.
- **Search Slicer**: Search by `dim_customer[name]` or `customer_id`.

---

### Page 4: Promotions & Returns Analytics
> **Objective**: Measure promotional campaign ROI, discount elasticity, and pinpoint root causes of merchandise returns to minimize inventory write-downs.

#### Visual 4.1: Promotional Sales Lift (Clustered Column Chart)
- **X-Axis**: `dim_date[month_name]`
- **Series / Legend**: `dim_promotion[promotion_type]`
- **Values**: `[Total Sales]`
- **Title**: *Monthly Sales Volume by Promotion Type*

#### Visual 4.2: Return Reasons Breakdown (Pareto / Clustered Bar Chart)
- **Y-Axis**: `fact_returns[return_reason]`
- **X-Axis**: `[Total Returns]` (£ Refund Amount)
- **Secondary Line**: Cumulative Return %
- **Title**: *Primary Return Reasons by Refund Impact*

#### Visual 4.3: Promotion Campaign Effectiveness Table
- **Columns**: `dim_promotion[promo_name]`, `dim_promotion[promotion_type]`, `dim_promotion[discount_percent]`, `[Promo Sales]`, `[Promo Orders]`, `[Avg Order Value]`
- **Sort**: `[Promo Sales]` Descending.
- **Title**: *Campaign ROI and Order Volume*

#### Visual 4.4: High Return Impact Products Table
- **Columns**: `dim_product[product_name]`, `dim_product[category]`, `[Units Sold]`, `[Units Returned]`, `[Return Rate %]`, `[Total Returns]`
- **Filter**: `[Units Returned] > 0`
- **Conditional Formatting**: Red background on `[Return Rate %] > 5%`.
- **Title**: *Top Defective / Returned Stock Lines*

---

## 4. Verification Checklist

When opening your Power BI file or reviewing the implementation:
1. Verify all 11 DAX core measures calculate without error.
2. Ensure relationships are strictly 1:* from dimensions to facts without ambiguous bidirectional paths.
3. Check that dates cross-filter facts dynamically when selecting slicers.
4. Verify that the `retail_theme.json` applies consistent font hierarchies and palette colors across all visuals.
