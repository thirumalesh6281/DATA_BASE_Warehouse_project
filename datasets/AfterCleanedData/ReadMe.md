
# 📊 AfterCleanedData – Gold Layer (Dim & Fact Tables)

This folder contains the **final cleaned and transformed data** from the Snowflake Data Warehouse project.  
It represents the **Gold Layer**, structured using a **Star Schema** with Dimension and Fact tables ready for analytics and reporting.

---

# 🏗️ Data Warehouse Layer
**Layer:** Gold  
**Source:** Silver (cleaned & standardized data)  
**Purpose:** Business analytics & reporting  

The data in this folder is fully:
- Cleaned  
- Deduplicated  
- Standardized  
- Business-ready  

---

# ⭐ Star Schema Structure

The Gold layer follows a **Star Schema** design:

Fact table connects to multiple dimension tables for analysis.


---

# 📁 Tables Included

## 📌 Dimension Tables

### 👤 dim_customer
Customer master data for analysis.

**Columns:**
- customer_id  
- customer_key  
- first_name  
- last_name  
- gender  
- marital_status  
- create_date  

---

### 📦 dim_product
Product information and category hierarchy.

**Columns:**
- product_id  
- product_key  
- category_id  
- product_name  
- product_line  
- product_cost  
- start_date  
- end_date  

---

### 🌍 dim_location
Customer geographic information.

**Columns:**
- customer_id  
- country  

---

## 📈 Fact Table

### 💰 fact_sales
Transactional sales data for business metrics.

**Columns:**
- order_number  
- product_key  
- customer_id  
- order_date  
- ship_date  
- due_date  
- sales_amount  
- quantity  
- price  

---

# 🔄 Data Processing Flow

Raw Source Files
↓
Bronze Layer (Raw Load)
↓
Silver Layer (Clean & Standardize)
↓
Gold Layer (Dim & Fact)
↓
Analytics / BI / Reporting


---

# 📊 Business Use Cases

This Gold dataset supports:

- Sales analysis  
- Customer segmentation  
- Product performance  
- Geographic reporting  
- Time-based trends  

---

# 🧰 Technology Used

- Snowflake Data Warehouse  
- SQL (ETL & Transformations)  
- Star Schema Modeling  
- Dimensional Modeling  

---

# 📌 Notes

- Data exported from Snowflake Gold schema  
- Suitable for BI tools (Power BI / Tableau)  
- Represents final curated warehouse layer  

---

# 👨‍💻 Author

**Thirumalesh**  
Data Engineering Enthusiast  
Snowflake • SQL • Data Warehousing  
