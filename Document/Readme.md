# 📊 Data Warehouse – After Cleaned Data Layer

This repository contains the **dimension and fact tables** created in Snowflake after data cleaning and transformation.  
It represents the **structured analytical layer** of the Data Warehouse project.

---

# 🧱 Data Warehouse Architecture

The project follows a **Medallion Architecture** approach:

- **Bronze** → Raw data  
- **Silver** → Cleaned & transformed data  
- **Gold** → Analytical (Dim & Fact tables)

---

# 📁 Project Structure

```
AfterCleanedData/
│
├── dim_tables/          # Dimension tables
├── fact_tables/         # Fact tables
├── documents/
│   ├── data_flow.png
│   ├── data_model.png
│   ├── data_integration.png
│   └── data_architecture.png
└── README.md
```

---

# 🗺️ Data Architecture

![Data Architecture](document/data_architecture.png)

---

# 🔄 Data Flow

![Data Flow](documents/data_flow.png)

---

# 🔗 Data Integration

![Data Integration](documents/data_integration.png)

---

# ⭐ Data Model (Star Schema)

![Data Model](documents/data_model.png)

The Gold layer follows a **Star Schema** design:

- **Fact Tables** → Business events / transactions  
- **Dimension Tables** → Descriptive attributes  

---

# 🧾 Tables Included

## Dimension Tables
- dim_customer  
- dim_product  
- dim_date  
- dim_store  

## Fact Tables
- fact_sales  
- fact_orders  

---

# ❄️ Technology Used

- Snowflake
- SQL
- Data Warehouse Modeling
- ETL / ELT Concepts

---

# 🚀 Purpose of This Layer

This layer is designed for:

- Business reporting  
- Dashboarding  
- Analytics queries  
- BI tools (Power BI / Tableau)

---

# 👤 Author

**Thirumalesh**  
Data Engineering Enthusiast  

