# 🔍 Unified Data Query Engine

A scalable, intelligent data integration and querying system that brings together influencer data from Instagram and product data from Amazon and Flipkart. Designed to support cross-platform analysis and unified insights through natural language and SQL queries, this system enables powerful applications in market intelligence, influencer impact assessment, and product category performance tracking.

---

## 📌 Project Overview

### Why this project?

In today's digital economy, decision-makers often rely on fragmented datasets across multiple platforms (e.g., e-commerce and social media). This project addresses the challenge of integrating such distributed, heterogeneous data sources into a unified system that enables holistic insights.

### Target Users and Use-Cases

- **Marketers** analyzing product performance vs. influencer engagement  
- **E-commerce analysts** comparing price, rating, and availability across platforms  
- **Influencer agencies** mapping social media influence to product categories  

### Innovative Focus

Our approach features dynamic schema mapping, SQL-to-NoSQL query translation, and federated query processing, making it adaptive, extensible, and user-friendly.

---

## 🗃️ Data Sources

We use three distributed data sources hosted on different systems:

- **Instagram Data** → Stored in **MongoDB** (non-relational): Contains influencer metadata like followers, engagement, and category.
- **Flipkart Product Data** → Stored in **MySQL** (relational): Includes product names, prices, ratings, and categories.
- **Amazon Product Data** → Stored in **MongoDB** (non-relational): Structured similarly to Flipkart data.

Each source is populated with real-world-like datasets and is accessed using APIs or direct queries, depending on the query context.

---

## 🔗 Data Integration System

### Approach: Hybrid Integration

Combines materialized storage for performance and virtual access for flexibility.

### ETL Pipeline

Custom ETL scripts ingest data into MongoDB and MySQL from CSV and JSON files. Schema standardization and cleaning are performed during ingestion.

### Communication

Internal Python connectors and APIs are used to interface with all three systems.

---

## 🔄 Schema Matching and Mapping

### Dynamic Schema Matching

Using **fuzzywuzzy** to align entities like product categories and influencer topics across datasets.

### Entity Mapping

Flexible mapping mechanism between schemas, allowing data aggregation even with schema drift across platforms.

---

## 💬 Query Interface & Execution

### Query Interface

A user-facing input system where SQL queries can be written directly to interact with the unified dataset.

### Query Decomposer

The system parses the SQL query and identifies the relevant data sources and specific columns required.

### SQL-to-MongoDB Translator

If the query targets a MongoDB source (Instagram or Amazon), it is automatically converted from SQL to MongoDB query syntax.

### Federated Execution

Sub-queries are dispatched to the corresponding systems (MongoDB or MySQL). Data is fetched and merged based on the request.

### Result Aggregation

Once results from all systems are collected, they are unified, formatted, and displayed according to the original query’s intent.

---

## 🧠 Natural Language Querying (Optional Module)

Integrated **LLaMA-3.1** for converting user's natural language questions into SQL queries to make data access even more intuitive.

---

## 🚀 Innovation & Scalability

### Innovation

The integration of dynamic schema matching, SQL-to-MongoDB translation, and distributed query decomposition sets this project apart from basic hardcoded solutions.

---

## ⚙️ Tech Stack

- **Languages & Frameworks:** Python, SQL, Flask  
- **Databases:** MySQL, MongoDB  
- **Libraries & Tools:**  
  - **LLaMA-3.1** – Natural language to SQL translation  
  - **fuzzywuzzy** – For approximate string matching  
  - **Flask** – For building the query interface  
  - **PyMongo / SQLAlchemy** – For DB communication

---


