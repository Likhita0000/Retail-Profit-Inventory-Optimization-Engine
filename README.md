**Retail Profit & Inventory Optimization Engine
🏢 Business Context**

MetroMart (simulated) is a regional general merchandise retail chain operating 8 physical stores across one state. The company sells products across multiple categories including Electronics, Apparel, Home & Kitchen, Personal Care, and Seasonal goods.

Over the past 12 months, leadership observed:

* Stable revenue growth

* Declining gross profit margins

* Increased promotional discounting

* Frequent stockouts on high-demand SKUs

* Overstocking of slow-moving inventory

* Rising inventory carrying costs

Despite strong sales, profitability and operational efficiency are deteriorating.

The company lacks a centralized analytics system that connects sales, inventory, procurement, and discount data into a unified decision-support framework.

**🎯 Project Objective**

Design and implement an end-to-end retail analytics data warehouse using a Bronze–Silver–Gold medallion architecture to:

* Detect profit leakage drivers

* Quantify margin erosion caused by discounting

* Measure inventory efficiency and turnover

* Identify stockout-related revenue loss

* Classify products using ABC analysis

* Provide data-driven pricing and replenishment recommendations

This project simulates how a real retail operations analytics team would build decision-support infrastructure.

**📊 Core Business Questions**

- Which products generate high revenue but low profit?

- Which categories are most affected by discounting?

- Which SKUs frequently experience stockouts?

- Where is excess inventory tying up working capital?

- How efficient is inventory turnover across stores?

- Which products should be prioritized for reorder or markdown?

**🏗 Architecture**

This project follows a layered data architecture:

* Bronze Layer

Raw CSV data loaded into SQL Server without transformations.

* Silver Layer

Data cleaning, standardization, deduplication, and validation.
Conformed dimensions and cleaned fact tables are created here.

* Gold Layer

Star schema and analytical marts designed for business KPIs, profitability analysis, and executive dashboards.
