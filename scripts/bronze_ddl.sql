/*
====================================================================================
BRONZE LAYER – RAW DATA STRUCTURE CREATION
====================================================================================

Overview:
This script creates all Bronze layer tables for the Retail Inventory Data
Warehouse using a drop-and-recreate pattern to ensure clean deployment.

Purpose of Bronze Layer:
The Bronze layer stores raw, untransformed data exactly as received from
source systems (CRM and ERP). No business rules, data cleansing, or
standardization is applied at this stage.

Design Principles:
- All columns are defined as VARCHAR to prevent load failures due to
  formatting inconsistencies (dates, numeric formats, null strings, etc.).
- No primary keys, foreign keys, or constraints are applied.
- Tables are structured to mirror source system extracts.
- Designed for high-speed ingestion using BULK INSERT.

Execution Strategy:
- Existing tables are dropped if they exist.
- Tables are recreated to maintain schema consistency.
- Intended to be used prior to Bronze data loading procedure.

====================================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_registry', 'u') IS NOT NULL
    DROP TABLE bronze.crm_cust_registry;
CREATE TABLE bronze.crm_cust_registry(
    customer_number VARCHAR(50),
    segment VARCHAR(50),
    signup_date VARCHAR(50),
    postal_code VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_invt_mov', 'u') IS NOT NULL
    DROP TABLE bronze.erp_invt_mov;
CREATE TABLE bronze.erp_invt_mov(
    movement_ref VARCHAR(100),
    movement_date VARCHAR(50),
    store_code VARCHAR(50),
    sku_code VARCHAR(50),
    movement_type VARCHAR(50),
    quantity_change VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_product_catalog', 'u') IS NOT NULL
    DROP TABLE bronze.erp_product_catalog;
CREATE TABLE bronze.erp_product_catalog(
    sku_code VARCHAR(50),
    product_name VARCHAR(50),
    category_name VARCHAR(50),
    brand_name VARCHAR(50),
    standard_cost VARCHAR(50),
    list_price VARCHAR(50),
    active_flag VARCHAR(10)
);

IF OBJECT_ID ('bronze.erp_purchase_ord_header', 'u') IS NOT NULL
    DROP TABLE bronze.erp_purchase_ord_header;
CREATE TABLE bronze.erp_purchase_ord_header (
    po_number VARCHAR(100),
    supplier_id VARCHAR(100),
    store_code VARCHAR(50),
    order_date VARCHAR(50),
    expected_delivery_date VARCHAR(50),
    status VARCHAR(100),
    freight_cost VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_purchase_ord_line', 'u') IS NOT NULL
    DROP TABLE bronze.erp_purchase_ord_line;
CREATE TABLE bronze.erp_purchase_ord_line (
    po_number VARCHAR(100),
    line_number VARCHAR(50),
    sku_code VARCHAR(50),
    ordered_quantity VARCHAR(50),
    received_quantity VARCHAR(50),
    unit_cost VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_sales_transactions', 'u') IS NOT NULL
    DROP TABLE bronze.erp_sales_transactions;
CREATE TABLE bronze.erp_sales_transactions (
    transaction_id VARCHAR(100),
    transaction_date VARCHAR(50),
    store_code VARCHAR(50),
    sku_code VARCHAR(50),
    customer_number VARCHAR(100),
    quantity_sold VARCHAR(50),
    unit_price VARCHAR(50),
    discount_rate VARCHAR(50),
    net_amount VARCHAR(50),
    cost_amount VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_store_master', 'u') IS NOT NULL
    DROP TABLE bronze.erp_store_master;
CREATE TABLE bronze.erp_store_master (
    store_code VARCHAR(50),
    store_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    open_date VARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_supplier_directory', 'u') IS NOT NULL
    DROP TABLE bronze.erp_supplier_directory;
CREATE TABLE bronze.erp_supplier_directory (
    supplier_id VARCHAR(100),
    supplier_name VARCHAR(150),
    lead_time_days VARCHAR(50),
    reliability_score VARCHAR(50)
);
