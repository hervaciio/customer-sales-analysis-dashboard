-- =====================================================
-- DATABASE: sales_analytics
-- PROJECT: Customer Sales Analysis
-- AUTHOR: Karen Gervacio
-- =====================================================


CREATE TABLE staging_superstore (
  RowID VARCHAR(50),
  OrderID VARCHAR(50),
  OrderDate VARCHAR(50),
  ShipDate VARCHAR(50),
  ShipMode VARCHAR(50),
  CustomerID VARCHAR(50),
  CustomerName VARCHAR(255),
  Segment VARCHAR(50),
  Country VARCHAR(100),
  City VARCHAR(100),
  State VARCHAR(100),
  PostalCode VARCHAR(50),
  Region VARCHAR(50),
  ProductID VARCHAR(50),
  Category VARCHAR(100),
  SubCategory VARCHAR(100),
  ProductName VARCHAR(255),
  Sales DECIMAL,
  Quantity VARCHAR(50),
  Discount DECIMAL,
  Profit DECIMAL
);


LOAD DATA INFILE 'C:/Users/Administrator/Desktop/superstore.csv'
INTO TABLE staging_superstore
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;



CREATE TABLE superstore_clean AS
SELECT
  RowID,
  OrderID,
  STR_TO_DATE(OrderDate, '%Y-%m-%d') AS OrderDate,
  STR_TO_DATE(ShipDate, '%Y-%m-%d') AS ShipDate,
  ShipMode,
  CustomerID,
  CustomerName,
  Segment,
  Country,
  City,
  State,
  PostalCode,
  Region,
  ProductID,
  Category,
  SubCategory,
  ProductName,
  Sales,
  Quantity,
  Discount,
  Profit
FROM staging_superstore;


CREATE TABLE products (
  ProductID VARCHAR(50) PRIMARY KEY,
  ProductName VARCHAR(255),
  Category VARCHAR(100),
  SubCategory VARCHAR(100)
);


CREATE TABLE customers (
  CustomerID VARCHAR(50) PRIMARY KEY,
  CustomerName VARCHAR(255),
  Segment VARCHAR(50),
  City VARCHAR(100),
  State VARCHAR(100),
  PostalCode VARCHAR(50),
  Country VARCHAR(100)
);


CREATE TABLE orders (
  OrderID VARCHAR(50) PRIMARY KEY,
  OrderDate DATE,
  ShipDate DATE,
  ShipMode VARCHAR(50),
  CustomerID VARCHAR(50),
  ProductID VARCHAR(50),
  Quantity VARCHAR(50),
  Discount DECIMAL(6,2), 
  Sales DECIMAL(12,2),
  Profit DECIMAL(12,2),
  FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
  FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

INSERT IGNORE INTO products (ProductID, ProductName, Category, SubCategory)
SELECT DISTINCT
	   ProductID,
       ProductName,
       Category,
       SubCategory
FROM superstore_clean;

INSERT IGNORE INTO customers (CustomerID, CustomerName, Segment, City, State, PostalCode, Country)
SELECT DISTINCT CustomerID, CustomerName, Segment, City, State, PostalCode, Country
FROM superstore_clean;


INSERT IGNORE INTO orders (OrderID, OrderDate, ShipDate, ShipMode, CustomerID, ProductID, Quantity, Discount, Sales, Profit)
SELECT DISTINCT
  OrderID,
  STR_TO_DATE(OrderDate, '%Y-%m-%d'),   -- adjust format to match CSV (or '%Y-%m-%d')
  STR_TO_DATE(ShipDate, '%Y-%m-%d'),
  ShipMode,
  CustomerID,
  ProductID,
  Quantity,
  Discount,
  Sales,
  Profit
FROM superstore_clean
WHERE OrderID IS NOT NULL;



CREATE INDEX idx_orders_orderdate ON orders(OrderDate);
CREATE INDEX idx_orders_customer ON orders(CustomerID);
CREATE INDEX idx_orders_product ON orders(ProductID);



-- =====================
-- 1️. CREATE MASTER VIEW
-- =====================


CREATE OR REPLACE VIEW vw_sales_full AS
SELECT
  o.OrderID,
  o.OrderDate,
  o.ShipDate,
  o.ShipMode,
  o.CustomerID,
  c.CustomerName,
  c.Segment,
  o.ProductID,
  p.ProductName,
  p.Category,
  p.SubCategory,
  o.Quantity,
  o.Discount,
  o.Sales,
  o.Profit
FROM orders o
LEFT JOIN customers c ON o.CustomerID = c.CustomerID
LEFT JOIN products p ON o.ProductID = p.ProductID;




-- =====================
-- 2. Core KPIs
-- =====================

#Total Sales & Profit
SELECT 
  SUM(Sales) AS total_sales,
  SUM(Profit) AS total_profit,
  ROUND(SUM(Profit)/NULLIF(SUM(Sales),0)*100,2) AS profit_margin_pct
FROM orders;


#Sales Over Time
select date_format(OrderDate, '%Y-%m') as Month,
	sum(Sales) as Sales,
    sum(Profit) as Profit
from orders
group by month
order by month;


#Top 10 Customers by Sale
SELECT c.CustomerID, c.CustomerName, SUM(o.sales) AS total_sales, COUNT(DISTINCT o.OrderID) AS orders_count
FROM orders o
JOIN customers c USING (CustomerID)
GROUP BY c.CustomerID, c.CustomerName
ORDER BY total_sales DESC
LIMIT 10;


#Top Categories/Products
-- Category
SELECT category, SUM(sales) AS sales, SUM(profit) AS profit
FROM vw_sales_full
GROUP BY category
ORDER BY sales DESC;

-- Top 10 products
SELECT ProductName, SubCategory, SUM(Sales) AS sales, SUM(Profit) AS profit
FROM vw_sales_full
GROUP BY ProductName, SubCategory
ORDER BY sales DESC
LIMIT 10;



-- =====================
-- 3. Customer behavior & RFM segmentation
-- =====================


#Prepare last order date and monetary values per customer
CREATE OR REPLACE VIEW vw_customer_metrics AS
SELECT 
  CustomerID,
  MAX(OrderDate) AS last_order_date,
  COUNT(DISTINCT OrderID) AS frequency,
  SUM(Sales) AS monetary,
  DATEDIFF(CURDATE(), MAX(OrderDate)) AS recency_days
FROM orders
GROUP BY CustomerID;


#Simple RFM buckets using CASE
SELECT 
  CustomerID,
  recency_days,
  frequency,
  monetary,
  CASE 
    WHEN recency_days <= 30 THEN 'R1'
    WHEN recency_days <= 90 THEN 'R2'
    ELSE 'R3' END AS recency_bucket,
  CASE 
    WHEN frequency >= 10 THEN 'F1'
    WHEN frequency >= 5 THEN 'F2'
    ELSE 'F3' END AS frequency_bucket,
  CASE 
    WHEN monetary >= 10000 THEN 'M1'
    WHEN monetary >= 2000 THEN 'M2'
    ELSE 'M3' END AS monetary_bucket
FROM vw_customer_metrics
ORDER BY monetary DESC
LIMIT 50;



-- =====================
-- 4. Lifetime Value & Average Order Value (AOV)
-- =====================


#Average Order Value
SELECT ROUND(SUM(Sales) / NULLIF(COUNT(DISTINCT OrderID),0),2) AS avg_order_value
FROM orders;


#Customer Lifetime Value (cohort simplified)
SELECT 
  CustomerID,
  SUM(Sales) AS lifetime_sales,
  COUNT(DISTINCT OrderID) AS orders,
  ROUND(SUM(Sales)/NULLIF(COUNT(DISTINCT OrderID),0),2) AS aov
FROM orders
GROUP BY CustomerID
ORDER BY lifetime_sales DESC
LIMIT 20;



-- =====================
-- 5. Discount vs Profit Analysis (to measure campaign effectiveness)
-- =====================

SELECT 
  ROUND(discount,2) AS discount_pct,
  COUNT(*) AS transactions,
  SUM(Sales) AS total_sales,
  SUM(Profit) AS total_profit,
  ROUND(SUM(profit)/NULLIF(SUM(Sales),0)*100,2) AS profit_margin_pct
FROM orders
GROUP BY discount_pct
ORDER BY discount_pct;



-- =====================
-- 6. Cohort / Retention analysis (monthly cohorts)
-- =====================

-- first order month per customer
CREATE OR REPLACE VIEW vw_customer_first_order AS
SELECT CustomerID, DATE_FORMAT(MIN(OrderDate), '%Y-%m-01') AS cohort_month
FROM orders
GROUP BY CustomerID;

-- orders joined to cohort
SELECT
  f.cohort_month,
  DATE_FORMAT(o.OrderDate, '%Y-%m-01') AS order_month,
  COUNT(DISTINCT o.CustomerID) AS active_customers
FROM orders o
JOIN vw_customer_first_order f ON o.CustomerID = f.CustomerID
GROUP BY f.cohort_month, order_month
ORDER BY f.cohort_month, order_month;



-- =====================
-- 7. Useful analytical queries (segments, regions, channels, campaigns)
-- =====================

#Sales by Region & Segment
SELECT Segment, SUM(Sales) AS sales, SUM(Profit) AS profit
FROM vw_sales_full
GROUP BY Segment
ORDER BY sales DESC;


