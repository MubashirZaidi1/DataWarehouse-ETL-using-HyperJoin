USE electronica;
CREATE TABLE product_table (
    productID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice VARCHAR(255)
);

INSERT INTO electronica.product_table (productID, productName, productPrice)
SELECT productID, productName, productPrice
FROM master.master_data;

select * from product_table;

select * from master_data;
select * from transactions;

USE electronica;
CREATE TABLE supplier_table (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(255)
);

INSERT INTO electronica.supplier_table (supplierID, supplierName)
SELECT supplierID, supplierName
FROM master.master_data
ON DUPLICATE KEY UPDATE supplierName = VALUES(supplierName);


USE electronica;
CREATE TABLE store_table (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255)
);

INSERT INTO electronica.store_table (storeID, storeName)
SELECT storeID, storeName
FROM master.master_data
ON DUPLICATE KEY UPDATE storeName = VALUES(storeName);

USE electronica;
CREATE TABLE order_table (
    OrderID INT PRIMARY KEY,
    OrderDate VARCHAR(255)
);

INSERT INTO electronica.order_table (OrderID, OrderDate)
SELECT `Order ID`, `Order Date`
FROM transaction.transactions
ON DUPLICATE KEY UPDATE OrderDate = VALUES(OrderDate);

select * from order_table;
select * from product_table;
select * from store_table;
select * from supplier_table;
select * from customer_table;
select * from fact_table;



USE electronica;
CREATE TABLE customer_table (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(255)
);

INSERT INTO electronica.customer_table (CustomerID, CustomerName, Gender)
SELECT
    CASE
        WHEN CustomerID REGEXP '^[0-9]+$' THEN CAST(CustomerID AS SIGNED)
        ELSE NULL
    END AS CustomerID,
    CustomerName,
    Gender
FROM transaction.transactions
ON DUPLICATE KEY UPDATE
    CustomerID = CASE
        WHEN VALUES(CustomerID) REGEXP '^[0-9]+$' THEN CAST(VALUES(CustomerID) AS SIGNED)
        ELSE NULL
    END;


CREATE TABLE fact_table (
    OrderID INT,
    CustomerID INT,
    ProductID INT,
    StoreID INT,
    SupplierID INT,
    OrderDate DATE,
    Quantity INT,
    Sales DECIMAL(10, 2),
    PRIMARY KEY (OrderID), -- Setting OrderID as the primary key of fact_table
    FOREIGN KEY (OrderID) REFERENCES order_table(OrderID),
    FOREIGN KEY (CustomerID) REFERENCES customer_table(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES product_table(ProductID),
    FOREIGN KEY (StoreID) REFERENCES store_table(storeID),
    FOREIGN KEY (SupplierID) REFERENCES supplier_table(supplierID)
);




