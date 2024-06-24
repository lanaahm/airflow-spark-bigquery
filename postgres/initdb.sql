CREATE TABLE territories (
  "territoryID" varchar PRIMARY KEY,
  "territoryDescription" varchar,
  "regionID" int
);

CREATE TABLE suppliers (
  "supplierID" int PRIMARY KEY,
  "companyName" varchar,
  "contactName" varchar,
  "contactTitle" varchar,
  "address" varchar,
  "city" varchar,
  "region" varchar,
  "postalCode" varchar,
  "country" varchar,
  "phone" varchar,
  "fax" varchar,
  "homePage" varchar
);

CREATE TABLE shippers (
  "shipperID" int PRIMARY KEY,
  "companyName" varchar,
  "phone" varchar
);

CREATE TABLE regions (
  "regionID" int PRIMARY KEY,
  "regionDescription" varchar
);

CREATE TABLE products (
  "productID" int PRIMARY KEY,
  "productName" varchar,
  "supplierID" int,
  "categoryID" int,
  "quantityPerUnit" varchar,
  "unitPrice" numeric,
  "unitsInStock" int,
  "unitsOnOrder" int,
  "reorderLevel" int,
  "discontinued" int
);

CREATE TABLE orders (
  "orderID" int PRIMARY KEY,
  "customerID" varchar,
  "employeeID" int,
  "orderDate" date,
  "requiredDate" date,
  "shippedDate" date,
  "shipVia" int,
  "freight" numeric,
  "shipName" varchar,
  "shipAddress" varchar,
  "shipCity" varchar,
  "shipRegion" varchar,
  "shipPostalCode" varchar,
  "shipCountry" varchar
);

CREATE TABLE order_details (
  "orderID" int,
  "productID" int,
  "unitPrice" numeric,
  "quantity" int,
  "discount" numeric
);

CREATE TABLE employees (
  "employeeID" int PRIMARY KEY,
  "lastName" varchar,
  "firstName" varchar,
  "title" varchar,
  "titleOfCourtesy" varchar,
  "birthDate" date,
  "hireDate" date,
  "address" varchar,
  "city" varchar,
  "region" varchar,
  "postalCode" varchar,
  "country" varchar,
  "homePhone" varchar,
  "extension" varchar,
  "photo" varchar,
  "notes" text,
  "reportsTo" int,
  "photoPath" varchar
);

CREATE TABLE employee_territories (
  "employeeID" int,
  "territoryID" varchar
);

CREATE TABLE customers (
  "customerID" varchar PRIMARY KEY,
  "companyName" varchar,
  "contactName" varchar,
  "contactTitle" varchar,
  "address" varchar,
  "city" varchar,
  "region" varchar,
  "postalCode" varchar,
  "country" varchar,
  "phone" varchar,
  "fax" varchar
);

CREATE TABLE categories (
  "categoryID" int PRIMARY KEY,
  "categoryName" varchar,
  "description" text,
  "picture" varchar
);

ALTER TABLE territories ADD FOREIGN KEY ("regionID") REFERENCES regions ("regionID");
ALTER TABLE products ADD FOREIGN KEY ("supplierID") REFERENCES suppliers ("supplierID");
ALTER TABLE products ADD FOREIGN KEY ("categoryID") REFERENCES categories ("categoryID");
ALTER TABLE orders ADD FOREIGN KEY ("customerID") REFERENCES customers ("customerID");
ALTER TABLE orders ADD FOREIGN KEY ("employeeID") REFERENCES employees ("employeeID");
ALTER TABLE orders ADD FOREIGN KEY ("shipVia") REFERENCES shippers ("shipperID");
ALTER TABLE order_details ADD FOREIGN KEY ("orderID") REFERENCES orders ("orderID");
ALTER TABLE order_details ADD FOREIGN KEY ("productID") REFERENCES products ("productID");
ALTER TABLE employee_territories ADD FOREIGN KEY ("employeeID") REFERENCES employees ("employeeID");
ALTER TABLE employee_territories ADD FOREIGN KEY ("territoryID") REFERENCES territories ("territoryID");
