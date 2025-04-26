

CREATE TABLE Categories (
    id BIGINT PRIMARY KEY IDENTITY,
    name NVARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE Products (
    id BIGINT PRIMARY KEY IDENTITY,
    name NVARCHAR(255) NOT NULL,
    description NVARCHAR(MAX),
    price DECIMAL(10, 2) NOT NULL,
    category_id BIGINT,
    FOREIGN KEY (category_id) REFERENCES Categories(id)
);

CREATE TABLE Suppliers (
    id BIGINT PRIMARY KEY IDENTITY,
    name NVARCHAR(255) NOT NULL,
    contact_name NVARCHAR(255),
    phone NVARCHAR(50),
    email NVARCHAR(255)
);

CREATE TABLE Inventory (
    id BIGINT PRIMARY KEY IDENTITY,
    product_id BIGINT,
    quantity INT NOT NULL,
    location NVARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES Products(id)
);

CREATE TABLE Orders (
    id BIGINT PRIMARY KEY IDENTITY,
    order_date DATE NOT NULL,
    supplier_id BIGINT,
    FOREIGN KEY (supplier_id) REFERENCES Suppliers(id)
);

CREATE TABLE OrderDetails (
    id BIGINT PRIMARY KEY IDENTITY,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES Orders(id),
    FOREIGN KEY (product_id) REFERENCES Products(id)
);

CREATE TABLE Customers (
    id BIGINT PRIMARY KEY IDENTITY,
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    email NVARCHAR(255),
    phone NVARCHAR(50)
);

CREATE TABLE Sales (
    id BIGINT PRIMARY KEY IDENTITY,
    sale_date DATE NOT NULL,
    customer_id BIGINT,
    FOREIGN KEY (customer_id) REFERENCES Customers(id)
);

CREATE TABLE SalesDetails (
    id BIGINT PRIMARY KEY IDENTITY,
    sale_id BIGINT,
    product_id BIGINT,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (sale_id) REFERENCES Sales(id),
    FOREIGN KEY (product_id) REFERENCES Products(id)
);