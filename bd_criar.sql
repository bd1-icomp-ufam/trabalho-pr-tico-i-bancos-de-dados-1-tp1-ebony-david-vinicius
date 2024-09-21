DROP Table IF EXISTS Category_Product CASCADE;
DROP TABLE IF EXISTS Review CASCADE;
DROP TABLE IF EXISTS Similar_Product CASCADE;
DROP TABLE IF EXISTS Product CASCADE;
DROP TABLE IF EXISTS Categories CASCADE;
DROP TABLE IF EXISTS Group_Name CASCADE;

CREATE TABLE IF NOT EXISTS Group_name (
            id INT NOT NULL,
            name VARCHAR(500) NOT NULL,
            PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS Categories (
            id INT NOT NULL,
            category VARCHAR(500) NOT NULL,
            PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS Product (
            id SERIAL PRIMARY KEY,
            asin VARCHAR(500) NOT NULL UNIQUE,
            title VARCHAR (500) NOT NULL,
            salesrank INT,
            group_id INT NOT NULL,
            FOREIGN KEY (group_id) REFERENCES Group_name (id)
);

CREATE TABLE IF NOT EXISTS Similar_Product (
            product_A_asin VARCHAR NOT NULL,
            product_B_asin VARCHAR NOT NULL,
            PRIMARY KEY (product_A_asin, product_B_asin),
            FOREIGN KEY (product_A_asin) REFERENCES Product (asin),
            FOREIGN KEY (product_B_asin) REFERENCES Product (asin)
);

CREATE TABLE IF NOT EXISTS Review (
            id SERIAL,
            product_id INT NOT NULL,
            date DATE NOT NULL,
            customer VARCHAR(500) NULL,
            rating FLOAT NULL,
            votes INT NULL,
            helpful INT NULL,
            PRIMARY KEY (id),
            FOREIGN KEY (product_id) REFERENCES Product (id)
);

 CREATE TABLE IF NOT EXISTS Category_Product (
            category_id INT NOT NULL,
            product_id INT NOT NULL,
            PRIMARY KEY (category_id, product_id),
            FOREIGN KEY (category_id) REFERENCES Categories (id),
            FOREIGN KEY (product_id) REFERENCES Product (id)
);