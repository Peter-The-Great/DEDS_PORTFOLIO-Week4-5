CREATE TABLE RETAILER (
    RETAILER_Retailer_code VARCHAR(255) PRIMARY KEY,
    RETAILER_Retailer_codemr VARCHAR(255),
    RETAILER_Company_name VARCHAR(255),
    RETAILER_retailer_type VARCHAR(50),
    RETAILER_address VARCHAR(255),
    RETAILER_city VARCHAR(100),
    RETAILER_region VARCHAR(100),
    RETAILER_postal_zone VARCHAR(20),
    RETAILER_phone VARCHAR(20),
    RETAILER_fax VARCHAR(20),
    RETAILER_country_name VARCHAR(100),
    RETAILER_country_language VARCHAR(50),
    RETAILER_country_currency VARCHAR(10),
    RETAILER_country_flag VARCHAR(100),
    RETAILER_country_sales_territory VARCHAR(100),
    RETAILER_segment_name VARCHAR(100),
    RETAILER_segment_description VARCHAR(255),
    RETAILER_retailer_site_code VARCHAR(50),
    RETAILER_first_name VARCHAR(100),
    RETAILER_last_name VARCHAR(100),
    RETAILER_job_position VARCHAR(100),
    RETAILER_extension VARCHAR(20),
    RETAILER_fax_alt VARCHAR(20),
    RETAILER_email VARCHAR(255),
    RETAILER_gender CHAR(1)
    FOREIGN KEY (RETAILER_Retailer_codemr) REFERENCES Retailer(RETAILER_Retailer_code)
);

CREATE TABLE SALES_STAFF (
    SALES_STAFF_code INT PRIMARY KEY,
    SALES_STAFF_first_name VARCHAR(100),
    SALES_STAFF_last_name VARCHAR(100),
    SALES_STAFF_position VARCHAR(100),
    SALES_STAFF_work_phone VARCHAR(20),
    SALES_STAFF_extension VARCHAR(20),
    SALES_STAFF_fax VARCHAR(20),
    SALES_STAFF_email VARCHAR(255),
    SALES_STAFF_date_hired DATE,
    SALES_STAFF_manager INT,
    SALES_STAFF_sales_branch VARCHAR(100),
    SALES_STAFF_address VARCHAR(255),
    SALES_STAFF_city VARCHAR(100),
    SALES_STAFF_region VARCHAR(100),
    SALES_STAFF_postal_zone VARCHAR(20),
    SALES_STAFF_country_name VARCHAR(100),
    FOREIGN KEY (SALES_STAFF_manager) REFERENCES SALES_STAFF(SALES_STAFF_code)
);
CREATE TABLE RETURN_REASON (
    RETURN_REASON_code VARCHAR(255) PRIMARY KEY,
    RETURN_REASON_description VARCHAR(255)
);
CREATE TABLE AGE_GROUP (
    AGEGROUP_age_group_code_id VARCHAR(255) PRIMARY KEY,
    AGEGROUP_upper_age_number INT,
    AGEGROUP_lower_age_number INT
);
CREATE TABLE COURSE (
    COURSE_course_code VARCHAR(255) PRIMARY KEY,
    COURSE_course_description VARCHAR(255),
);
CREATE TABLE PRODUCT (
    PRODUCT_id VARCHAR(255) PRIMARY KEY,
    PRODUCT_introduction_date DATE,
    PRODUCT_product_type_code VARCHAR(50),
    PRODUCT_product_line VARCHAR(100),
    PRODUCT_production_cost DECIMAL(10, 2),
    PRODUCT_margin DECIMAL(5, 2),
    PRODUCT_product_image VARCHAR(255),
    PRODUCT_language VARCHAR(50),
    PRODUCT_product_name VARCHAR(255),
    PRODUCT_description VARCHAR(MAX),
    PRODUCT_order_method VARCHAR(50)
);
CREATE TABLE SATISFACTION_TYPE (
    SATISFACTION_TYPE_code VARCHAR(255) PRIMARY KEY,
    SATISFACTION_TYPE_description VARCHAR(255)
);

CREATE TABLE ORDERS (
    order_header VARCHAR(255) PRIMARY KEY,
    order_detail_code VARCHAR(255),
    quantity INT,
    unit_cost DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    unit_sale_price DECIMAL(10, 2),
    product_number VARCHAR(50),
    order_date DATE,
    turnover_per_line DECIMAL(10, 2),
    total_turnover DECIMAL(10, 2),
    production_cost DECIMAL(10, 2),
    margin DECIMAL(5, 2),
    return_code INT NULL,
    return_date DATE NULL,
    return_quantity INT NULL,
    return_reason_code INT NULL,
    order_method VARCHAR(50) NULL,
    retailer_site_code VARCHAR(50) NULL,
    retailer_contact_code INT NULL,
    sales_staff_code INT NULL,
    sales_branch_code INT NULL,
    FOREIGN KEY (return_reason_code) REFERENCES RETURN_REASON(return_reason_code),
    FOREIGN KEY (order_method) REFERENCES ORDERS(order_method_code),
    FOREIGN KEY (retailer_site_code) REFERENCES RETAILER(retailer_site_code),
    FOREIGN KEY (retailer_contact_code) REFERENCES RETAILER(retailer_contact_code),
    FOREIGN KEY (sales_staff_code) REFERENCES SALES_STAFF(sales_staff_code),
    FOREIGN KEY (sales_branch_code) REFERENCES SalesBranches(sales_branch_code)
);