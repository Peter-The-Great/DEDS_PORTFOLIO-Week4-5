CREATE TABLE course(
    course_id varchar(255),
    course_description varchar(255),
    PRIMARY KEY (course_id)
);

CREATE TABLE age_group(
    age_group_id varchar(255),
    age_group_description varchar(255),
    upper_age int,
    lower_age int,
    PRIMARY KEY (age_group_id)
);

CREATE TABLE sales_demographic(
    demographic_code varchar(255),
    sales_percentage float,
    retailer_codemr varchar(255),
    age_group_id varchar(255),
    PRIMARY KEY (demographic_code),
    FOREIGN KEY (retailer_codemr) REFERENCES retailer(retailer_codemr),
    FOREIGN KEY (age_group_id) REFERENCES age_group(age_group_id)
);

CREATE TABLE retailer_site(
    retailer_codemr varchar(255),
    retailer_name varchar(255),
    phone_number varchar(255),
    fax varchar(255),
    sales_territory_name varchar(255),
    region_name varchar(255),
    city_name varchar(255),
    address varchar(255),
    postal_zone_code varchar(255),
    segment_code varchar(255),
    PRIMARY KEY (retailer_codemr)
);

CREATE TABLE retailer(
    retailer_codemr varchar(255),
    company_name varchar(255),
    phone_number varchar(255),
    fax varchar(255),
    sales_territory_name varchar(255),
    region_name varchar(255),
    city_name varchar(255),
    address varchar(255),
    postal_zone_code varchar(255),
    PRIMARY KEY (retailer_codemr)
);

CREATE TABLE country(
    country_code varchar(255),
    country_name varchar(255),
    language_name varchar(255),
    currency_name varchar(255),
    flag_name varchar(255),
    sales_territory_code varchar(255),
    sales_territory_name varchar(255),
    PRIMARY KEY (country_code)
);

CREATE TABLE sales_staff(
    sales_staff_code varchar(255),
    first_name varchar(255),
    last_name varchar(255),
    extension varchar(255),
    work_phone varchar(255),
    fax varchar(255),
    email varchar(255),
    date_hired date,
    sales_branch_name varchar(255),
    manager_name varchar(255),
    PRIMARY KEY (sales_staff_code),
    FOREIGN KEY (sales_branch_name) REFERENCES sales_branch(name),
    FOREIGN KEY (manager_name) REFERENCES sales_staff(first_name)
);

CREATE TABLE satisfaction_type(
    satisfaction_type_code varchar(255),
    satisfaction_type_description varchar(255),
    PRIMARY KEY (satisfaction_type_code)
);

CREATE TABLE return_season(
    return_reason_code varchar(255),
    return_description_en varchar(255),
    PRIMARY KEY (return_reason_code)
);

CREATE TABLE product(
    product_id varchar(255),
    product_image varchar(255),
    product_name varchar(255),
    product_description varchar(255),
    language varchar(255),
    production_cost float,
    cost_margin_percentage float,
    introduction_date date,
    order_method_name varchar(255),
    product_line_name varchar(255),
    product_type_name varchar(255),
    PRIMARY KEY (product_id),
    FOREIGN KEY (order_method_name) REFERENCES order_method(name),
    FOREIGN KEY (product_line_name) REFERENCES product_line(name),
    FOREIGN KEY (product_type_name) REFERENCES product_type(name)
);