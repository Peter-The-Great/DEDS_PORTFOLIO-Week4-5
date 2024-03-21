import pandas as pd
import pyodbc
import sqlite3
import sqlalchemy
import json
import os
from settings import Settings
from tableutils import *
def run(settings: Settings):
    select_tables = "SELECT name FROM sqlite_master WHERE type='table'"

    sql_server_conn = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={settings.server};DATABASE={settings.database};UID={settings.username};PWD={settings.password};Trusted_Connection=yes")
    cursor = sql_server_conn.cursor()

    # Verbind met sqlite go_sales staff
    sales_conn = getSqlite(settings, "go_sales.sqlite")
    sales_tables = pd.read_sql_query(select_tables, sales_conn)

    sales_country       = pd.read_sql_query("SELECT * FROM country;", sales_conn)
    order_details       = pd.read_sql_query("SELECT * FROM order_details;", sales_conn)
    order_header        = pd.read_sql_query("SELECT * FROM order_header;", sales_conn)
    order_method        = pd.read_sql_query("SELECT * FROM order_method;", sales_conn)
    product             = pd.read_sql_query("SELECT * FROM product;", sales_conn)
    product_line        = pd.read_sql_query("SELECT * FROM product_line;", sales_conn)
    product_type        = pd.read_sql_query("SELECT * FROM product_type;", sales_conn)
    sales_retailer_site = pd.read_sql_query("SELECT * FROM retailer_site;", sales_conn)
    return_reason       = pd.read_sql_query("SELECT * FROM return_reason;", sales_conn)
    returned_item       = pd.read_sql_query("SELECT * FROM returned_item;", sales_conn)
    sales_branch        = pd.read_sql_query("SELECT * FROM sales_branch;", sales_conn)
    sales_staff         = pd.read_sql_query("SELECT * FROM sales_staff;", sales_conn)
    SALES_TARGETData    = pd.read_sql_query("SELECT * FROM SALES_TARGETData;", sales_conn)
    sqlite_sequence     = pd.read_sql_query("SELECT * FROM sqlite_sequence;", sales_conn)
    print("Import sales")

    staff_conn = getSqlite(settings, "go_sales_staff.sqlite")
    staff_tables = pd.read_sql_query(select_tables, staff_conn)
    course            = pd.read_sql_query("SELECT * FROM course;", staff_conn)
    sales_branch      = pd.read_sql_query("SELECT * FROM sales_branch;", staff_conn)
    sales_staff       = pd.read_sql_query("SELECT * FROM sales_staff;", staff_conn)
    satisfaction      = pd.read_sql_query("SELECT * FROM satisfaction;", staff_conn)
    satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type;", staff_conn)
    training          = pd.read_sql_query("SELECT * FROM training;", staff_conn)
    print("Imported staff")

    crm_conn = getSqlite(settings, "go_sales_crm.sqlite")
    crm_tables = pd.read_sql_query(select_tables, crm_conn)
                            
    age_group             = pd.read_sql_query("SELECT * FROM age_group;", crm_conn)
    crm_country           = pd.read_sql_query("SELECT * FROM country;", crm_conn)
    retailer              = pd.read_sql_query("SELECT * FROM retailer;", crm_conn)
    retailer_contact      = pd.read_sql_query("SELECT * FROM retailer_contact;", crm_conn)
    retailer_headquarters = pd.read_sql_query("SELECT * FROM retailer_headquarters;", crm_conn)
    retailer_segment      = pd.read_sql_query("SELECT * FROM retailer_segment;", crm_conn)
    crm_retailer_site     = pd.read_sql_query("SELECT * FROM retailer_site;", crm_conn)
    retailer_type         = pd.read_sql_query("SELECT * FROM retailer_type;", crm_conn)
    sales_demographic     = pd.read_sql_query("SELECT * FROM sales_demographic;", crm_conn)
    sales_territory       = pd.read_sql_query("SELECT * FROM sales_territory;", crm_conn)
    print("Imported crm tables")

    inventory_level = getCSV(settings, "GO_INVENTORY_LEVELData.csv")
    print("Imported inventory")

    sales_forecast = getCSV(settings, "GO_SALES_PRODUCT_FORECASTData.csv")
    print("Imported sales_product_forecast")

    json_file = {}

    with open('renames.json') as f_in:
        json_file = json.load(f_in)

    # Merge duplicate tables into single table
    retailer_site = merge_tables(sales_retailer_site, crm_retailer_site, 'RETAILER_SITE_CODE')
    # Column name mismatch
    sales_country = sales_country.rename(columns={'COUNTRY': 'COUNTRY_EN'})
    country = merge_tables(sales_country, crm_country, 'COUNTRY_CODE')
    # Tables to create at end         
    etl_tables = []

    # Merge
    product_etl = pd.merge(product, product_type, on="PRODUCT_TYPE_CODE")
    product_etl = pd.merge(product_etl, product_line, on="PRODUCT_LINE_CODE")

    # Hernoem
    product_etl = product_etl.rename(columns=json_file)

    # Filter
    product_etl = filterColumns(product_etl)

    # Check
    sizeCheck(product_etl,10)
    product_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Product', product_etl, 'PRODUCT_id'))

    # Merge
    sales_staff_etl = pd.merge(sales_staff, sales_branch, on='SALES_BRANCH_CODE')
    sales_staff_etl = pd.merge(sales_staff_etl, country, on='COUNTRY_CODE')
    sales_staff_etl = pd.merge(sales_staff_etl, sales_territory, on='SALES_TERRITORY_CODE')

    # Hernoem
    sales_staff_etl = sales_staff_etl.rename(columns=json_file)

    # Filter
    sales_staff_etl = filterColumns(sales_staff_etl)

    # Check
    sizeCheck(sales_staff_etl,23)
    sales_staff_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Sales_Staff', sales_staff_etl, 'SALES_STAFF_id'))

    # Hernoem
    sales_forecast_etl = sales_forecast.rename(columns=json_file)

    # Filter
    sales_forecast_etl = filterColumns(sales_forecast_etl)

    # Check
    sizeCheck(sales_forecast_etl,4)
    sales_forecast_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Sales_Forecast', sales_forecast_etl, 'PRODUCT_id'))

    # Hernoem
    training_etl = training.rename(columns=json_file)

    # Filter
    training_etl = filterColumns(training_etl)

    # Check
    sizeCheck(training_etl,3)
    training_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Training', training_etl, None))
    training_etl

    # Hernoem
    satisfaction_etl = satisfaction.rename(columns=json_file)

    # Filter
    satisfaction_etl = filterColumns(training_etl)

    # Check
    sizeCheck(satisfaction_etl,3)
    satisfaction_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Satisfaction', satisfaction_etl, None))
    satisfaction_etl

    # Hernoem
    course_etl = course.rename(columns=json_file)

    # Filter
    course_etl = filterColumns(course_etl)

    # Check
    sizeCheck(course_etl,2)
    course_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Course', course_etl, 'COURSE_id'))

    # Hernoem
    sales_forecast_etl = sales_forecast.rename(columns=json_file)

    # Filter
    sales_forecast_etl = filterColumns(sales_forecast_etl)

    # Check
    sizeCheck(sales_forecast_etl,4)
    sales_forecast_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Sales_Forecast', sales_forecast_etl, 'PRODUCT_id'))

    # Merge
    retailer_contact_etl = pd.merge(retailer_contact, retailer_site, on='RETAILER_SITE_CODE')
    retailer_contact_etl = pd.merge(retailer_contact_etl, country, on='COUNTRY_CODE')
    retailer_contact_etl = pd.merge(retailer_contact_etl, sales_territory, on='SALES_TERRITORY_CODE')\
        
    # Hernoem 
    retailer_contact_etl = retailer_contact_etl.rename(columns=json_file)

    # Filter
    retailer_contact_etl = filterColumns(retailer_contact_etl)

    # Check
    sizeCheck(retailer_contact_etl,23)
    retailer_contact_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Retailer_Contact', retailer_contact_etl, 'RETAILER_CONTACT_id'))

    # Merge
    retailer_etl = pd.merge(retailer, retailer_headquarters, on='RETAILER_CODEMR')
    retailer_etl = pd.merge(retailer_etl, retailer_type, on='RETAILER_TYPE_CODE')

    # Merge en hernoem de taal kolommen via de country tabel en retailer_segment tabel
    retailer_etl = pd.merge(retailer_etl, retailer_segment, on='SEGMENT_CODE').rename(columns={'LANGUAGE':'SEGMENT_LANGUAGE_code'})
    retailer_etl = pd.merge(retailer_etl, country, on='COUNTRY_CODE').rename(columns={'LANGUAGE':'COUNTRY_LANGUAGE_code'})

    # Sluit kolommen vroegtijdig uit vanwege samenvoegingsnaamconflicten, want duidelijk creert SQL Server deze kolommen.
    retailer_etl = excludeColumns(retailer_etl, ['TRIAL219','TRIAL222_x','TRIAL222_y','TRIAL222'])

    # Hernoem
    retailer_etl = pd.merge(retailer_etl, sales_territory, on='SALES_TERRITORY_CODE')\
        .rename(columns=json_file)

    # Filter
    retailer_etl = filterColumns(retailer_etl)

    # Check
    sizeCheck(retailer_etl,22)
    retailer_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Retailer', retailer_etl, 'RETAILER_id'))

    # Merge
    order_etl = pd.merge(order_header, order_method, on='ORDER_METHOD_CODE').rename(columns=json_file)

    # Sluit redundante kolommen met externe sleutels uit
    # RETAILER_SITE_code word afgeleid van RETAILER_CONTACT_id
    # SALES_BRANCH_code word afgeleid van SALES_STAFF_id
    order_etl = excludeColumns(order_etl, ['RETAILER_SITE_id', 'SALES_BRANCH_id'])

    order_etl.reset_index(inplace=True)
    order_etl.rename(columns={'index': 'SURROGATE_KEY'}, inplace=True)

    # Filter
    order_etl = filterColumns(order_etl)

    # Check
    sizeCheck(order_etl,7)
    order_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Orders', order_etl, 'ORDER_TABLE_id'))

    # Hernoem
    return_reason_etl = return_reason.rename(columns=json_file)

    # Filter
    return_reason_etl = filterColumns(return_reason_etl)

    # Check
    sizeCheck(return_reason_etl,2)
    return_reason_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Return_Reason', return_reason_etl, 'RETURN_REASON_id'))

    # Hernoem 
    returned_item_etl = returned_item.rename(columns=json_file)

    # Filter 
    returned_item_etl = filterColumns(returned_item_etl)

    # Check
    sizeCheck(returned_item_etl,5)
    returned_item_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Returns', returned_item_etl, 'RETURNS_id'))

    # Hernoem
    order_detail_etl = order_details.rename(columns=json_file)

    # Filter
    order_detail_etl = filterColumns(order_detail_etl)

    # Check
    sizeCheck(order_detail_etl,7)
    order_detail_etl

    # Create Table en doe het in de lijst.
    etl_tables.append(('Order_Details', order_detail_etl, 'ORDER_DETAIL_id'))

    # Hernoem
    sales_target_etl = SALES_TARGETData.rename(columns=json_file)
    sales_target_etl = sales_target_etl.rename(columns={'Id':'TARGET_id'})

    # Filter
    sales_target_etl = filterColumns(sales_target_etl)

    # Check
    sizeCheck(sales_target_etl,5)
    sales_target_etl  

    # Create Table en doe het in de lijst.
    etl_tables.append(('Sales_Target', sales_target_etl, 'TARGET_id'))

    # Nu maken we de tabellen aan
    for table in etl_tables:
        print(f"Creating {table[0]}")
        createTable(table[0], table[1], table[2])
        insertTable(table[0], table[1], table[2])
        print(f"Inserted {table[0]}")

    # Close the connection
    print("All is done")

    tables = cursor.execute("SELECT t.name FROM sys.tables t")
    tables = tables.fetchall()
    for table in tables:
        table = table[0]
        print(table)
    print()

    print("Resultaat:")
    leef = cursor.execute("SELECT * FROM PRODUCT WHERE PRODUCT_id = '8'")
    print(leef.fetchall())