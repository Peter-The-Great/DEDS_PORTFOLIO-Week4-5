# %% [markdown]
# # PR 4.3, 5.2 en 5.3
# Van Pjotr en Sennen
# 
# Hierin gaan wij een paar queries aanvragen aan de database die wij hebben gemaakt gebasseerd op de ETL diagram van de Great_Outdoors.
# 
# Hieronder zullen we beginnen met de setup van de libraries en het verbinden met de database. We zullen de pyodbc library gebruiken om de verbinding tussen SSMS en Python. Verder gebruiken we ook pandas om data makkelijk te lezen.

# %%
import pandas as pd
import pyodbc
import os
import sqlite3
from dotenv import load_dotenv
import sqlalchemy
import json
import warnings
warnings.filterwarnings("ignore")
load_dotenv()

# %%
DB = {'servername': os.getenv('NAME'),
      'database': os.getenv('DATABASE'),
      'username': os.getenv('USER'),
      'password': os.getenv('PASSWORD')}

# Increase the connection timeout value to 30 seconds
conn_str = f"DRIVER=SQL Server;SERVER={DB['servername']};DATABASE={DB['database']};UID={DB['username']};PWD={DB['password']};Trusted_Connection=yes;Connection"

conn = pyodbc.connect(conn_str, timeout=120)
cursor = conn.cursor()

engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

# Hoe checken we of de connectie werkt?
print(cursor.execute("SELECT @@version;"))

# %% [markdown]
# Om ervoor te zorgen dat we weten dat we data kunnen aanvragen vanaf de server zullen we hier een paar queries executeren. Zodat we kunnen bevestigen dat we alles goed hebben geconfigureerd. Eerst pakken we alle tabelen van uit de database.

# %%
cursor.execute("SELECT t.name FROM sys.tables t")
tables = cursor.fetchall()

# Voor elke tabel in de database print de naam van de tabel
if(tables == []):
    print("No tables found, the database is empty.")
else:
    for table in tables:
        table = table[0]
        print(table)
    for table in tables:
        table_name = table[0]
        cursor.execute(f"DROP TABLE {table_name}")
    try:
        cursor.commit()
    except pyodbc.Error as e:
        print("Error:", e)
        conn.rollback()

# %% [markdown]
# Hieronder checken we of we alle drivers hebben geinstalleerd. Meestal maken we gebruikt van de SQL Server driver en SQLite driver. Maar als je op een nieuwer systeem zit kan je ook gebruik maken van de Microsoft Access driver gebruik maken.

# %%
pyodbc.drivers()

# %% [markdown]
# Daarna wat we willen zijn dus de functies maken die we later in het project gaan gebruiken om de data in te laden in ons project. Zodat we de data uit de bron kunnen transformeren en overzetten naar onze SQL Server database. We maken hiervoor een extract functie maken en een laad functie.

# %% [markdown]
# ## Extract
# Eerst gaan we de data uit de access database halen en ervoor zorgen dat we ze later goed kunnen transformeren in de database.

# %%
select_tables = "SELECT name FROM sqlite_master WHERE type='table'"

# Verbind met sqlite go_sales staff
sales_conn = sqlite3.connect("go_sales.sqlite")
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

staff_conn = sqlite3.connect("go_staff.sqlite")
staff_tables = pd.read_sql_query(select_tables, staff_conn)
course            = pd.read_sql_query("SELECT * FROM course;", staff_conn)
sales_branch      = pd.read_sql_query("SELECT * FROM sales_branch;", staff_conn)
sales_staff       = pd.read_sql_query("SELECT * FROM sales_staff;", staff_conn)
satisfaction      = pd.read_sql_query("SELECT * FROM satisfaction;", staff_conn)
satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type;", staff_conn)
training          = pd.read_sql_query("SELECT * FROM training;", staff_conn)
print("Imported staff")

crm_conn = sqlite3.connect("go_crm.sqlite")
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

inventory_level = pd.read_csv("GO_SALES_INVENTORY_LEVELSData.csv")
print("Imported inventory")

sales_forecast = pd.read_csv("GO_SALES_PRODUCT_FORECASTData.csv")
print("Imported sales_product_forecast")

# %% [markdown]
# ## Transform
# Nadat we de data eruit hebben gehaald, gaan de data transformeren, zodat we ze in de database kunnen stoppen. Eerst doen maken we een merge functie van de data die we hebben geëxtraheerd en de data die we al hebben in de database. Daarna gaan we de data transformeren zodat we ze in de database kunnen stoppen. Ik maak wat functies die ik heb gevonden op GitHub waarmee we makkelijk de data kunnen mergen. Hierin kan ik dat makkelijk uitvoeren.

# %% [markdown]
# ### Merge Functie

# %%
"""
Flexible method to merge two tables
- NaN values of one dataframe can be filled by the other dataframe
- Uses all available columns
- Errors when a row of the two dataframes doesn't match (df1 has 'A' and df2 has 'B' in row)
"""
def merge_tables(df1, df2, index_col):
    # Zorg ervoor dat het index_col een kolom is in beide dataframes
    if index_col not in df1.columns or index_col not in df2.columns:
        raise KeyError(f"{index_col} must be a column in both DataFrames.")
    
    df1 = df1.set_index(index_col)
    df2 = df2.set_index(index_col)

    # Identificeer de kolommen die in beide dataframes voorkomen
    common_columns = df1.columns.intersection(df2.columns)
    exclusive_df1 = df1.columns.difference(df2.columns)
    exclusive_df2 = df2.columns.difference(df1.columns)

    # Concatenate exclusive columns from each DataFrame onto the other
    df1_combined = pd.concat([df1, df2[exclusive_df2]], axis=1, sort=False)
    df2_combined = pd.concat([df2, df1[exclusive_df1]], axis=1, sort=False)

    # Los conflicts op in de common columns
    for col in common_columns:
        # Zet de kolommen van de dataframes naast elkaar
        series1, series2 = df1_combined[col].align(df2_combined[col])

        # Check voor conflicts die niet opgelost kunnen worden (waar beide dataframes een waarde hebben)
        conflict_mask = (~series1.isnull() & ~series2.isnull() & (series1 != series2))
        if conflict_mask.any():
            raise ValueError(f"Merge failed due to conflict in column '{col}'")

        # Use values from df2 where df1 is null (prioritizing df1 values)
        df1_combined[col] = series1.combine_first(series2)

    return df1_combined

# Merge duplicate tables into single table
retailer_site = merge_tables(sales_retailer_site, crm_retailer_site, 'RETAILER_SITE_CODE')
# Column name mismatch
sales_country = sales_country.rename(columns={'COUNTRY': 'COUNTRY_EN'})
country = merge_tables(sales_country, crm_country, 'COUNTRY_CODE')

# %% [markdown]
# ### JSON importeren
# Eerst zorgen we ervoor dat we de rename.json gaan importeren, waar ik de nieuwe namen van de kolommen heb gezet. Met deze kolommen gaan we de data mergen.

# %%
# importeer de json file
with open('rename.json') as f:
    json_file = json.load(f)

# Geef een lijst van alle waardes in de json file
valid_columns = list(json_file.values())

# Filter de kolommen van de dataframes, door alleen de kolommen te houden die in de json file staan.
def filterColumns(dataframe):
    valid_columns_set = set(valid_columns)
    actual_columns_set = set(dataframe.columns)
    intersection_columns = list(actual_columns_set.intersection(valid_columns_set))

    # Gebruik de kolommen die in de json file staan om de dataframes te filteren
    return dataframe[intersection_columns]

# Filter de kolommen van de dataframes, door alleen de kolommen te houden die niet in de json file staan.
def excludeColumns(dataframe, column_names):
    return dataframe[dataframe.columns.difference(column_names)]

# Check de grootte van de dataframes en print een bericht als de grootte niet overeenkomt met de verwachte grootte
def sizeCheck(df, expected_column_count):
    actual_column_count = len(df.columns)
    if actual_column_count == expected_column_count:
        print(f'Table has {actual_column_count} columns')
    else:
        raise Exception(f'Table has {actual_column_count} columns, expected {expected_column_count}')


# %% [markdown]
# ### Columns aanpassen
# Ik neem nu even wat code over van Joran zijn notebook, omdat hij een makkelijke manier heeft gegeven om types aan te geven.

# %%
"""
Get the last slice of a string
"""
def getTypes():
    types = {}
    for column in json_file.values():
        column_type = column.rsplit('_', 1)[1]
        types[column_type] = ''
    return types


"""
Uses the column name to derive a SQL Server compatible type
- The type is derived from the column name (COLUMN_NAME_type)
- Column names without a type are invalid
"""
def columnType(column_name):
    column_types = {
        'name': 'NVARCHAR(80)',
        'image': 'NVARCHAR(60)',
        'id': 'INT',
        'description': 'NTEXT',
        'money': 'DECIMAL(19,4)',
        'percentage': 'DECIMAL(12,12)',
        'date': 'NVARCHAR(30)',
        'code': 'NVARCHAR(40)',
        'char': 'CHAR(1)',
        'number': 'INT',
        'phone': 'NVARCHAR(30)',
        'address': 'NVARCHAR(80)',
        'bool': 'BIT',
    }

    err = ''
    try:
        return column_types[column_name.rsplit('_', 1)[1]]
    except IndexError:
        err = "Column name doesn't contain a type"
    except KeyError:
        err = "Column type not found"
    raise Exception(err)

"""
Method to insert dataframe data into SQL server
"""
def createTable(tablename, dataframe, PK):
    SK = ''
    if PK == None:
        SK = f'SK_{tablename}'
        columns = ''
    else:
        SK = f'SK_{PK}'
        columns = f'{PK} {columnType(PK)} NOT NULL'
    # Add Primary Key as third column
    
    # Add all the other columns
    for column in dataframe.columns:
        if column != PK: # PK is already added
            columns += f', {column} {columnType(column)}'

    surogate_columns = f"{SK} INT IDENTITY(1,1) NOT NULL PRIMARY KEY, Timestamp DATETIME NOT NULL DEFAULT(GETDATE())"

    # Create the command
    command = f"CREATE TABLE {tablename} ({surogate_columns}, {columns})"

    try:
        cursor.execute(command)
        cursor.commit()
    except pyodbc.Error as e:
        if 'There is already an object named' in str(e):
            print('Table already exists in database')
        else:
            raise(e)


"""
Method to insert dataframe data into SQL server
"""
def insertTable(tablename, dataframe, PK):
    # Add Primary Key as first column
    columns = PK
    
    # Add all the other columns
    for column in dataframe.columns:
        if column != PK: # PK is already added
            columns += f', {column}'
    
    # Execute inserts
    for i, row in dataframe.iterrows():
        values = ''
        values += str(row[PK])

        for column in dataframe.columns:
            if column != PK: # PK is already added
                try:
                    val = str(row[column]).replace("'","''")
                    if val != 'None':
                        values += f", '{val}'"
                    else:
                        values += f", NULL"
                except AttributeError:
                    values += f", NULL"

        command = f"INSERT INTO {tablename} ({columns}) VALUES ({values});\n"
        
        cursor.execute(command)
    
    try:
        cursor.commit()
    except pyodbc.Error as e:
        if 'There is already an object named' in str(e):
            print('Table already exists in database')
        else:
            print(command)
            print(e)

# Tables to create at end         
etl_tables = []

# %% [markdown]
# Nu gaan we eindelijk de data transformeren en in de database stoppen. Eerst gaan we producten, staff, satisfaction, course, sales_forcast, retailer_contact, retailer, Orders, returned_season, returned_item en Order_details importeren. Dit zijn de tabellen die we hebben gemaakt in de database.

# %% [markdown]
# ## Transforming the data
# 
# ### Producten

# %%
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

# %% [markdown]
# ### Sales_staff

# %%
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

# %% [markdown]
# ### Satisfaction_type

# %%
# Hernoem
satisfaction_type_etl = satisfaction_type.rename(columns=json_file)

# Filter
satisfaction_type_etl = filterColumns(satisfaction_type_etl)

# Check
sizeCheck(satisfaction_type_etl,2)
satisfaction_type_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Satisfaction_Type', satisfaction_type_etl, 'SATISFACTION_TYPE_id'))

# %% [markdown]
# ### Course

# %%
# Hernoem
course_etl = course.rename(columns=json_file)

# Filter
course_etl = filterColumns(course_etl)

# Check
sizeCheck(course_etl,2)
course_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Course', course_etl, 'COURSE_id'))

# %% [markdown]
# ### Sales Forecast

# %%
# Hernoem
sales_forecast_etl = sales_forecast.rename(columns=json_file)

# Filter
sales_forecast_etl = filterColumns(sales_forecast_etl)

# Check
sizeCheck(sales_forecast_etl,4)
sales_forecast_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Sales_Forecast', sales_forecast_etl, 'PRODUCT_id'))

# %% [markdown]
# ### Retailer_contact

# %%
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

# %% [markdown]
# ### Retailer

# %%
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

# %% [markdown]
# ### Orders

# %%
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

# %% [markdown]
# ### Returned_season

# %%
# Hernoem
return_reason_etl = return_reason.rename(columns=json_file)

# Filter
return_reason_etl = filterColumns(return_reason_etl)

# Check
sizeCheck(return_reason_etl,2)
return_reason_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Return_Reason', return_reason_etl, 'RETURN_REASON_id'))

# %% [markdown]
# ### Returned_item

# %%
# Hernoem 
returned_item_etl = returned_item.rename(columns=json_file)

# Filter 
returned_item_etl = filterColumns(returned_item_etl)

# Check
sizeCheck(returned_item_etl,5)
returned_item_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Returns', returned_item_etl, 'RETURNS_id'))

# %% [markdown]
# ### Order_details

# %%
# Hernoem
order_detail_etl = order_details.rename(columns=json_file)

# Filter
order_detail_etl = filterColumns(order_detail_etl)

# Check
sizeCheck(order_detail_etl,7)
order_detail_etl

# Create Table en doe het in de lijst.
etl_tables.append(('Order_Details', order_detail_etl, 'ORDER_DETAIL_id'))

# %% [markdown]
# ### Sales Target

# %%
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

# %% [markdown]
# ### Uploaden naar de database.
# 
# Nu gaan we alle data uploaden naar de database om ervoor te zorgen dat we alle data juist in de database hebben geupload.

# %%
# Nu maken we de tabellen aan
for table in etl_tables:
    print(f"Creating {table[0]}")
    createTable(table[0], table[1], table[2])
    insertTable(table[0], table[1], table[2])
    print(f"Inserted {table[0]}")

# Close the connection
print("All is done")

# %% [markdown]
# ## Loading

# %% [markdown]
# Hieronder gaan we de data inladen vanuit de SQL Server database. Met de database verzorgen we ervoor dat we makkelijk de data kunnen inladen in de database. We maken een functie die de data inlaad in de database. Dat deden we all gedeelte lijk boven, maar nu gaan we de data uit de database halen.

# %%
tables = cursor.execute("SELECT t.name FROM sys.tables t")
tables = tables.fetchall()
for table in tables:
    table = table[0]
    print(table)
print()

print("Resultaat:")
leef = cursor.execute("SELECT * FROM PRODUCT WHERE PRODUCT_id = '8'")
print(leef.fetchall())



