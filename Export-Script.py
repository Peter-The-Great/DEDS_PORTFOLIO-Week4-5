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
import numpy as np
import sqlalchemy
import json
import warnings
warnings.filterwarnings("ignore")

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
sales_conn = sqlite3.connect("data/go_sales.sqlite")
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

staff_conn = sqlite3.connect("data/go_staff.sqlite")
staff_tables = pd.read_sql_query(select_tables, staff_conn)
course            = pd.read_sql_query("SELECT * FROM course;", staff_conn)
sales_branch      = pd.read_sql_query("SELECT * FROM sales_branch;", staff_conn)
sales_staff       = pd.read_sql_query("SELECT * FROM sales_staff;", staff_conn)
satisfaction      = pd.read_sql_query("SELECT * FROM satisfaction;", staff_conn)
satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type;", staff_conn)
training          = pd.read_sql_query("SELECT * FROM training;", staff_conn)
print("Imported staff")

crm_conn = sqlite3.connect("data/go_crm.sqlite")
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

inventory_level = pd.read_csv("data/GO_SALES_INVENTORY_LEVELSData.csv")
print("Imported inventory")

sales_forecast = pd.read_csv("data/GO_SALES_PRODUCT_FORECASTData.csv")
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
# Filter de kolommen van de dataframes, door alleen de kolommen te houden die in de json file staan.
def filterColumns(dataframe):
    # importeer de json file
    with open('rename.json') as f:
        json_file = json.load(f)

    # Geef een lijst van alle waardes in de json file
    valid_columns = list(json_file.values())
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
# importeer de json file
with open('rename.json') as f:
    json_file = json.load(f)

"""
Pak de verschillende types.
"""
def getTypes():
    types = {}
    # importeer de json file
    with open('rename.json') as f:
        json_file = json.load(f)
    for column in json_file.values():
        column_type = column.rsplit('_', 1)[1]
        types[column_type] = ''
    return types


"""
Gebruikt de kolomnaam om een ​​SQL Server-compatibel type af te leiden
- Het type is afgeleid van de kolomnaam (COLUMN_NAME_type)
- Kolomnamen zonder type zijn ongeldig
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
Methode om tabellen aan te maken van dataframes in SQL server
"""
def createTable(tablename, dataframe, PK, SK_list, cursor):
    SK = ''
    columns = ''
    foreign_SQL_SK_columns = ''
    if PK == None:
        PK = dataframe.columns[0]
        SK = f'SK_{tablename}'
        columns = f'{PK} {columnType(PK)}'
    else:
        SK = f'SK_{PK}'
        columns = f'{PK} {columnType(PK)} NOT NULL'
    # Voeg de primary key toe, maar als die leeg is probeer je de eerste kolom.
    
    # Voege de rest van de kolommen toe
    for column in dataframe.columns:
        if column != PK: # PK bestaat al
            columns += f', {column} {columnType(column)}'
            if column in SK_list:
                foreign_SQL_SK_columns += f', SK_{column} INT'

    surogate_columns = f"{SK} INT IDENTITY(1,1) NOT NULL PRIMARY KEY, Timestamp DATETIME NOT NULL DEFAULT(GETDATE())"

    # Maak de tabel aan
    command = f"CREATE TABLE {tablename} ({surogate_columns}, {columns+foreign_SQL_SK_columns})"

    try:
        cursor.execute(command)
        cursor.commit()
    except pyodbc.Error as e:
        raise(e)


"""
Methode of de dataframe te inserten in de SQL server
"""
def insertTable(tablename, dataframe, PK, SK_list, cursor):
    # Voeg de primary key toe, maar als die leeg is probeer je de eerste kolom.
    SQL_columns = ''
    SQL_SK_columns = ''
    if PK == None:
        PK = dataframe.columns[0]
        
    SQL_columns = PK
        
    # Voeg alle andere kolommen toe
    for column in dataframe.columns:
        # Skip de PK kolom
        if column != PK: 
            # Maak de SK kolommen aan
            if column in SK_list:
                SQL_columns    += f', {column}'
                SQL_SK_columns += f', SK_{column}'
            else:
                SQL_columns    += f', {column}'

    
    # Voer de insert commando uit
    for i, row in dataframe.iterrows():
        values = ''
        SK_values = ''
        values += str(row[PK])

        # Add values
        for column in dataframe.columns:
            if column != PK: # PK is already added
                try:
                    val = str(row[column]).replace("'","''")
                    if val != 'None' and val != np.nan:
                        values += f", '{val}'"
                        if column in SK_list:
                            # NULL refereerd naar de SK van de andere tabel
                            SK_values += ', 0'
                    else:
                        values += f", NULL"
                        if column in SK_list:
                            # NULL refereerd naar de SK van de andere tabel
                            SK_values += ', NULL'
                except AttributeError:
                    values += f", NULL"

        command = f"INSERT INTO {tablename} ({SQL_columns+SQL_SK_columns}) VALUES ({values+SK_values});\n"
        
        cursor.execute(command)
    
    try:
        cursor.commit()
    except pyodbc.Error as e:
        if 'There is already an object named' in str(e):
            print('Table already exists in database')
        else:
            print(command)
            print(e)

"""
Methode om de surrogaatsleutels van een tabel in de SQL-server bij te werken
"""
def updateSurrogate(table, foreign_table, column, foreign_column, cursor):

    command = \
    f"WITH CTE_MostRecent AS ( \
            SELECT \
                SK_{foreign_column}, \
                {foreign_column}, \
                ROW_NUMBER() OVER(PARTITION BY SK_{foreign_column} ORDER BY Timestamp DESC) AS rn \
            FROM \
                {foreign_table}  \
        ) \
        UPDATE t \
        SET t.SK_{column} = f.SK_{foreign_column} \
        FROM {table} t \
        INNER JOIN CTE_MostRecent f ON t.{column} = f.{foreign_column} AND f.rn = 1 \
        WHERE t.SK_{column} = 0;"

    cursor.execute(command)
    cursor.commit()

"""
Methode voor het bijwerken van de lijst met surrogaatsleutels
"""
def updateSurrogates(surrogates, cursor):
    for surrogate in surrogates:
        table = surrogate['table']
        column = surrogate['column']

        try:
            foreign_table = surrogate['foreign_table']
        except KeyError: # foreign_table niet gedefinieerd, neem aan dat hetzelfde is als tabel
            foreign_table = table
        try:
            foreign_column = surrogate['foreign_column']
        except KeyError: # foreign_table niet gedefinieerd, neem aan dat hetzelfde is als tabel
            foreign_column = column

        updateSurrogate(table, foreign_table, column, foreign_column, cursor)

# Tabellen worden gemaakt aan het einde       
etl_tables = []
surrogates = []

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
sizeCheck(product_etl,12)
product_etl

# Create Table en doe het in de lijst.
etl_tables.append({'table_name': 'Product','dataframe': product_etl,'PK': 'PRODUCT_id','SK_columns': []})

# %% [markdown]
# ### Sales_staff

# %%
# Merge
sales_staff_etl = pd.merge(sales_staff, sales_branch, on='SALES_BRANCH_CODE')
sales_staff_etl = pd.merge(sales_staff_etl, country, on='COUNTRY_CODE')
sales_staff_etl = pd.merge(sales_staff_etl, sales_territory, on='SALES_TERRITORY_CODE')

# Voeg de volledige naam toe
sales_staff_etl['FULL_NAME'] = sales_staff_etl['FIRST_NAME'] + ' ' + sales_staff_etl['LAST_NAME']

# Hernoem
sales_staff_etl = sales_staff_etl.rename(columns=json_file)

# Filter
sales_staff_etl = filterColumns(sales_staff_etl)

# Check
sizeCheck(sales_staff_etl,23)
sales_staff_etl

# Create Table en doe het in de lijst.
etl_tables.append({ 'table_name': 'Sales_Staff', 'dataframe': sales_staff_etl, 'PK': 'SALES_STAFF_id', 'SK_columns': ['MANAGER_id'] })

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
etl_tables.append({ 'table_name': 'Satisfaction_Type', 'dataframe': satisfaction_type_etl, 'PK': 'SATISFACTION_TYPE_id', 'SK_columns': [] })

# %% [markdown]
# ### Training

# %%
# Hernoem
training_etl = training.rename(columns=json_file)

# Filter
training_etl = filterColumns(training_etl)

# Check
sizeCheck(training_etl,3)
training_etl

# Create Table en doe het in de lijst.
etl_tables.append({ 'table_name': 'Training', 'dataframe': training_etl, 'PK': None, 'SK_columns': [] })

# %% [markdown]
# ### Satisfaction

# %%
# Hernoem
satisfaction_etl = satisfaction.rename(columns=json_file)

# Filter
satisfaction_etl = filterColumns(training_etl)

# Check
sizeCheck(satisfaction_etl,3)
satisfaction_etl

# Create Table en doe het in de lijst.
etl_tables.append({ 'table_name': 'Satisfaction', 'dataframe': satisfaction_etl, 'PK': None, 'SK_columns': [] })

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
etl_tables.append({ 'table_name': 'Course', 'dataframe': course_etl, 'PK': 'COURSE_id', 'SK_columns': [] })

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
etl_tables.append({
        'table_name': 'Sales_Forecast',
        'dataframe': sales_forecast_etl,
        'PK': 'PRODUCT_id',
        # Broken
        'SK_columns': [] #['PRODUCT_id']
    })

# %% [markdown]
# ### Retailer_contact

# %%
# Merge
retailer_contact_etl = pd.merge(retailer_contact, retailer_site, on='RETAILER_SITE_CODE')
retailer_contact_etl = pd.merge(retailer_contact_etl, country, on='COUNTRY_CODE')
retailer_contact_etl = pd.merge(retailer_contact_etl, sales_territory, on='SALES_TERRITORY_CODE')\

# Voeg de volledige naam toe
retailer_contact_etl['FULL_NAME'] = retailer_contact_etl['FIRST_NAME'] + ' ' + retailer_contact_etl['LAST_NAME']

# Hernoem 
retailer_contact_etl = retailer_contact_etl.rename(columns=json_file)

# Filter
retailer_contact_etl = filterColumns(retailer_contact_etl)

# Check
sizeCheck(retailer_contact_etl,23)
retailer_contact_etl

# Create Table en doe het in de lijst.
etl_tables.append({ 'table_name': 'Retailer_Contact', 'dataframe': retailer_contact_etl, 'PK': 'RETAILER_CONTACT_id', 'SK_columns': ['RETAILER_id'] })

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
etl_tables.append({ 'table_name': 'Retailer', 'dataframe': retailer_etl, 'PK': 'RETAILER_id', 'SK_columns': [] })

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
etl_tables.append({ 'table_name': 'Orders', 'dataframe': order_etl, 'PK': 'ORDER_TABLE_id', 'SK_columns': ['SALES_STAFF_id', 'RETAILER_CONTACT_id'] })

# Voeg de Surrogates toe aan de surrogates lijst
surrogates.append({
    'table': 'Orders',
    'foreign_table': 'Sales_Staff',
    'column': 'SALES_STAFF_id',
}) 
surrogates.append({
    'table': 'Orders',
    'foreign_table': 'Retailer_Contact',
    'column': 'RETAILER_CONTACT_id',
}) 

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
etl_tables.append({ 'table_name': 'Return_Reason', 'dataframe': return_reason_etl, 'PK': 'RETURN_REASON_id', 'SK_columns': [] })

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
etl_tables.append({ 'table_name': 'Returns', 'dataframe': returned_item_etl, 'PK': 'RETURNS_id', 'SK_columns': [] })

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
etl_tables.append({ 'table_name': 'Order_Details', 'dataframe': order_detail_etl, 'PK': 'ORDER_DETAIL_id', 'SK_columns': ['PRODUCT_id'] })

# Voeg de Surrogates toe aan de lijst
surrogates.append({
    'table': 'Order_Details',
    'foreign_table': 'Product',
    'column': 'PRODUCT_id',
}) 

# %% [markdown]
# ### Sales Target

# %%
# Verwijder de RETAILER_NAME kolom, omdat die al in de retailer tabel zit.
sales_target_etl = SALES_TARGETData.drop('RETAILER_NAME', axis=1)

# Hernoem
sales_target_etl = SALES_TARGETData.rename(columns=json_file)
sales_target_etl = sales_target_etl.rename(columns={'Id':'TARGET_id'})

# Filter
sales_target_etl = filterColumns(sales_target_etl)

# Check
sizeCheck(sales_target_etl,5)
sales_target_etl  

# Create Table en doe het in de lijst.
etl_tables.append({ 'table_name': 'Sales_Target', 'dataframe': sales_target_etl, 'PK': 'TARGET_id', 'SK_columns': ['SALES_STAFF_id', 'PRODUCT_id'] })

# Voeg de Surrogates toe aan de lijst
surrogates.append({
    'table': 'Sales_Target',
    'foreign_table': 'Product',
    'column': 'PRODUCT_id',
}) 

surrogates.append({
    'table': 'Sales_Target',
    'foreign_table': 'Sales_Staff',
    'column': 'SALES_STAFF_id',
}) 

# %% [markdown]
# ### Uploaden naar de database.
# 
# Nu gaan we alle data uploaden naar de database om ervoor te zorgen dat we alle data juist in de database hebben geupload. Daarna voegen we de surrogate keys toe aan de database.

# %%
# Nu maken we de tabellen aan
for table in etl_tables:
    print(f"Creating {table['table_name']}")
    createTable(table['table_name'], table['dataframe'], table['PK'], table['SK_columns'], cursor)
    insertTable(table['table_name'], table['dataframe'], table['PK'], table['SK_columns'], cursor)
    print(f"Inserted {table['table_name']}")

# Update surrogates, zodat de surrogaatsleutels worden bijgewerkt in de SQL server
updateSurrogates(surrogates, cursor)

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



