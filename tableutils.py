import pandas as pd
import json
import sqlite3
import pyodbc
import os
from settings import Settings

"""
Zorg dat de connectie met SQLIte goed is.
"""
def getSqlite(settings: Settings, filename):
    path = os.path.join(settings.data_dir, filename)
    return sqlite3.connect(path)

"""
Lees data uit een CSV-bestand en retourneer een pandas DataFrame
"""
def getCSV(settings: Settings, filename):
    path = os.path.join(settings.data_dir, filename)
    return pd.read_csv(path)

"""
Flexibele methode om twee tabellen samen te voegen
- NaN-waarden van het ene dataframe kunnen worden gevuld door het andere dataframe
- Gebruikt alle beschikbare kolommen
- Fouten wanneer een rij van de twee dataframes niet overeenkomt (df1 heeft 'A' en df2 heeft 'B' in rij)
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

        # Gebruik waarden uit df2 waarbij df1 nul is (prioriteit voor df1-waarden)
        df1_combined[col] = series1.combine_first(series2)

    return df1_combined


# Filter de kolommen van de dataframes, door alleen de kolommen te houden die in de json file staan.
def filterColumns(dataframe):
    rename_mapping = {}

    with open('renames.json') as f_in:
        rename_mapping = json.load(f_in)

    # List of all vetted columns
    valid_columns = set(rename_mapping.values())

    valid_columns_set = set(valid_columns)
    actual_columns_set = set(dataframe.columns)
    intersection_columns = list(actual_columns_set.intersection(valid_columns_set))

    # Use the intersection result to filter columns from dataframe
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


"""
Pak het laatste stukje van een touwtje
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
Methode om een ​​tabel in de SQL-server te maken
"""
def createTable(tablename, dataframe, PK):
    SK = ''
    if PK == None:
        SK = f'SK_{tablename}'
        columns = ''
    else:
        SK = f'SK_{PK}'
        columns = f'{PK} {columnType(PK)} NOT NULL'
    # Voeg primary key toe als eerste kolom
    
    # Voeg alle andere kolommen toe
    for column in dataframe.columns:
        if column != PK: # PK is al toegevoegd
            columns += f', {column} {columnType(column)}'

    surogate_columns = f"{SK} INT IDENTITY(1,1) NOT NULL PRIMARY KEY, Timestamp DATETIME NOT NULL DEFAULT(GETDATE())"

    # Maak de command en execute het.
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
Methode om dataframegegevens in de SQL-server in te voegen
"""
def insertTable(tablename, dataframe, PK):
    # Voeg primary key toe als eerste kolom
    columns = PK
    
    # Voeg alle andere kolommen toe
    for column in dataframe.columns:
        if column != PK: # PK is al toegevoegd
            columns += f', {column}'
    
    # Doe de inserts
    for i, row in dataframe.iterrows():
        values = ''
        values += str(row[PK])

        for column in dataframe.columns:
            if column != PK: # PK is al toegevoegd
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