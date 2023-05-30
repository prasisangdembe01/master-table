import json
import sys
sys.path.append('/Users/pangdembe/opt/anaconda3/lib/python3.9/site-packages')
sys.path.append('/Users/pangdembe/opt/anaconda3/lib/python3.9/site-packages/numpy')
sys.path.append('/Users/pangdembe/opt/anaconda3/lib/python3.9/site-packages (2.4.0)')
import pandas as pd
from smart_open import smart_open
import numpy as np
import psycopg2
import datetime
import re
import os
import glob
import csv



params = {
    'dbname': 'dev_data',
    'host': '54.226.9.116',
    'port': '5433',
    'user': 'prasis_angdembe',
    'password': 'xpJ2QZ'
}

# conn = psycopg2.connect(**params)

def generate_table_names(file_path):
    table_names = []   
    if os.path.isfile(file_path) and file_path.endswith('.csv'):
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        table_name = file_name.replace(' ', '').replace('-', '')
        table_names.append({"file_name_path": file_path, "Table_Name": table_name})
    elif os.path.isdir(file_path):
        for root, dirs, files in os.walk(file_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    file_name = os.path.splitext(os.path.basename(file_path))[0]
                    table_name = file_name.replace(' ', '_').replace('-', '_')
                    table_names.append({"file_name_path": file_path, "Table_Name": table_name})
    else:
        print(f"{file_path} is not a CSV file or directory.")
        return None
    table_names_df = pd.DataFrame(table_names)
    return table_names_df

def get_delimiters_in_file(directory):
    list_of_possible_delimiters = [';', '|', '\t', ',', ' ', '*']
    delimiter_dict = {}
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.csv'):
                file_name_path = os.path.join(dirpath, filename)
                if os.path.isfile(file_name_path): # check if file_name_path is a file
                    with smart_open(file_name_path, 'r') as file:
                        first_line = file.readline()
                        for delimiter in list_of_possible_delimiters:
                            if delimiter in first_line:
                                delimiter_dict[os.path.join(dirpath, filename)] = delimiter
                                break
                    file.close()
    return delimiter_dict

def get_column_with_replaced_char(file_name_path, delimiter_dict):
    list_of_characters_to_replace = '|'.join(['\-','\ ', '\&', '\*', '/'])
    df_dict = {}
    for root, dirs, files in os.walk(file_name_path):
        for file_name in files:
            if file_name.endswith('.csv'):
                file_path = os.path.join(root, file_name)
                print(f"Processing file: {file_path}")
                try:
                    df = pd.read_csv(file_path, sep=delimiter_dict[file_path], dtype='object')
                except Exception as e:
                    print(f"Error reading file {file_path}: {str(e)}")
                    continue
                print("Before replace")
                print(df.columns)
                df.columns = df.columns.str.replace(list_of_characters_to_replace, "_", regex=True).str.lower()
                print("After replace")
                print(df.columns)
                table_name = os.path.splitext(os.path.basename(file_path))[0]
                df_dict[file_path] = df.columns.tolist()
    if not df_dict:
        print("No data frames found.")
        return {}
    print("Getting column with replaced characters.")
    return {file_path: col_names for file_path, col_names in df_dict.items()}

def get_max_length_of_each_col(file_name_path, delimiter, column_with_replaced_char):
    # Get the delimiter value from the delimiter argument
    if isinstance(delimiter, dict):
        delimiter = delimiter.get('delimiter', ',')
    result_dict = {}
    for dirpath, dirnames, filenames in os.walk(file_name_path):
        column_name_with_length = {}
        for file_name in filenames:
            if file_name.endswith('.csv'):
                file_path = os.path.join(dirpath, file_name)
                print(f"Getting max length of all columns for file {file_path}")
                measurer = np.vectorize(len)
                read_file = pd.read_csv(file_path, sep=delimiter, dtype='object', engine='python').head(300000)
                col_len = dict(zip(column_with_replaced_char[file_path], measurer(read_file.values.astype(str)).max(axis=0)))
                if file_path not in column_name_with_length:
                    column_name_with_length[file_path] = {}
                column_name_with_length[file_path].update(col_len)
        for file_path, columns in column_name_with_length.items():
            file_columns = {}
            for key, value in columns.items():
                if 1 <= int(value) <= 5:
                    file_columns[key] = 10
                else:
                    file_columns[key] = value + round(50 / 100 * value)
            result_dict[file_path] = file_columns
    return result_dict


def write_ddl(column_name_with_length, table_names_df):
    create_queries = []
    for i, row in table_names_df.iterrows():
        table_name = row['Table_Name']
        create_query = f'CREATE TABLE {table_name} ( \n'
        columns = column_name_with_length[row['file_name_path']]
        l = len(columns.keys())
        j = 0
        for key, value in columns.items():
            j += 1
            if j < l:
                q = f'\t{key} \t VARCHAR({value}), \n'
            else:
                q = f'\t{key} \t VARCHAR({value}));'
            create_query += q
        create_queries.append(create_query.lower())
    return create_queries

def write_dml(table_names_df, all_columns, delimiter, file_name_path):
    insert_statements = []
    for i, row in table_names_df.iterrows():
        table_name = row['Table_Name']
        values_str = ""
        for root, dirs, files in os.walk(file_name_path):
            for file_name in files:
                if file_name.endswith('.csv') and file_name.startswith(table_name):
                    file_path = os.path.join(root, file_name)
                    if file_path not in all_columns:
                        continue
                    columns = all_columns[file_path]
                    with open(file_path, 'r') as f:
                        reader = csv.reader(f, delimiter=delimiter[file_path])
                        next(reader)  # Skip header row
                        for row in reader:
                            values = [f"'{value}'" for value in row]
                            values_str += f"({', '.join(values)}),"
        if not values_str:
            continue  # Skip tables with no rows to insert
        values_str = values_str[:-1]  # Remove trailing comma
        column_names = [f'"{c}"' for c in columns.keys()]
        column_names_str = ", ".join(column_names)
        insert_statement = f"INSERT INTO {table_name} ({column_names_str}) VALUES {values_str};"
        if values_str and not insert_statements:  # Check if values_str is not empty and insert_statements is empty
            insert_statements.append(insert_statement)
        elif values_str:  # Check if values_str is not empty before appending
            insert_statements.append('\n' + insert_statement)
    return insert_statements



def table_exists(table_names_df):
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    for index, row in table_names_df.iterrows():
        if 'Table_Name' in row and isinstance(row['Table_Name'], str):
            table_name = row['Table_Name'].lower()
            print(table_name)
            cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
            result = cur.fetchone()[0]
            if result:
                cur.execute(f"DROP TABLE {table_name}")
                conn.commit()
                print(f"Table '{table_name}' dropped successfully")
    cur.close()
    conn.close()
    return False


# def table_exists(table_names_df, ddl_statements):
#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()
#     for index, row in table_names_df.iterrows():
#         if 'Table_Name' in row and isinstance(row['Table_Name'], str):
#             table_name = row['Table_Name'].lower()
#             print(table_name)
#             cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
#             result = cur.fetchone()[0]
#             if result:
#                 cur.execute(f"DROP TABLE {table_name}")
#                 conn.commit()
#                 print(f"Table '{table_name}' dropped successfully")
#             else:
#                 cur.execute(ddl_statements[table_name])
#                 conn.commit()
#                 print(f"Table '{table_name}' created successfully")
#     cur.close()
#     conn.close()
#     return False

# def table_exists(table_names_df, ddl_statements):
#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()
#     for index, row in table_names_df.iterrows():
#         if 'Table_Name' in row and isinstance(row['Table_Name'], str):
#             table_name = row['Table_Name'].lower()
#             print(table_name)
#             cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
#             result = cur.fetchone()[0]
#             if result:
#                 cur.execute(f"DROP TABLE {table_name}")
#                 conn.commit()
#                 print(f"Table '{table_name}' dropped successfully")
            
#             cur.execute(ddl_statements[table_name])
#             conn.commit()
#             print(f"Table '{table_name}' created successfully")
            
#     cur.close()
#     conn.close()
# def table_exists(table_names_df,all_columns):
#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()
#     for index, row in table_names_df.iterrows():
#         if 'Table_Name' in row and isinstance(row['Table_Name'], str):
#             table_name = row['Table_Name'].lower()
#             print(table_name)
#             cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
#             result = cur.fetchone()[0]
#             if result:
#                 cur.execute(f"DROP TABLE {table_name}")
#                 conn.commit()
#                 print(f"Table '{table_name}' dropped successfully")
#             else:
#                 create_queries = write_ddl(all_columns, table_names_df)
#                 cur.execute(create_queries[index])
#                 conn.commit()
#                 print(f"Table '{table_name}' created successfully")
#     cur.close()
#     conn.close()

# def table_exists(table_names_df, all_columns):
#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()
#     for index, row in table_names_df.iterrows():
#         if 'Table_Name' in row and isinstance(row['Table_Name'], str):
#             table_name = row['Table_Name'].lower()
#             print(table_name)
#             cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
#             result = cur.fetchone()[0]
#             if result:
#                 cur.execute(f"DROP TABLE {table_name}")
#                 conn.commit()
#                 print(f"Table '{table_name}' dropped successfully")
#             create_query = write_ddl(all_columns, table_name)
#             cur.execute(create_query)
#             conn.commit()
#             print(f"Table '{table_name}' created successfully")
#     cur.close()
#     conn.close()

# def table_exists(table_names_df, all_columns):
#     conn = psycopg2.connect(**params)
#     cur = conn.cursor()
#     for index, row in table_names_df.iterrows():
#         if 'Table_Name' in row and isinstance(row['Table_Name'], str):
#             table_name = row['Table_Name'].lower()
#             print(table_name)
#             cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s)", [table_name])
#             result = cur.fetchone()[0]
#             if result:
#                 cur.execute(f"DROP TABLE {table_name}")
#                 conn.commit()
#                 print(f"Table '{table_name}' dropped successfully")
#             else:
#                 create_query = write_ddl(all_columns,table_names_df)
#                 cur.execute(create_query)
#                 conn.commit()
#                 print(f"Table '{table_name}' created successfully")
#     cur.close()
#     conn.close()


def create_table_statement(create_query):
    conn = psycopg2.connect(**params)
    print("database connected successfully !!!")
    cur = conn.cursor()
    for query in create_query:
        cur.execute(''.join(query))
        conn.commit()
    cur.close()
    conn.close()

def copy_values_into_table(copy_statements):
    conn = psycopg2.connect(**params)
    print("Inserting into tables")
    cur = conn.cursor()   
    for copy_query in copy_statements:
        cur.execute(copy_query)
        conn.commit()    
    cur.close()
    conn.close()

def main():
    file_name_path =os.getcwd() #current directory
    print(f'Local file path is {file_name_path}')
    delimiter = get_delimiters_in_file(file_name_path)
    print(f'The delimiter of file {file_name_path} is {delimiter}.\n')
    print(delimiter)
    table_names_df = generate_table_names(file_name_path)
    print(table_names_df)
    # print('Getting column with replaced characters.')
    column_with_replaced_char = get_column_with_replaced_char(file_name_path, delimiter)
    print(column_with_replaced_char)
    print('Replacing unwanted character completed.\n')
    print('Getting max length of all columns.')
    print(f'Getting max length of all columns for file {file_name_path}')
    all_columns=get_max_length_of_each_col(file_name_path, delimiter, column_with_replaced_char)
    print('\nMaximum column lengths:')
    print(all_columns)
    print('Generating DDL statements.')
    if (table_exists(table_names_df)==False):
        print('Generating DDL statements.')
        create_queries = write_ddl( all_columns, table_names_df)
        print(create_queries)
        print(f'Creating table: "{table_names_df}".')
        try:
            create_table_statement(create_queries)
            print(f"Table {table_names_df} created succesfully\n")
        except (Exception, psycopg2.DatabaseError) as error:
            print("ERROR: ", error)
    print('Generating DML statements.')
    copy_query = write_dml(table_names_df, all_columns, delimiter, file_name_path)
    print(copy_query)
    # print(copy_query)
    print(f'copying data of file "{file_name_path}" into table: "{table_names_df}".')
    try:
        copy_values_into_table(copy_query)
        print(f'Data for table "{table_names_df}" have been inserted successfully.\n\n')
    except (Exception, psycopg2.DatabaseError) as error:
        print("ERROR: ", error)


if __name__ == "__main__":
    main()