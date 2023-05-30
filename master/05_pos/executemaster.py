import psycopg2
import os
import glob
from tqdm import tqdm
import getpass
import config


def execute_file(con, file, schema_name):
    try:
        with open(file, 'r') as f:
            query = f.read().replace('SCHEMANAME', schema_name)
            cur = con.cursor()  # create new cursor
            cur.execute(query)
            print(f'{file} executed.')
            cur.close()  # close cursor after executing query
            return file, 'ok', ''
    except Exception as e:
        print(f'{file} not executed')
        return file, 'failed', str(e).replace('\n', ' ')

def execute_files(db, user, password, schema_name, path, host, port):
    # Connect to the source database and create a cursor.
    try:
        con = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except:
        print('Database connection ERROR !')
        exit()
               
    # Create log files list
    log_files = []
    
    # Define generator function to yield SQL files one at a time
    def sql_files_generator(dir_path):
        for filename in sorted(glob.glob(f'{dir_path}/**/*.sql', recursive=True)):
            if os.path.isfile(filename):
                yield filename
    
    # Loop through SQL files and execute them one at a time
    for file in sql_files_generator(path):
        log_files.append(execute_file(con, file, schema_name))
        con.commit()

    # Create log table and insert log files
    log_table = '''
        CREATE TABLE IF NOT EXISTS {}.log_table(
        file_name VARCHAR(100),
        status VARCHAR(10),
        error_message VARCHAR(1000) NULL,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO {}.log_table (file_name, status, error_message) VALUES {}
        '''.format(schema_name, schema_name, str(log_files).strip('[]'))

    cur = con.cursor()
    cur.execute(log_table)
    con.commit()
    cur.close()
    con.close()

if __name__ == "__main__":
    all_info = {
        "db": config.db,
        "user": config.user,
        "password": config.password,
        "schema_name": config.schema_name,
        "path": os.getcwd(),
        "host": config.host,
        "port":config.port
    }

    # Access and print the current folder path from the all_info dictionary
    print("Path: ", all_info["path"])

    # Function to execute the .sql files
    execute_files(
        all_info["db"],
        all_info["user"],
        all_info["password"],
        all_info["schema_name"],
        all_info["path"],
        all_info["host"],
        all_info["port"]
    )
    print('Execution completed!')