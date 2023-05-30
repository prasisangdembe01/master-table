import psycopg2
import os
import glob
from tqdm import tqdm
import getpass
import gc
import sys
sys.path.append('/Users/pangdembe/opt/anaconda3/lib/python3.9/site-packages')
from memory_profiler import profile
import time
import config
# from config import schema_name



@profile
def execute_file(con, file, schema_name):
    try:
        with open(file, 'r') as f:
            query = f.read().replace('events', schema_name)
            with con.cursor() as cur:
                cur.execute(query)
                # Create log table and insert log record
                log_table = f'''
                    INSERT INTO {schema_name}.log_table (file_name, status) VALUES ('{os.path.basename(file)}', 'ok')
                    '''
                cur.execute(log_table)
                # cur.commit()
                print(f'{file} executed.')
            del cur
            gc.collect()
            return file, 'ok', ''
    except Exception as e:
        print(f'{file} not executed')
        return file, 'failed', str(e).replace('\n', ' ')


def execute_files(db, user, password, schema_name, path, host, port, delay=5):
    # Connect to the source database and create a cursor.
    try:
        con = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port
        )
        con.autocommit = True
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
    cur = con.cursor()
    sql = f'''
                    CREATE TABLE IF NOT EXISTS {schema_name}.log_table(
                    file_name VARCHAR(100),
                    status VARCHAR(10),
                    error_message VARCHAR(1000) NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );'''
    cur.execute(sql)
    # cur.commit()
    cur.close()
    for file in sql_files_generator(path):
        log_files.append(execute_file(con, file, schema_name))
        time.sleep(delay)

    con.close()
    print('Execution completed!')
    return log_files

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
        all_info["port"],
        delay=5
    )
    # print('Execution completed!')
