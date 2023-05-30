import psycopg2
import os
from tqdm import tqdm

def executeFiles(db, user, password, schemaName, path, host, port):
    # Connection to source database and create a cursor.
    try:
        con = psycopg2.connect(
            dbname=db,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = con.cursor()
    except:
        print('Database connection ERROR !')
        exit()

    # Generator function to iterate through all .sql files in the directory structure
    def get_sql_files(dir_path):
        for dirpath, dirnames, filenames in os.walk(dir_path):
            for filename in sorted(filenames): #Sort filenames alphabetically
                if filename.endswith('.sql'):
                    yield os.path.join(dirpath, filename)

    # Iterate through all .sql files and execute them
    log_files = []
    for file in get_sql_files(path):
        try:
            with open(file, 'r') as f:
                cur.execute(f.read().replace('SCHEMANAME','prasis_angdembe'))
                print(f'{file} executed.')
                log_files.append((file, 'ok', ''))
                con.commit()
        except Exception as e:
            print(f'{file} not executed')
            log_files.append((file, 'failed', str(e).replace('\n', ' ')))
            break


    # Generate a log table 
    log_table = '''
    CREATE TABLE  IF NOT EXISTS prasis_angdembe.log_table(
    file_name VARCHAR(100),
    status VARCHAR(10),
    error_message varchar(1000) NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO prasis_angdembe.log_table (file_name, status, error_message) VALUES {}
    '''.format(str(log_files).strip('[]'))


    cur.execute(log_table)

    con.commit()
    cur.close()
    con.close()

if __name__ == "__main__":
    all_info = {
        "db": "dev_data",
        "user": "prasis_angdembe",
        "password": "xpJ2QZ",
        "schemaName": "prasis_angdembe",
        "path": "/Users/pangdembe/Desktop/master_tbl",
        "host": "54.226.9.116",
        "port": "5433"
    }

    # Function to execute the .sql files
    executeFiles(all_info["db"], all_info["user"],
                 all_info["password"], all_info["schemaName"], all_info["path"], all_info["host"], all_info["port"])

    print('Completed in Execution !')
