import psycopg2

# Database connection details
dbname = 'risk_modeler_dev'
user = 'prasis_angdembe'
password = 'xpJ2QZ'
host = '34.194.238.112'
port = '5678'


schema_name = 'nemo_dev'
excluded_tables=['grant_users','log_table','nemo_pcp_detail']

#db connection
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
cur = conn.cursor()

#fetching all the tables inside schema
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s
""", (schema_name,))

tables = cur.fetchall()
print(tables)

#dropping tables

for table in tables:
    table_name = table[0]
    if table_name not in excluded_tables:
        cur.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name}')

#drop procedure



conn.commit()
conn.close()

print(f"All tables from the schema '{schema_name}' have been deleted.")
