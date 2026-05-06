import psycopg2

def run_sql_file(conn_params, sql_path):
    with open(sql_path, "r") as f:
        query = f.read()
        
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    