import dagster as dg
import psycopg2, os



@dg.resource
def neon_postgres_resource(context):
    conn = psycopg2.connect(os.getenv("NEON_POSTGRES_DB_URL"))
    conn.autocommit = True
    yield conn
    conn.close()