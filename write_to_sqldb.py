import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os 
from sqlalchemy import create_engine
from subprocess import call
import usa_spending_api

load_dotenv("./env_info/.env")

hostname = 'localhost'
database = 'financial'
username = os.getenv('postgres_user')
pwd = os.getenv('postgresql_pwd')
port_id = os.getenv('postgresql_port')

try:
    conn = psycopg2.connect(host=hostname, database=database, user=username, password=pwd, port=port_id)
    print("Connected to database.")

    cur = conn.cursor()

    # insert pandas dataframe into postgresql database
    engine = create_engine(f'postgresql+psycopg2://{username}:{pwd}@{hostname}:{port_id}/{database}')
    print(engine)

    agency_aid_df = usa_spending_api.api_pull.agency_aid()
    agency_aid_df.to_sql('agency_aid', engine, if_exists='append', index=False)

    # write postgresql query to drop duplicates from agency_aid table
    drop_duplicates = """
    DELETE FROM agency_aid
    WHERE aid NOT IN (
        SELECT aid, max(update_time) as max_date
        FROM agency_aid
        GROUP BY aid
        );
    """
    print("Agency AID Reloaded.")

except Exception as error:
    print(error)
except operationalError as error2:
    print("Check database name, username, and password: ", error2)
finally:
    if cur is not None:
        cur.close()
        print("Cursor closed.")
    if conn is not None:
        conn.close()
        print("Database connection closed.")
