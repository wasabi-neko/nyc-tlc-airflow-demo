# conn_uri_gen.py
# Connection URI generator script
# It will generates the connection_uri for airflow using "secrects.json"

import os, json
from airflow.models.connection import Connection

with open('../secrects.json') as file:
    config = json.load(file)
    aws_access_key = config.get("AWS_ACCESS_KEY")
    aws_secret = config.get("AWS_SECRET_KEY")
    snow_user = config.get("SNOWFLAKE_USER")
    snow_pass = config.get("SNOWFLAKE_PASS")
    snow_account = config.get("SNOWFLAKE_ACCOUNT")

aws_conn = Connection(
    conn_id="aws_default",
    conn_type="aws",
    login=aws_access_key,  # Reference to AWS Access Key ID
    password=aws_secret,  # Reference to AWS Secret Access Key
)

snow_conn = Connection(
    conn_id="snowflake_default",
    conn_type="snowflake",
    login=snow_user,  
    password=snow_pass,  
    schema="public",
    extra= {
        "account": snow_account,
        "warehouse": "COMPUTE_WH",
        "database": "test_db",
    }
)


def gen_uri(conn):
    # Generate Environment Variable Name and Connection URI
    env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
    conn_uri = conn.get_uri()
    print(f"{env_key}={conn_uri}")

    os.environ[env_key] = conn_uri
    print(conn.test_connection())  # Validate connection credentials.
    print('')

gen_uri(aws_conn)
gen_uri(snow_conn)