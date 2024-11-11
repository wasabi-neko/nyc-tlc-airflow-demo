import numpy as np
import streamlit as st
import pandas as pd
import snowflake.connector
import json
import matplotlib.pyplot as plt

"""
# Compare the executing time of two diffrent loading method
"""

"""
## Loading data
"""

query = """
select
    count(QUERY_ID),
    try_parse_json(query_tag)['type']::string as query_type,
    -- SUM(total_elapsed_time) as sum_total_elaspe_time_ms,
    timediff(second, min(start_time), MAX(end_time)) as real_duration_sec,
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp,
    MIN(start_time),
    MAX(end_time),
from
    snowflake.account_usage.query_history
where 
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-demo-compare-v2' and
    execution_status = 'SUCCESS'

GROUP BY
    try_parse_json(query_tag)['dag_timestamp'],
    try_parse_json(query_tag)['type']::string
ORDER BY
    dag_timestamp desc;
"""

st.code(query)


with open('../secrects.json') as file:
    config = json.load(file)
    account = config.get("SNOWFLAKE_ACCOUNT")
    passwd = config.get("SNOWFLAKE_PASS")

"""
## Data Frame
"""

conn = snowflake.connector.connect(
    user = 'WASABINEKO',
    password = passwd,
    account = account,    # don't use `.`, use `-` as separator instead :angy:
    login_timeout = 60
)
df = pd.read_sql(query, conn)
conn.close()
st.write(df)


"""
## Transform and Visualization
"""

df = df[df['QUERY_TYPE'] != 'other']
pivot_df = df.pivot(index='DAG_TIMESTAMP', columns='QUERY_TYPE', values='REAL_DURATION_SEC')
st.write(pivot_df)


fig, ax = plt.subplots()
x = np.arange(len(pivot_df.index))
width = 0.35
ax.bar(x - width/2, pivot_df['internal_stage'], width, label='internal_stage', color='skyblue')
ax.bar(x + width/2, pivot_df['external_stage'], width, label='external_stage', color='salmon')

# Adding labels and title
ax.set_xlabel('Timestamp')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Execution Time Comparison of Methods Over Time')
ax.set_xticks(x)
ax.set_xticklabels(pivot_df.index.strftime('%Y-%m-%d %H:%M'), rotation=45)
ax.legend()

# Display plot in Streamlit
st.pyplot(fig)