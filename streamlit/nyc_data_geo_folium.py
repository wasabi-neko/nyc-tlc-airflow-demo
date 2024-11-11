import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap
import pandas as pd

import json
import snowflake.connector

query = """
SELECT
START_LAT,
START_LON,
END_LAT,
END_LON
FROM NYC_TLC.PUBLIC.EX_FINAL
WHERE YEAR(PICKUP_DATETIME) = 2019;
"""

with open('../secrects.json') as file:
    config = json.load(file)
    account = config.get("SNOWFLAKE_ACCOUNT")
    passwd = config.get("SNOWFLAKE_PASS")

conn = snowflake.connector.connect(
    user = 'WASABINEKO',
    warehouse = 'COMPUTE_LARGE',
    password = passwd,
    account = account,    # don't use `.`, use `-` as separator instead :angy:
    login_timeout = 60
)
df = pd.read_sql(query, conn)
conn.close()
st.write(df)    # print data frame raw


@st.cache_data
def draw_heatmap(df, lat_key, lon_key):
    # Initialize a Folium map centered on New York City
    m = folium.Map(location=[df[lat_key].mean(), df[lon_key].mean()], zoom_start=12)

    # Prepare data for HeatMap
    heat_data = df[[lat_key, lon_key]].values.tolist()

    # Add HeatMap layer
    HeatMap(heat_data).add_to(m)
    return m


PICKUP = "pickup"
DROPOFF = "dropoff"
select_col = st.selectbox('Select pick/dropoff heatmap', (PICKUP, DROPOFF))

if select_col:
    with st.spinner('loading'):
        if select_col == PICKUP:
            lat_key = 'START_LAT'
            lon_key = 'START_LON'
        else:
            lat_key = 'END_LAT'
            lon_key = 'END_LON'
        m = draw_heatmap(df, lat_key, lon_key)
        st_data = st_folium(m, width=725)