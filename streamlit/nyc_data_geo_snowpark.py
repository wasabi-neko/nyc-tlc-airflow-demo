import folium.plugins
import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap

from snowflake.snowpark.functions import col, year
import snowflake.snowpark as snowpark

# TODO: aggregate in snowpark and count intensity

# Snowflake Snowpark connection parameters
connection_parameters = {
    "account": "XEWDEPD-WD83355",
    "user": "WASABINEKO",
    "password": "Hc>zam-X7LCjYT2",
    "warehouse": "COMPUTE_LARGE",
    "database": "NYC_TLC",
    "schema": "PUBLIC"
}

session = snowpark.Session.builder.configs(connection_parameters).create()

@st.cache_resource
def get_geo_data_by_year(_year, sample_size=10000):
    df = session.table("EX_FINAL")
    df = df.filter(year(col('PICKUP_DATETIME')) == _year)
    # df = df.select('START_LAT', 'START_LON', 'END_LAT', 'END_LON')
    df = df.sample(n=sample_size)
    return df.to_pandas()


# @st.cache_data
def draw_heatmap(df, lat_key, lon_key):
    # Initialize a Folium map centered on New York City

    # Prepare data for HeatMap
    heat_data = df[[lat_key, lon_key]].values.tolist()
    map_center = [40.7128, -74.0060]  # NYC center
    m = folium.Map(location=map_center, zoom_start=10)

    # remove last heatmap layer
    for layer in m._children.values():
        if isinstance(layer, folium.plugins.HeatMap):
            m._children.pop(layer._name)

    # Add HeatMap layer
    HeatMap(heat_data, radius=15, blur=10, min_opacity=0.3).add_to(m)
    return m

with st.form('fliter'):
    PICKUP = "pickup"
    DROPOFF = "dropoff"
    years = range(2009, 2024)
    select_col = st.selectbox('Select pick/dropoff heatmap', (PICKUP, DROPOFF))
    select_year = st.selectbox('Select year', years)
    sample_size = st.slider('Select Sample Size', min_value=1000, max_value=10000, step=10, value=10000)

    submit = st.form_submit_button('submit')

if submit:
    st.session_state.df = get_geo_data_by_year(int(select_year), int(sample_size))

if 'df' in st.session_state:
    if select_col == PICKUP:
        lat_key = 'START_LAT'
        lon_key = 'START_LON'
    else:
        lat_key = 'END_LAT'
        lon_key = 'END_LON'
    st.write(st.session_state.df.head(100))
    st.session_state.m = draw_heatmap(st.session_state.df, lat_key, lon_key)
    st_folium(st.session_state.m, width=725)
else:
    st.header('press submit')

