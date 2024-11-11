import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import contextily as ctx  # For map tiles

# Snowflake Snowpark connection parameters
connection_parameters = {
    "account": "XEWDEPD-WD83355",
    "user": "WASABINEKO",
    "password": "Hc>zam-X7LCjYT2",
    "warehouse": "COMPUTE_LARGE",
    "database": "NYC_TLC",
    "schema": "PUBLIC"
}

# Initialize Snowflake session
session = Session.builder.configs(connection_parameters).create()

# Fetch data from Snowflake with Snowpark
pickup_dropoff_query = session.table("EX_FINAL").select(
    col("START_LON"), 
    col("START_LAT"), 
).limit(100).to_pandas()

# Close Snowflake session
session.close()

# Concatenate pickup and dropoff locations for heatmap
pickup_locations = pickup_dropoff_query[['START_LON', 'START_LAT']]
pickup_locations.columns = ['longitude', 'latitude']

st.write(pickup_locations)


# Plotting the heatmap
fig, ax = plt.subplots(figsize=(10, 10))
sns.kdeplot(
    data=pickup_locations,
    x="longitude",
    y="latitude",
    fill=True,
    cmap="viridis",
    thresh=0.1,
    levels=100,
    ax=ax
)

# Adding the map tiles from contextily with CartoDB Positron
# ctx.add_basemap(ax, zoom=12, crs="EPSG:4326", alpha=0.5)
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)

# Set limits to NYC bounding box
# ax.set_xlim(-74.05, -73.75)
# ax.set_ylim(40.63, 40.85)

# Display plot in Streamlit
st.pyplot(fig)