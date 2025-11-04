import os
import duckdb
import folium
import streamlit as st
from streamlit.folium import st_folium
from dotenv import load_dotenv

load_dotenv()


st.title("🚲 London TfL Bike Point Availability")

# Connect to DuckDB and fetch data
@st.cache_data
def load_data():
    conn = duckdb.connect(os.getenv("DBT_DUCKDB_PATH"))
    df = conn.execute("""
        SELECT 
            bike_point_id,
            common_name,
            lat,
            lon,
            avg_bikes_available,
            avg_availability_pct,
            availability_status
        FROM 
            marts.tfl__bike_point_availability_snapshot
    """).df()
    conn.close()
    return df

df = load_data()

st.write(f"Showing {len(df)} bike points")

# Create base map centred on London
map = folium.Map(
    location=[51.5074, -0.1278],
    zoom_start=12
)

# Add markers for each bike point
for i, row in df.iterrows():
    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=5,
        popup=row['common_name'],
        tooltip=f"{row['common_name']}: {row['avg_bikes_available']} bikes",
        color='blue',
        fill=True
    ).add_to(map)

# Display map in Streamlit
st_folium(map, width=700, height=600)