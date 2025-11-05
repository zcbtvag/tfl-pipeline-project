import os
import duckdb
import folium
import streamlit as st
import plotly.graph_objects as go
from streamlit_folium import st_folium
from dotenv import load_dotenv

load_dotenv()


st.title("🚲 Live London TfL Bike Point Availability")

# Connect to DuckDB and fetch data
@st.cache_data
def load_data():
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"), read_only=True)
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
            main_marts.tfl__bike_point_availability_snapshot
    """).df()
    conn.close()
    return df

@st.cache_data
def load_timeseries_data():
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"), read_only=True)
    df = conn.execute("""
        SELECT
            bike_point_id,
            common_name,
            hour_timestamp,
            avg_bikes_available,
            avg_availability_pct
        FROM
            main_marts.fct_tfl__bike_points
        ORDER BY bike_point_id, hour_timestamp
    """).df()
    conn.close()
    return df

df = load_data()

st.info(f"Click on any of the {len(df)} bike point markers on the map below to see the 24-hour availability trend")

# Create base map centred on London
map = folium.Map(
    location=[51.5074, -0.1278],
    zoom_start=12
)

# Add markers for each bike point
for i, row in df.iterrows():
    tooltip_html = f"""
        <b>{row['common_name']}</b><br>
        ID: {row['bike_point_id']}<br>
        Bikes Available: {row['avg_bikes_available']}<br>
        Availability: {row['avg_availability_pct']}%<br>
        Status: {row['availability_status']}
    """

    if row['availability_status'] == 'High':
        colour='green'
    elif row['availability_status'] == 'Medium':
        colour='orange'
    else:
        colour='red'

    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=5,
        tooltip=folium.Tooltip(tooltip_html),
        color=colour,
        fill=True,
        fill_color=colour
    ).add_to(map)

# Display map in Streamlit and capture click events
map_data = st_folium(map, width=700, height=600)

# Show chart when a marker is clicked
if map_data and map_data.get('last_object_clicked'):
    clicked_lat = map_data['last_object_clicked']['lat']
    clicked_lon = map_data['last_object_clicked']['lng']

    # Find the bike point that was clicked
    clicked_point = df[
        (df['lat'].round(6) == round(clicked_lat, 6)) &
        (df['lon'].round(6) == round(clicked_lon, 6))
    ]

    if not clicked_point.empty:
        bike_point_id = clicked_point.iloc[0]['bike_point_id']
        common_name = clicked_point.iloc[0]['common_name']

        st.subheader(f"📊 {common_name}")

        # Load timeseries data only when needed
        timeseries_df = load_timeseries_data()

        # Filter data for this bike point
        df_point = timeseries_df[timeseries_df['bike_point_id'] == bike_point_id].copy()
        df_point = df_point.sort_values('hour_timestamp')

        if not df_point.empty:
            # Create Plotly chart
            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=df_point['hour_timestamp'],
                y=df_point['avg_bikes_available'],
                name='Availability %',
                line=dict(color='green', width=3),
                mode='lines+markers',
                marker=dict(size=8),
                fill='tozeroy',
                fillcolor='rgba(0, 255, 0, 0.1)'
            ))

            fig.update_layout(
                title=f"24-Hour Trend",
                xaxis=dict(title='Time'),
                yaxis=dict(title='Number of bikes available', range=[0, 100]),
                height=400,
                hovermode='x unified'
            )

            st.plotly_chart(fig, use_container_width=True)

            # Show summary stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Current Availability", f"{clicked_point.iloc[0]['avg_availability_pct']:.1f}%")
            with col2:
                st.metric("Avg (24h)", f"{df_point['avg_availability_pct'].mean():.1f}%")
            with col3:
                st.metric("Current Bikes", f"{clicked_point.iloc[0]['avg_bikes_available']:.0f}")
        else:
            st.info("No historical data available for this bike point yet.")