import os
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import json

from streamlit_autorefresh import st_autorefresh
import base64
from io import BytesIO

st.set_page_config(
    page_title="World Bank Live Indicators",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.markdown("""
    <style>
    /* Minimalist Modern Theme with Pastel Blue, Pastel Brown, White, and Black */
    .main { padding: 2rem; }
    .stApp { background-color: #ffffff; color: #333333; }
    h1 { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; font-size: 2rem; font-weight: 400; color: #333333; margin-bottom: 1rem; }
    h2, h3 { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; font-weight: 400; color: #666666; }
    .stMarkdown { font-size: 1rem; color: #333333; }
    .stButton > button { background-color: #d2b48c; border: none; border-radius: 4px; padding: 0.5rem 1rem; font-weight: 500; color: #333333; transition: background-color 0.2s; }
    .stButton > button:hover { background-color: #c2a47c; }
    .stMetric { background-color: #ffffff; border-radius: 8px; padding: 1rem; border: 1px solid #000000; color: #333333; }
    .stMetric > label { color: #666666 !important; }
    .stSelectbox, .stMultiSelect { background-color: #ffffff; border-radius: 4px; border: 1px solid #000000; color: #333333; }
    .stPlotlyChart { border-radius: 8px; }
    .stDataFrame { background-color: #ffffff; border-radius: 8px; overflow: hidden; color: #333333; }
    .stDataFrame td { background-color: #ffffff !important; color: #333333 !important; border-color: #000000 !important; }
    .sidebar .sidebar-content { background-color: #b3d9ff; color: #333333; }
    section[data-testid="stStatusWidget"] div { background-color: #f5f5f5; color: #333333; }
    </style>
""", unsafe_allow_html=True)

# --- Configuration ---
MONGO_URI = 'mongodb+srv://qclumadac_db_user:123@groceryinventorysystem.lxegof0.mongodb.net/?retryWrites=true&w=majority&appName=GroceryInventorySystem'
DB_NAME = 'worldbank_db'
COLLECTION_RAW = 'indicator_history'

# --- MongoDB ---
@st.cache_resource
def init_mongo():
    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    return client[DB_NAME]

db = init_mongo()
raw_col = db[COLLECTION_RAW]

@st.cache_data(ttl=30)
def get_historical(hours=24):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cursor = raw_col.find({"timestamp": {"$gte": cutoff.isoformat()}}).sort("timestamp", -1)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df = df.drop(columns=['_id'], errors='ignore')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if hasattr(df['timestamp'].dtype, 'tz') and df['timestamp'].dtype.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    return df

@st.cache_data(ttl=10)
def get_live_mongo_data(minutes=10):
    """Get live data: most recent records from MongoDB (last N minutes)."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    cursor = raw_col.find({"timestamp": {"$gte": cutoff.isoformat()}}).sort("timestamp", -1).limit(500)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df = df.drop(columns=['_id'], errors='ignore')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if hasattr(df['timestamp'].dtype, 'tz') and df['timestamp'].dtype.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    return df

def get_available_countries():
    return raw_col.distinct('country_name')

def get_available_indicators():
    return raw_col.distinct('indicator_name')

if 'live_df' not in st.session_state:
    st.session_state.live_df = pd.DataFrame()

# --- Export Helper ---
def export_data(df, fmt='csv'):
    export_df = df.copy()
    for col in export_df.select_dtypes(include=['datetimetz']).columns:
        export_df[col] = export_df[col].dt.tz_localize(None)

    if fmt == 'csv':
        data = export_df.to_csv(index=False)
        mime = 'text/csv'
        ext = 'csv'
    elif fmt == 'excel':
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as w:
            export_df.to_excel(w, index=False)
        data = output.getvalue()
        mime = 'application/vnd.openxmlformats-officerspreadsheetml.sheet'
        ext = 'xlsx'
    else:
        return None
    b64 = base64.b64encode(data.encode() if isinstance(data, str) else data).decode()
    return f'<a href="data:{mime};base64,{b64}" download="worldbank_data.{ext}">Download {ext.upper()}</a>'

# --- UI ---
st.sidebar.markdown("# 🌍 World Bank")
st.sidebar.markdown("**Indicators Dashboard**")
page = st.sidebar.radio("Select View", ["Live Stream", "Historical Analysis"])

if page == "Live Stream":
    st.markdown("# Live Development Indicators")
    st_autorefresh(interval=5000, key="live_refresh")

    df_live = get_live_mongo_data(minutes=10)
    st.session_state.live_df = df_live

    if not df_live.empty:
        st.markdown(f'<div style="background: linear-gradient(135deg, #166534, #15803d); color: white; padding: 1rem; border-radius: 8px; text-align: center; font-weight: 500;">Live data active: {len(df_live)} records (last 10 min)</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="background: #7c2d12; color: #f8fafc; padding: 1rem; border-radius: 8px; text-align: center; font-weight: 500;">Awaiting data... Pipeline updates every 60s.</div>', unsafe_allow_html=True)
        st.info("World Bank data is annual; 'live' shows latest pipeline inserts.")

    if not df_live.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Filter Countries**")
            selected_countries = st.multiselect(
                label="Countries",
                options=sorted(st.session_state.live_df['country_name'].unique()),
                default=sorted(st.session_state.live_df['country_name'].unique())[:3]
            )
        with col2:
            st.markdown("**Filter Indicators**")
            selected_indicators = st.multiselect(
                label="Indicators",
                options=sorted(st.session_state.live_df['indicator_name'].unique()),
                default=sorted(st.session_state.live_df['indicator_name'].unique())[:2]
            )

        df_filtered = st.session_state.live_df.copy()
        if selected_countries:
            df_filtered = df_filtered[df_filtered['country_name'].isin(selected_countries)]
        if selected_indicators:
            df_filtered = df_filtered[df_filtered['indicator_name'].isin(selected_indicators)]
        df_live = df_filtered

        st.markdown("### Latest Values by Indicator")
        latest_by_indicator = df_live.sort_values('timestamp').groupby(['country_name', 'indicator_name']).last().reset_index()

        for indicator in selected_indicators:
            st.markdown(f"**{indicator}**")
            cols = st.columns(4)
            for i, country in enumerate(selected_countries[:4]):
                country_data = latest_by_indicator[(latest_by_indicator['country_name'] == country) & (latest_by_indicator['indicator_name'] == indicator)]
                if not country_data.empty:
                    val = country_data.iloc[0]['value']
                    year = country_data.iloc[0]['year']
                    with cols[i]:
                        st.metric(country, f"{val:,.0f}" if val > 100 else f"{val:,.2f}", delta=f"Year: {year}")

        st.markdown("### Cross-Country Comparison")
        if len(selected_indicators) > 0:
            fig_bar = px.bar(
                latest_by_indicator,
                x='country_name',
                y='value',
                color='indicator_name',
                barmode='group',
                title="Latest Indicator Values by Country"
            )
            fig_bar.update_layout(template='plotly_white')
            st.plotly_chart(fig_bar, width="stretch")

        if df_live['timestamp'].nunique() > 1:
            st.markdown("### Recent Updates")
            fig_line = px.line(
                df_live.sort_values('timestamp'),
                x='timestamp',
                y='value',
                color='country_name',
                line_dash='indicator_name',
                title="Values Over Time"
            )
            fig_line.update_layout(template='plotly_white')
            st.plotly_chart(fig_line, width="stretch")

        st.markdown("### Recent Records")
        st.dataframe(df_live.sort_values('timestamp', ascending=False).head(20), width="stretch")
    else:
        st.info("No live data yet. Ensure the producer is running and has fetched data.")

elif page == "Historical Analysis":
    st.markdown("# Historical Indicator Analysis")

    col1, col2 = st.columns(2)
    with col1:
        hours = st.slider("Data Range", 1, 168, 24)
    df_hist = get_historical(hours)

    if not df_hist.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Countries**")
            hist_countries = st.multiselect(
                label="Countries",
                options=sorted(df_hist['country_name'].unique()),
                default=sorted(df_hist['country_name'].unique())[:5]
            )
        with col2:
            st.markdown("**Indicators**")
            hist_indicators = st.multiselect(
                label="Indicators",
                options=sorted(df_hist['indicator_name'].unique()),
                default=sorted(df_hist['indicator_name'].unique())[:2]
            )

        if hist_countries:
            df_hist = df_hist[df_hist['country_name'].isin(hist_countries)]
        if hist_indicators:
            df_hist = df_hist[df_hist['indicator_name'].isin(hist_indicators)]

        st.markdown(f"### Data Overview: Last {hours} Hours ({len(df_hist)} records)")

        fig_hist = px.line(
            df_hist.sort_values('timestamp'),
            x='timestamp',
            y='value',
            color='country_name',
            line_dash='indicator_name',
            title="Indicator Trends"
        )
        fig_hist.update_layout(template='plotly_white')
        st.plotly_chart(fig_hist, width="stretch")

        st.markdown("### Summary Statistics")
        summary = df_hist.groupby(['country_name', 'indicator_name'])['value'].agg(['mean', 'min', 'max', 'count']).round(2)
        st.dataframe(summary, width="stretch")

        st.markdown("### Export Options")
        col1, col2, _ = st.columns([1,1,2])
        with col1:
            if st.button("Export CSV"):
                st.markdown(export_data(df_hist, 'csv'), unsafe_allow_html=True)
        with col2:
            if st.button("Export Excel"):
                st.markdown(export_data(df_hist, 'excel'), unsafe_allow_html=True)

        with st.expander("View All Historical Data"):
            st.dataframe(df_hist.sort_values('timestamp', ascending=False), width="stretch")
    else:
        st.warning("No historical data found. Ensure the consumer is running and storing data.")
