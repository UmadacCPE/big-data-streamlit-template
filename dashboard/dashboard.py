import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh
import base64
from io import BytesIO

st.set_page_config(
    page_title="World Bank Live Indicators",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Use Streamlit's native dark theme (set in .streamlit/config.toml or via UI) ---
# No heavy custom CSS needed – Streamlit handles colors automatically.
# Only minimal overrides for specific elements that may still appear off.
st.markdown("""
<style>
    /* Ensure metric labels and values are readable */
    .stMetric label, .stMetric div[data-testid="stMetricValue"] {
        color: inherit !important;
    }
    /* Sidebar text color */
    section[data-testid="stSidebar"] * {
        color: inherit;
    }
    /* Make plotly charts blend with dark background */
    .stPlotlyChart {
        background-color: transparent !important;
    }
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

@st.cache_data(ttl=5)
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
        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
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
        st.success(f"✅ Live data active: {len(df_live)} records (last 10 min)")
    else:
        st.warning("⏳ Awaiting data... Pipeline updates every 60s.")
        st.info("World Bank data is annual; 'live' shows latest pipeline inserts.")

    if not df_live.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Filter Countries**")
            selected_countries = st.multiselect(
                label="Countries",
                options=sorted(df_live['country_name'].unique()),
                default=sorted(df_live['country_name'].unique())[:3]
            )
        with col2:
            st.markdown("**Filter Indicators**")
            selected_indicators = st.multiselect(
                label="Indicators",
                options=sorted(df_live['indicator_name'].unique()),
                default=sorted(df_live['indicator_name'].unique())[:2]
            )

        df_filtered = df_live.copy()
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
                title="Latest Indicator Values by Country",
                template="plotly_dark"
            )
            st.plotly_chart(fig_bar, width="stretch")

        if df_live['timestamp'].nunique() > 1:
            st.markdown("### Recent Updates")
            fig_line = px.line(
                df_live.sort_values('timestamp'),
                x='timestamp',
                y='value',
                color='country_name',
                line_dash='indicator_name',
                title="Values Over Time",
                template="plotly_dark"
            )
            st.plotly_chart(fig_line, width="stretch")

        st.markdown("### Recent Records")
        st.dataframe(df_live.sort_values('timestamp', ascending=False).head(20), width="stretch")
    else:
        st.info("No live data yet. Ensure the producer is running and has fetched data.")

elif page == "Historical Analysis":
    st.markdown("# Historical Indicator Analysis")

    hours = st.slider("Data Range (hours)", 1, 168, 24)
    df_hist = get_historical(hours)

    if not df_hist.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Countries**")
            hist_countries = st.multiselect(
                label="Countries",
                options=sorted(df_hist['country_name'].unique()),
                default=sorted(df_hist['country_name'].unique())[:5],
                key="hist_countries"
            )
        with col2:
            st.markdown("**Indicators**")
            hist_indicators = st.multiselect(
                label="Indicators",
                options=sorted(df_hist['indicator_name'].unique()),
                default=sorted(df_hist['indicator_name'].unique())[:2],
                key="hist_indicators"
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
            title="Indicator Trends",
            template="plotly_dark"
        )
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