import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from collections import Counter

# Real-Time Crime Monitoring Dashboard
# Read-only MongoDB access. This app does not modify or delete stored data.

st.set_page_config(
    page_title="Crime Intelligence | Live Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=5000, key="crime_dashboard_refresh")
MONGO_URI = "mongodb://localhost:27017/"
FALLBACK_DATABASE = "crime_platform"
FALLBACK_COLLECTION = "crime_events"

st.markdown("""
<style>
:root { color-scheme: dark; }
.stApp {
    background: radial-gradient(ellipse at 48% 0%, #08213b 0%, #04101f 44%, #020813 100%);
    color: #eaf4ff;
}
[data-testid="stHeader"] { background: rgba(2, 8, 19, .88); }
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #06172a, #030b16);
    border-right: 1px solid #123451;
}
.block-container { padding-top: 1.15rem; padding-bottom: 2rem; max-width: 1800px; }
h1, h2, h3 { color: #eaf4ff !important; }
small, .muted { color: #91aac5; }
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(10, 35, 61, .96), rgba(5, 17, 32, .96));
    border: 1px solid #17466d; border-radius: 12px; padding: 15px 17px;
    box-shadow: 0 0 18px rgba(0, 126, 255, .06);
}
div[data-testid="stMetricLabel"] { color: #9bb6d1; }
div[data-testid="stMetricValue"] { color: #eaf6ff; }
.panel {
    background: linear-gradient(145deg, rgba(8, 27, 47, .96), rgba(3, 13, 25, .96));
    border: 1px solid #16456b; border-radius: 13px; padding: 16px 18px;
    margin-bottom: 12px; min-height: 100%;
}
.panel-title {
    color: #dff2ff; font-size: 15px; font-weight: 700; letter-spacing: .06em;
    text-transform: uppercase; padding-bottom: 10px; margin-bottom: 12px;
    border-bottom: 1px solid #12334f;
}
.live-dot {
    display:inline-block; width:9px; height:9px; border-radius:50%;
    background:#19efad; box-shadow:0 0 12px #19efad; margin-right:8px;
}
.status-pill {
    color:#21f0b0; border:1px solid #146c5a; background:#062a2b;
    padding:5px 10px; border-radius:20px; font-size:12px;
}
.alert-row {
    display:flex; justify-content:space-between; gap:12px; align-items:center;
    padding:10px 2px; border-bottom:1px solid #102b42; font-size:13px;
}
.alert-row:last-child { border-bottom:0; }
.dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:9px; }
.map-surface {
    position:relative; overflow:hidden; height:365px; border-radius:10px;
    border:1px solid #155080;
    background:
      radial-gradient(circle at 22% 66%, rgba(0,146,255,.28), transparent 16%),
      radial-gradient(circle at 71% 27%, rgba(255,55,75,.25), transparent 15%),
      radial-gradient(circle at 62% 70%, rgba(0,146,255,.2), transparent 17%),
      repeating-linear-gradient(27deg, transparent 0 22px, rgba(17,95,151,.22) 23px 25px),
      repeating-linear-gradient(118deg, transparent 0 31px, rgba(15,76,126,.3) 32px 34px),
      linear-gradient(135deg, #06172b, #071f37 55%, #03101f);
}
.map-river {
    position:absolute; width:130%; height:55px; left:-10%; top:49%;
    transform:rotate(-24deg); background:rgba(0,115,220,.18);
    border-top:2px solid #0878cf; border-bottom:2px solid #0878cf;
}
.zone {
    position:absolute; border:1px solid rgba(0,148,255,.85); background:rgba(0,92,180,.13);
    color:#71d4ff; padding:5px 12px; border-radius:5px; font-size:12px;
    box-shadow:0 0 14px rgba(0,135,255,.12);
}
.pin {
    position:absolute; color:#ff5263; font-size:22px; text-shadow:0 0 12px #ff263f;
}
.pin.blue { color:#13a9ff; text-shadow:0 0 12px #008cff; }
.pin.yellow { color:#ffc928; text-shadow:0 0 12px #ffb000; }
div[data-testid="stDataFrame"] { border:1px solid #17466d; border-radius:8px; }
.stButton button { border:1px solid #1764a0; background:#063b68; color:white; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500)

def load_events():
    """Find an existing collection containing event-like documents; read only."""
    client = get_client()
    client.admin.command("ping")
    candidates = []
    preferred = [(FALLBACK_DATABASE, FALLBACK_COLLECTION)]
    for db_name in client.list_database_names():
        if db_name in ("admin", "config", "local"):
            continue
        try:
            for coll_name in client[db_name].list_collection_names():
                if (db_name, coll_name) not in preferred:
                    candidates.append((db_name, coll_name))
        except Exception:
            continue

    ordered = preferred + candidates
    seen = set()
    for db_name, coll_name in ordered:
        if (db_name, coll_name) in seen:
            continue
        seen.add((db_name, coll_name))
        coll = client[db_name][coll_name]
        try:
            sample = coll.find_one({}, {"_id": 0})
            if not sample:
                continue
            keys = set(sample.keys())
            eventish = bool(keys.intersection({
                "event_id", "crime_type", "crime_id", "type", "location", "area"
            }))
            if eventish:
                docs = list(coll.find({}, {"_id": 0}).sort("_id", -1).limit(5000))
                if docs:
                    return pd.DataFrame(docs), f"{db_name}.{coll_name}"
        except Exception:
            continue
    return pd.DataFrame(), "No event collection found"

def first_existing(df, names):
    for name in names:
        if name in df.columns:
            return name
    return None

def safe_text(value):
    return str(value) if pd.notna(value) else "Unknown"

st.markdown("""
<div style="display:flex;align-items:center;gap:13px;flex-wrap:wrap">
  <div style="font-size:35px">🛡️</div>
  <div style="flex:1">
    <div style="font-size:27px;font-weight:800;letter-spacing:.08em;color:#eaf6ff">CRIME INTELLIGENCE</div>
    <div style="color:#20baff;font-size:14px;letter-spacing:.09em">REAL-TIME MONITORING PLATFORM</div>
  </div>
  <span class="status-pill"><span class="live-dot"></span>LIVE MONITORING</span>
</div>
<hr style="border-color:#123651;margin:12px 0 20px 0">
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🛡️ Control Center")
    page = st.radio(
        "NAVIGATION",
        ["Overview", "Live Incidents", "Crime Analytics", "Hotspot Map", "Suspect Network", "Reports"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption("SYSTEM STATUS")
    st.markdown('<span class="live-dot"></span> Services dashboard available', unsafe_allow_html=True)
    st.caption("Data is read from MongoDB. Kafka ingestion remains handled by your existing producer/consumer processes.")
    refresh = st.button("↻ Refresh data", use_container_width=True)
    if refresh:
        st.cache_data.clear()
        st.rerun()

try:
    df, source_name = load_events()
    mongo_ok = True
except Exception as exc:
    df, source_name, mongo_ok = pd.DataFrame(), "MongoDB unavailable", False
    st.error(f"MongoDB connection failed: {type(exc).__name__}: {exc}")

if df.empty:
    st.warning(
        f"No event documents were found in MongoDB ({source_name}). "
        "The UI is ready, but it can only show events that are stored in a MongoDB collection."
    )
    st.info("Your existing live_dashboard.py can continue running. This Streamlit page does not alter Kafka, Docker volumes, or stored data.")
    st.stop()

crime_col = first_existing(df, ["crime_type", "type", "crime"])
location_col = first_existing(df, ["location", "area", "city"])
time_col = first_existing(df, ["timestamp", "date", "created_at"])
event_col = first_existing(df, ["event_id", "crime_id", "id"])
source_col = first_existing(df, ["source"])
gang_col = first_existing(df, ["gang_id"])
suspect_col = first_existing(df, ["criminal_ids", "suspect_id", "suspect_ids"])

df = df.copy()
if crime_col:
    df["_crime"] = df[crime_col].fillna("Unknown").astype(str)
else:
    df["_crime"] = "Unknown"
if location_col:
    df["_location"] = df[location_col].fillna("Unknown").astype(str)
else:
    df["_location"] = "Unknown"
if time_col:
    df["_time"] = pd.to_datetime(df[time_col], errors="coerce", utc=True)
else:
    df["_time"] = pd.NaT

total = len(df)
crime_counts = df["_crime"].value_counts()
location_counts = df["_location"].value_counts()
unique_crimes = int(df["_crime"].nunique())
unique_locations = int(df["_location"].nunique())
latest = df.sort_values("_time", ascending=False, na_position="last").head(8)

# Top-level status row
h1, h2 = st.columns([3, 1])
with h1:
    st.markdown(f"### {page}")
    st.caption(f"Data source: `{source_name}` · Loaded records: {total:,}")
with h2:
    st.markdown(f'<div style="text-align:right;padding-top:12px"><span class="status-pill"><span class="live-dot"></span>{"MongoDB connected" if mongo_ok else "Offline"}</span></div>', unsafe_allow_html=True)

if page in ("Overview", "Live Incidents"):
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Incidents", f"{total:,}")
    m2.metric("Crime Types", unique_crimes)
    m3.metric("Areas Monitored", unique_locations)
    m4.metric("Latest Event", safe_text(latest.iloc[0][time_col])[:19] if time_col and not latest.empty else "—")

    left, right = st.columns([2.15, 1])
    with left:
        st.markdown('<div class="panel"><div class="panel-title">⌖ Live incident area map</div>', unsafe_allow_html=True)
        # A stylized area map is used when records only contain labels such as Area A-D.
        # If real coordinates exist, render the real map below instead.
        lat_col = first_existing(df, ["latitude", "lat"])
        lon_col = first_existing(df, ["longitude", "lon", "lng"])
        if lat_col and lon_col:
            coords = df[[lat_col, lon_col]].copy()
            coords.columns = ["lat", "lon"]
            coords["lat"] = pd.to_numeric(coords["lat"], errors="coerce")
            coords["lon"] = pd.to_numeric(coords["lon"], errors="coerce")
            coords = coords.dropna()
            if not coords.empty:
                st.map(coords, size=30)
            else:
                st.info("Coordinates exist, but no valid latitude/longitude values were found.")
        else:
            zones = {"Area A": (22, 62), "Area B": (48, 30), "Area C": (70, 68), "Area D": (79, 24)}
            points = [(17, 55, "🔴", ""), (28, 42, "📍", "blue"), (42, 25, "🔴", ""),
                      (58, 42, "📍", "blue"), (71, 54, "🔴", ""), (84, 39, "📍", "yellow"),
                      (37, 72, "📍", "yellow"), (62, 78, "📍", "blue"), (52, 61, "🔴", "")]
            map_html = '<div class="map-surface"><div class="map-river"></div>'
            for name, (x, y) in zones.items():
                map_html += f'<span class="zone" style="left:{x}%;top:{y}%">{name}</span>'
            for x, y, symbol, cls in points:
                map_html += f'<span class="pin {cls}" style="left:{x}%;top:{y}%">{symbol}</span>'
            map_html += '<div style="position:absolute;left:14px;top:12px;background:#031222dd;border:1px solid #1a5079;padding:9px 12px;border-radius:7px;font-size:12px">🔵 Theft &nbsp; 🔴 Burglary &nbsp; 🟠 Assault &nbsp; 🟡 Robbery</div></div>'
            st.markdown(map_html, unsafe_allow_html=True)
            st.caption("Illustrative area layout: actual map coordinates are not available in the stored records.")
        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown('<div class="panel"><div class="panel-title">◉ Crime distribution</div>', unsafe_allow_html=True)
        if not crime_counts.empty:
            st.bar_chart(crime_counts.rename_axis("Crime type"), height=220)
        st.markdown('<div class="panel-title" style="margin-top:18px">⌁ Area incident ranking</div>', unsafe_allow_html=True)
        if not location_counts.empty:
            st.bar_chart(location_counts.rename_axis("Area"), height=190)
        st.markdown("</div>", unsafe_allow_html=True)

    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown('<div class="panel"><div class="panel-title">◷ Incidents by time</div>', unsafe_allow_html=True)
        timed = df.dropna(subset=["_time"]).copy()
        if not timed.empty:
            timed["_hour"] = timed["_time"].dt.hour
            hourly = timed.groupby("_hour").size().reindex(range(24), fill_value=0)
            st.bar_chart(hourly.rename_axis("Hour (UTC)"), height=230)
        else:
            st.info("Timestamp data is not available for hourly analysis.")
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="panel"><div class="panel-title">⚠ Recent alerts</div>', unsafe_allow_html=True)
        for _, row in latest.head(6).iterrows():
            timestamp = safe_text(row[time_col])[:19] if time_col else "Time unavailable"
            crime = safe_text(row["_crime"])
            area = safe_text(row["_location"])
            color = "#ff4d5f" if crime.lower() in ("burglary", "robbery", "assault") else "#12b7ff"
            st.markdown(f'<div class="alert-row"><span><span class="dot" style="background:{color}"></span><b>{crime}</b><br><span style="color:#91aac5;font-size:11px">{timestamp}</span></span><span style="color:#9fdcff">{area}</span></div>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

if page == "Crime Analytics":
    a, b = st.columns(2)
    with a:
        st.markdown("#### Crime type breakdown")
        st.dataframe(crime_counts.rename("Incidents").reset_index(names="Crime type"), use_container_width=True, hide_index=True)
    with b:
        st.markdown("#### Area breakdown")
        st.dataframe(location_counts.rename("Incidents").reset_index(names="Area"), use_container_width=True, hide_index=True)
    if not crime_counts.empty and not location_counts.empty:
        st.markdown("#### Crime type vs area")
        cross = pd.crosstab(df["_crime"], df["_location"])
        st.dataframe(cross, use_container_width=True)

if page == "Hotspot Map":
    lat_col = first_existing(df, ["latitude", "lat"])
    lon_col = first_existing(df, ["longitude", "lon", "lng"])
    if lat_col and lon_col:
        points = df[[lat_col, lon_col]].copy()
        points.columns = ["lat", "lon"]
        points["lat"] = pd.to_numeric(points["lat"], errors="coerce")
        points["lon"] = pd.to_numeric(points["lon"], errors="coerce")
        st.map(points.dropna())
    else:
        st.info("Your current events use area labels rather than geographic coordinates. The overview shows a stylized area map; a true geographic hotspot map requires real latitude/longitude fields.")

if page == "Suspect Network":
    if gang_col:
        st.markdown("#### Incidents by gang")
        st.dataframe(df.groupby(gang_col).size().rename("Incidents").reset_index(), use_container_width=True, hide_index=True)
    elif suspect_col:
        st.markdown("#### Suspect-linked incident records")
        cols = [c for c in [event_col, crime_col, location_col, suspect_col, time_col] if c]
        st.dataframe(df[cols].head(300), use_container_width=True, hide_index=True)
    else:
        st.info("No gang_id or suspect identifier fields were found in the detected event collection. This section will populate when those fields exist in stored records.")

if page == "Reports":
    st.markdown("#### Incident records")
    search = st.text_input("Search crime type, area, event ID, or source")
    report_df = df.copy()
    if search.strip():
        mask = report_df.astype(str).apply(lambda col: col.str.contains(search, case=False, na=False)).any(axis=1)
        report_df = report_df[mask]
    st.dataframe(report_df.drop(columns=["_time"], errors="ignore"), use_container_width=True, hide_index=True)
    csv_data = report_df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇ Export filtered incidents as CSV", csv_data, "crime_incidents_report.csv", "text/csv")

st.caption("Crime Intelligence Dashboard · Existing databases are accessed read-only. The area map is illustrative unless coordinates are present.")
