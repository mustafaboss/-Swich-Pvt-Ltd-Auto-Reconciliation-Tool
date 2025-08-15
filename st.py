# app.py — Final (original logic preserved + animated UI + logo) — Cloud-safe
import os
import zipfile
import shutil
import paramiko
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# --------------------
# Helpers: prefer Streamlit secrets; fallback to env (.env)
# --------------------
def cfg(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

# --------------------
# Config (same keys; now secrets-friendly)
# --------------------
SFTP_HOST = cfg("SFTP_HOST")
SFTP_PORT = int(cfg("SFTP_PORT", 22) or 22)
SFTP_USERNAME = cfg("SFTP_USERNAME")
SFTP_PASSWORD = cfg("SFTP_PASSWORD")
REMOTE_DIR = cfg("REMOTE_DIR", ".")

DB_SERVER = cfg("DB_SERVER")
DB_NAME = cfg("DB_NAME")
DB_USER = cfg("DB_USER")
DB_PASSWORD = cfg("DB_PASSWORD")
DB_DRIVER = cfg("DB_DRIVER", "ODBC Driver 18 for SQL Server")

LOCAL_ZIP = "temp_download.zip"
EXTRACT_DIR = "temp_extracted"

# Paths (hero uses logo.jpg; browser tab uses favicon.ico)
LOGO_PATH = r"C:\Users\Ghulam Mustafa\OneDrive\Desktop\filezila\logo.jpg"       # hero image
FAVICON_PATH = r"C:\Users\Ghulam Mustafa\OneDrive\Desktop\filezila\favicon.ico" # browser tab icon
PAGE_ICON = FAVICON_PATH if os.path.exists(FAVICON_PATH) else LOGO_PATH

# --------------------
# Page + CSS (animated hero, 3D cards, colorful buttons)
# --------------------
st.set_page_config(layout="wide", page_title="Swich Pvt Ltd — Reconciliation", page_icon=PAGE_ICON if os.path.exists(PAGE_ICON) else "📊")
st.markdown(
    """
    <style>
    :root { --blue: #0ea5e9; --green: #22c55e; --teal: #14b8a6; --cardA: rgba(255,255,255,0.95); --cardB: rgba(245,248,255,0.92); }

    /* animated soft background */
    .stApp {
        background: radial-gradient(1200px 600px at 10% 10%, #eff6ff 0%, #ffffff 45%);
        background-size: 200% 200%;
        animation: bgShift 18s ease-in-out infinite alternate;
    }
    @keyframes bgShift { to { background-position: 80% 60%; } }

    /* Hero (FULL WIDTH + TALLER) */
    .hero {
        background: linear-gradient(135deg,var(--blue),var(--green));
        color:#fff;
        padding: 50px 40px;
        min-height: 200px;
        width: 100%;
        border-radius:20px;
        box-shadow: 0 20px 60px rgba(2,6,23,0.12), inset 0 -12px 30px rgba(255,255,255,0.08);
        margin-bottom: 20px;
        display:flex; align-items:center; gap:30px; justify-content:center;
        animation: fadeUp .5s ease both;
    }
    .hero .logo {
        width: 140px; height: 140px;
        object-fit: contain;
        border-radius:14px;
        box-shadow: 0 12px 28px rgba(20,184,166,0.12);
        transition: transform .6s cubic-bezier(.2,.8,.2,1);
    }
    .hero .logo:hover { transform: rotateY(20deg) scale(1.05); filter: drop-shadow(0 18px 40px rgba(20,184,166,0.12)); }
    .hero .meta { text-align:left; }
    .hero h1 { margin:0; font-size:36px; letter-spacing:.3px; }
    .hero p  { margin:0; opacity:.92; font-size:18px; }

    @keyframes fadeUp { from { opacity:0; transform: translateY(8px); } to { opacity:1; transform: translateY(0); } }

    /* Card */
    .card {
        background: linear-gradient(180deg,var(--cardA),var(--cardB));
        border-radius: 16px; padding:16px;
        box-shadow: 0 10px 25px rgba(15,23,42,0.08);
        border: 1px solid rgba(0,0,0,0.04);
        transition: transform .18s ease, box-shadow .18s ease;
        transform-style: preserve-3d;
    }
    .card:hover { transform: translateY(-6px) rotateX(1.6deg) rotateY(-1.6deg); box-shadow: 0 18px 42px rgba(15,23,42,0.14); }

    /* Big 3D chip */
    .big-3d {
        background: linear-gradient(135deg,#0f766e,var(--teal));
        color:white; padding:18px; border-radius:14px; font-weight:700; text-align:center;
        box-shadow: 0 20px 50px rgba(20,184,166,0.16);
    }

    /* Buttons */
    .stButton > button, .stDownloadButton > button {
        background: linear-gradient(135deg,var(--blue),var(--green));
        color:#fff; border:none; padding:.55rem .9rem; border-radius:10px; font-weight:700;
        box-shadow: 0 10px 28px rgba(2,6,23,0.10);
        transition: transform .12s ease, box-shadow .18s ease, filter .12s ease;
        position: relative; overflow: hidden;
    }
    .stButton > button:hover, .stDownloadButton > button:hover { transform: translateY(-1px); filter: brightness(1.03); box-shadow: 0 16px 36px rgba(34,197,94,0.18); cursor:pointer; }

    /* Table hover */
    .stDataFrame table tbody tr:hover { box-shadow: inset 0 0 0 9999px rgba(14,165,233,0.06); transition: box-shadow .18s; }

    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size:13px; color:#083344; }
    .small-muted { color:#6b7280; font-size:13px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------
# HERO (full width — no Streamlit column constraint)
# --------------------
if os.path.exists(LOGO_PATH):
    st.markdown(
        f"""
        <div class="hero">
          <img src="file:///{LOGO_PATH}" class="logo" alt="logo"/>
          <div class="meta">
            <h1>📊 Swich Pvt Ltd — Auto Reconciliation</h1>
            <p>FileZilla ZIP → TXT parse → MSSQL fetch → Pivots → Reconcile</p>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.markdown(
        """
        <div class="hero">
          <div class="meta">
            <h1>📊 Swich Pvt Ltd — Auto Reconciliation</h1>
            <p>FileZilla ZIP → TXT parse → MSSQL fetch → Pivots → Reconcile</p>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# --------------------
# Controls (same as original)
# --------------------
use_date = st.checkbox("📅 Use Date Filter for DB", value=True)
start_date = None
end_date = None
if use_date:
    c1, c2 = st.columns(2)
    start_date = c1.date_input("From date", pd.to_datetime("2025-08-04"))
    end_date = c2.date_input("To date", pd.to_datetime("2025-08-04"))
    if start_date > end_date:
        st.error("Start date cannot be after end date.")
        st.stop()

# --------------------
# Charges function (unchanged)
# --------------------
def calculate_charges_value(a):
    try:
        a = float(a)
    except:
        return 0.0
    if a <= 10000:
        return 12.5
    elif a <= 100000:
        return 31.25
    elif a <= 250000:
        return 62.5
    elif a <= 1000000:
        return 125
    elif a <= 2500000:
        return 250
    elif a <= 5000000:
        return 375
    else:
        return 500

# --------------------
# STEP A: SFTP download + parse TXT (UNCHANGED logic)
# --------------------
st.subheader("📥 FileZilla (SFTP) files")
col_a, col_b = st.columns([2, 1])
with col_a:
    st.markdown('<div class="card"><div class="small-muted">SFTP Status</div>', unsafe_allow_html=True)
with col_b:
    st.markdown('</div>', unsafe_allow_html=True)

sftp = None
transport = None
latest_filename = ""
try:
    with st.spinner("Connecting to SFTP and downloading latest zip..."):
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        files = sftp.listdir_attr(REMOTE_DIR)
        zip_files = [f for f in files if f.filename.lower().endswith(".zip")]
        if not zip_files:
            st.error("No .zip files found on remote directory.")
            raise Exception("No zip files on remote")

        latest = max(zip_files, key=lambda x: x.st_mtime)
        latest_filename = latest.filename
        sftp.get(f"{REMOTE_DIR}/{latest.filename}", LOCAL_ZIP)
        st.success(f"Downloaded latest zip: {latest.filename}")
except Exception as e:
    st.error(f"SFTP error: {e}")
    latest_filename = ""

# Extract ZIP (unchanged)
try:
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    if os.path.exists(LOCAL_ZIP):
        # clean previous extraction for safety
        try:
            shutil.rmtree(EXTRACT_DIR)
            os.makedirs(EXTRACT_DIR, exist_ok=True)
        except Exception:
            pass
        with zipfile.ZipFile(LOCAL_ZIP, "r") as z:
            z.extractall(EXTRACT_DIR)
        st.success("ZIP extracted.")
    else:
        st.warning("No zip downloaded. If testing locally, place sample zip as temp_download.zip")
except Exception as e:
    st.error(f"Error extracting ZIP file: {e}")
    st.stop()

# Parse matching TXT files (unchanged)
target_txt_files = []
for root, dirs, files in os.walk(EXTRACT_DIR):
    for file in files:
        if file.startswith("SETLLEMENTSCROLL_Numbers_Pvt") and file.endswith(".txt"):
            target_txt_files.append(os.path.join(root, file))

if not target_txt_files:
    st.warning("No matching TXT files found after extraction. Using empty FileZilla dataset.")
else:
    st.write(f"Found {len(target_txt_files)} matching TXT files.")

all_dfs = []
for txt_file in target_txt_files:
    with open(txt_file, 'r', encoding="utf-8", errors="ignore") as f:
        lines = [line.rstrip('\n') for line in f if line.strip()]
    parsed_rows = []
    for line in lines:
        parsed_rows.append({
            "Biller": line[0:13].strip(),
            "Consumer No": line[13:33].strip(),
            "IMD": line[33:39].strip(),
            "IMD 2": line[39:45].strip(),
            "PAN": line[45:70].strip(),
            "Account": line[70:90].strip(),
            "Amount": line[90:102].strip(),
            "Paisa": line[102:104].strip(),
            "Trx Date": line[104:112].strip(),
            "Time": line[112:118].strip(),
            "Settlement Date": line[118:126].strip(),
            "Payment Mode": line[126:127].strip(),
            "Bank Name": line[127:132].strip(),
            "STAN": line[132:139].strip(),
            "AUTH ID": line[139:].strip()
        })
    df = pd.DataFrame(parsed_rows)
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
    df["Trx Date_y"] = df["Trx Date"].apply(lambda x: f"{x[0:4]}-{x[4:6]}-{x[6:8]}" if isinstance(x, str) and len(x) == 8 else "")
    df["Settlement Date_y"] = df["Settlement Date"].apply(lambda x: f"{x[0:4]}-{x[4:6]}-{x[6:8]}" if isinstance(x, str) and len(x) == 8 else "")
    df["Txn Date"] = pd.to_datetime(df["Trx Date_y"], errors="coerce").dt.strftime("%Y-%m-%d")
    all_dfs.append(df)

if all_dfs:
    df_fz = pd.concat(all_dfs, ignore_index=True)
else:
    df_fz = pd.DataFrame(columns=["Txn Date","Amount"])

st.write("FileZilla parsed sample:")
st.dataframe(df_fz.head(5))

# --------------------
# STEP B: DB fetch + Charges (unchanged)
# --------------------
st.subheader("🗄️ Database (MSSQL) Fetch")
params = {}
date_condition = ""
if use_date:
    date_condition = "AND CAST(T.ChannelResponseDateTime AS DATE) BETWEEN :start_date AND :end_date"
    params["start_date"] = start_date
    params["end_date"] = end_date

db_query = text(f"""
SELECT 
    T.Id,
    T.OrderID,
    T.CustomerTransactionId,
    C.Name as Customer,
    CG.Name as Service,
    CH.Name as Channel,
    T.Item,
    T.MSISDN,
    TS.Name as Status,
    T.Amount,
    T.RefundAmount,
    T.RefundReason,
    T.RefundDateTime,
    T.DiscountedAmount,
    T.Currency,
    T.ExchangeRate,
    T.ConvertedAmount,
    T.ConvertedCurrency,
    T.TransactionLocality AS Locality,
    T.PAN,
    T.CreatedDateTime,
    T.ChannelResponseCode,
    T.ChannelResponseMessage,
    T.ChannelResponseDateTime
FROM Transactionv2 T
LEFT JOIN TransactionStatusPayIn TS ON T.Status = TS.Id
LEFT JOIN Customer C ON T.CustomerId = C.Id
LEFT JOIN Category CG ON T.CategoryId = CG.Id
LEFT JOIN Channel CH ON T.ChannelId = CH.Id
WHERE T.CategoryId = 3
  AND T.Status = 3
  AND T.TransactionLocality = 1
  {date_condition}
""")

df_db = pd.DataFrame()
try:
    engine = create_engine(f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver={DB_DRIVER.replace(' ', '+')}")
    with st.spinner("Running DB query..."):
        df_db = pd.read_sql(db_query, engine, params=params)
    st.success("DB data fetched.")
except Exception as e:
    st.warning(f"DB fetch failed (using empty DB sample). Error: {e}")
    df_db = pd.DataFrame(columns=[
        "Id","OrderID","CustomerTransactionId","Customer","Service","Channel","Item","MSISDN","Status",
        "Amount","RefundAmount","RefundReason","RefundDateTime","DiscountedAmount","Currency","ExchangeRate",
        "ConvertedAmount","ConvertedCurrency","Locality","PAN","CreatedDateTime","ChannelResponseCode","ChannelResponseMessage","ChannelResponseDateTime"
    ])

if "ChannelResponseDateTime" in df_db.columns:
    df_db["Txn Date"] = pd.to_datetime(df_db["ChannelResponseDateTime"], errors="coerce").dt.strftime("%Y-%m-%d")
else:
    df_db["Txn Date"] = ""

if "ConvertedAmount" not in df_db.columns:
    df_db["ConvertedAmount"] = 0

df_db["ConvertedAmount"] = pd.to_numeric(df_db["ConvertedAmount"], errors="coerce").fillna(0)
df_db["Amount"] = pd.to_numeric(df_db.get("Amount", 0), errors="coerce").fillna(0)
df_db["Charges"] = df_db["ConvertedAmount"].apply(calculate_charges_value)

st.write("DB sample (first rows):")
st.dataframe(df_db.head(5))

# --------------------
# STEP C: Pivots (SAFE column rename to avoid ValueError)
# --------------------
st.subheader("📈 Pivots")

pivot_db = df_db.pivot_table(
    index="Txn Date",
    values=["Amount", "Charges", "ConvertedAmount"],
    aggfunc={
        "Amount": ["count", "sum"],
        "Charges": "sum",
        "ConvertedAmount": "sum"
    }
)

# SAFE flatten/rename
expected_columns = ["Count of Amount", "Sum of Amount", "Sum of Charges", "Sum of Converted Amount"]
try:
    if len(pivot_db.columns) == len(expected_columns):
        pivot_db.columns = expected_columns
    else:
        # If MultiIndex present, stringify to avoid crash and warn
        st.warning(f"⚠ Pivot column mismatch — expected {len(expected_columns)}, got {len(pivot_db.columns)}. Actual: {list(pivot_db.columns)}")
        pivot_db.columns = [(" ".join(map(str, c)) if isinstance(c, tuple) else str(c)) for c in pivot_db.columns]
except Exception as e:
    st.warning(f"Could not rename pivot columns safely: {e}")
    pivot_db.columns = [str(c) for c in pivot_db.columns]

pivot_db = pivot_db.reset_index().fillna(0)

# compute Net per date (Converted - Charges) if columns exist
if not pivot_db.empty and "Sum of Converted Amount" in pivot_db.columns and "Sum of Charges" in pivot_db.columns:
    pivot_db["Sum of Net Amt"] = pivot_db["Sum of Converted Amount"] - pivot_db["Sum of Charges"]

# add suffix for merge clarity (only rename if present)
rename_map = {
    "Count of Amount": "Count of Amount_DB",
    "Sum of Amount": "Sum of Amount_DB",
    "Sum of Charges": "Sum of Charges_DB",
    "Sum of Converted Amount": "Sum of Converted Amount_DB",
    "Sum of Net Amt": "Sum of Net Amt_DB"
}
pivot_db = pivot_db.rename(columns={k: v for k, v in rename_map.items() if k in pivot_db.columns})

pivot_fz = df_fz.pivot_table(index="Txn Date", values="Amount", aggfunc=["count","sum"]).reset_index().fillna(0)
pivot_fz.columns = ["Txn Date","Count of Amount_FZ","Sum of Amount_FZ"]

# layout display
r1, r2 = st.columns([2,1])
with r1:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("DB Pivot (per Txn Date)")
    st.dataframe(pivot_db, use_container_width=True, height=220)
    st.download_button("⬇️ Download DB Pivot CSV", pivot_db.to_csv(index=False).encode('utf-8'), file_name="db_pivot.csv")
    st.markdown('</div>', unsafe_allow_html=True)
with r2:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("FileZilla Pivot")
    st.dataframe(pivot_fz, use_container_width=True, height=220)
    st.download_button("⬇️ Download FileZilla Pivot CSV", pivot_fz.to_csv(index=False).encode('utf-8'), file_name="filezilla_pivot.csv")
    st.markdown('</div>', unsafe_allow_html=True)

# --------------------
# STEP D: Reconciliation (unchanged)
# --------------------
st.subheader("🔍 Reconciliation (Pivot summary)")
if st.button("🔁 Reconcile Now"):
    merged = pd.merge(pivot_db, pivot_fz, on="Txn Date", how="outer").fillna(0)
    # diffs
    if "Count of Amount_DB" in merged.columns and "Count of Amount_FZ" in merged.columns:
        merged["Diff - Count"] = merged["Count of Amount_DB"] - merged["Count of Amount_FZ"]
    if "Sum of Amount_DB" in merged.columns and "Sum of Amount_FZ" in merged.columns:
        merged["Diff - Sum"] = merged["Sum of Amount_DB"] - merged["Sum of Amount_FZ"]
    # per-date net (converted - charges) available in DB side already
    if "Sum of Converted Amount_DB" in merged.columns and "Sum of Charges_DB" in merged.columns:
        merged["ConvertedMinusCharges_DB"] = merged["Sum of Converted Amount_DB"] - merged["Sum of Charges_DB"]

    # calculate charges based on whichever amount available (FZ sum priority)
    def calc_charge_row(x):
        amt = x.get("Sum of Amount_FZ", 0) or x.get("Sum of Amount_DB", 0)
        return calculate_charges_value(amt)
    merged["Charges_Calc_From_Amount"] = merged.apply(calc_charge_row, axis=1)

    st.dataframe(merged, use_container_width=True, height=360)
    st.download_button("⬇️ Download Reconciliation CSV", merged.to_csv(index=False).encode('utf-8'), file_name="reconciliation_result.csv")

# --------------------
# Highlighted 3D Info card (unchanged)
# --------------------
total_converted_minus_charges = df_db["ConvertedAmount"].sum() - df_db["Charges"].sum()
st.markdown("---")
st.markdown(f'<div class="big-3d">💰 Sum of Converted Amount - Sum of Charges (DB): <span style="font-size:20px">{total_converted_minus_charges:,.2f}</span></div>', unsafe_allow_html=True)

# --------------------
# CSV Upload search (unchanged)
# --------------------
st.subheader("📂 Upload CSV to search for this value in 'credit' column")
uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])
if uploaded_file:
    try:
        uploaded_df = pd.read_csv(uploaded_file)
        st.write("Uploaded CSV Preview:", uploaded_df.head())
        lower_cols = [c.lower() for c in uploaded_df.columns]
        if "credit" not in lower_cols:
            st.error("Uploaded CSV does not contain a 'credit' column.")
        else:
            credit_col = uploaded_df.columns[lower_cols.index("credit")]
            uploaded_df[credit_col] = pd.to_numeric(uploaded_df[credit_col], errors='coerce').fillna(0)
            found = (uploaded_df[credit_col].round(2) == round(total_converted_minus_charges,2)).any()
            if found:
                st.success(f"✅ Value {total_converted_minus_charges:.2f} FOUND in uploaded CSV 'credit' column.")
            else:
                st.warning(f"❌ Value {total_converted_minus_charges:.2f} NOT FOUND in uploaded CSV 'credit' column.")
    except Exception as e:
        st.error(f"Error reading uploaded CSV: {e}")

# --------------------
# cleanup (unchanged)
# --------------------
try:
    if sftp:
        sftp.close()
    if transport:
        transport.close()
except:
    pass
