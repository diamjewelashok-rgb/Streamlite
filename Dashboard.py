import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# ========================
# SQL CONNECTION
# ========================

server = "localhost\\SQLEXPRESS"
database = "EMR"

connection_string = (
    f"mssql+pyodbc://@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

engine = create_engine(connection_string)

# ========================
# PAGE CONFIG
# ========================

st.set_page_config(
    page_title="Production Dashboard",
    layout="wide"
)

# ========================
# CURRENT PERIOD
# ========================

import datetime
now = datetime.datetime.now()
CURRENT_PERIOD = now.year * 100 + now.month  # e.g. 202604

GOLD_DEPTS   = ['SCAS','SCEN','SFIL','SFND','SMLT','SREF','SSSP']
SILVER_DEPTS = ['SCAS','SCEN','SFIL','SFND','SMLT','SREF','SSSP']
DIAMOND_LOC  = 'SDIA-JW'

DEPT_NAMES = {
    'SCAS': 'STK OF CASTING',
    'SCEN': 'STK OF CENTRAL',
    'SFIL': 'STK OF FILLING',
    'SFND': 'STK OF FINDING',
    'SMLT': 'STK OF MELTING',
    'SREF': 'STK OF REFINERY',
    'SSSP': 'STK OF SSP',
    'SDIA-JW': 'STK OF GND JOBWORK',
}

# ========================
# LOAD DATA
# ========================

@st.cache_data(ttl=30)
def load_data(metal_ctg, depts, period):
    depts_str = ",".join([f"'{d}'" for d in depts])

    query = f"""
    SELECT
        S.SLoc                                          AS Dept,
        R.RmCd                                         AS RmCode,
        ISNULL(R.RmDesc, '')                           AS Description,
        -- Opening = all periods before current
        ROUND(SUM(CASE WHEN S.SYyMm < {period}
            THEN (S.SRmDrWt - S.SRmCrWt) * ISNULL(R.RmPurityWt,1)
            ELSE 0 END), 3)                            AS Opening,
        -- Issue = CrWt this period * purity
        ROUND(SUM(CASE WHEN S.SYyMm = {period}
            THEN S.SRmCrWt * ISNULL(R.RmPurityWt,1)
            ELSE 0 END), 3)                            AS Issue,
        -- Receipt = DrWt this period * purity
        ROUND(SUM(CASE WHEN S.SYyMm = {period}
            THEN S.SRmDrWt * ISNULL(R.RmPurityWt,1)
            ELSE 0 END), 3)                            AS Receipt,
        -- Closing = cumulative all periods
        ROUND(SUM((S.SRmDrWt - S.SRmCrWt) * ISNULL(R.RmPurityWt,1)), 3) AS Closing
    FROM dbo.SYyMm S
    LEFT JOIN dbo.RmMst R ON R.RmCd = S.SRmCd
    WHERE S.SLoc IN ({depts_str})
      AND R.RmCtg = '{metal_ctg}'
    GROUP BY S.SLoc, R.RmCd, R.RmDesc, R.RmPurityWt
    HAVING ROUND(SUM((S.SRmDrWt - S.SRmCrWt) * ISNULL(R.RmPurityWt,1)), 3) <> 0
    ORDER BY S.SLoc, R.RmCd
    """

    df = pd.read_sql(query, engine)
    for col in ['Opening','Issue','Receipt','Closing']:
        df[col] = df[col].fillna(0).round(3)
    return df


@st.cache_data(ttl=30)
def load_diamond(period):
    query = f"""
    SELECT
        S.SLoc                                          AS Dept,
        R.RmCd                                         AS RmCode,
        ISNULL(R.RmDesc, '')                           AS Description,
        ROUND(SUM(CASE WHEN S.SYyMm < {period}
            THEN (S.SRmDrWt - S.SRmCrWt)
            ELSE 0 END), 3)                            AS Opening,
        ROUND(SUM(CASE WHEN S.SYyMm = {period}
            THEN S.SRmCrWt ELSE 0 END), 3)            AS Issue,
        ROUND(SUM(CASE WHEN S.SYyMm = {period}
            THEN S.SRmDrWt ELSE 0 END), 3)            AS Receipt,
        ROUND(SUM(S.SRmDrWt - S.SRmCrWt), 3)         AS Closing
    FROM dbo.SYyMm S
    LEFT JOIN dbo.RmMst R ON R.RmCd = S.SRmCd
    WHERE S.SLoc = '{DIAMOND_LOC}'
      AND R.RmCtg = 'D'
    GROUP BY S.SLoc, R.RmCd, R.RmDesc
    HAVING ROUND(SUM(S.SRmDrWt - S.SRmCrWt), 3) <> 0
    ORDER BY R.RmCd
    """

    df = pd.read_sql(query, engine)
    for col in ['Opening','Issue','Receipt','Closing']:
        df[col] = df[col].fillna(0).round(3)
    # Keep only positive closing (matches ERP)
    df = df[df['Closing'] > 0]
    return df

# ========================
# LOAD
# ========================

try:
    df_gold    = load_data('G', GOLD_DEPTS,   CURRENT_PERIOD)
    df_silver  = load_data('S', SILVER_DEPTS, CURRENT_PERIOD)
    df_diamond = load_diamond(CURRENT_PERIOD)
except Exception as e:
    st.error(e)
    st.stop()

# ========================
# SIDEBAR
# ========================

st.sidebar.title("Production Dashboard")

menu = st.sidebar.radio(
    "Reports",
    ["Gold Summary", "Silver Summary", "Diamond Summary", "Production Report"]
)

# Dept filter only for gold/silver
if menu in ["Gold Summary", "Silver Summary", "Production Report"]:
    dept_options = GOLD_DEPTS
    dept_filter = st.sidebar.multiselect(
        "Filter Department",
        dept_options,
        default=[]
    )
else:
    dept_filter = []

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

if st.sidebar.checkbox("Show Raw Data"):
    if menu == "Gold Summary":
        st.dataframe(df_gold)
    elif menu == "Silver Summary":
        st.dataframe(df_silver)
    elif menu == "Diamond Summary":
        st.dataframe(df_diamond)

# ========================
# KPI
# ========================

gold_total    = round(df_gold['Closing'].sum(), 3)
silver_total  = round(df_silver['Closing'].sum(), 3)
diamond_total = round(df_diamond['Closing'].sum(), 3)

c1, c2, c3 = st.columns(3)
c1.metric("Total Gold (g pure)",   gold_total)
c2.metric("Total Silver (g pure)", silver_total)
c3.metric("Total Diamond (g)",     diamond_total)

st.divider()

# ========================
# HELPER: render expandable dept table
# ========================

def render_dept_table(df, dept, weight_col='Closing', label='Pure Wt'):
    """Render one expandable dept section exactly like ERP"""

    dept_df = df[df['Dept'] == dept].copy()
    if dept_df.empty:
        return

    dept_name = DEPT_NAMES.get(dept, dept)

    # Dept totals
    tot_opening = round(dept_df['Opening'].sum(), 3)
    tot_issue   = round(dept_df['Issue'].sum(), 3)
    tot_receipt = round(dept_df['Receipt'].sum(), 3)
    tot_closing = round(dept_df[weight_col].sum(), 3)

    # Header label
    header = f"**[DJ] {dept}** — {dept_name} &nbsp;&nbsp;&nbsp; Closing: **{tot_closing}g**"

    with st.expander(header, expanded=False):

        # Column headers
        cols = st.columns([3, 4, 2, 2, 2, 2])
        cols[0].markdown("**Rm Code**")
        cols[1].markdown("**Description**")
        cols[2].markdown("**Opening**")
        cols[3].markdown("**Issue**")
        cols[4].markdown("**Receipt**")
        cols[5].markdown(f"**{label}**")

        st.markdown("---")

        # RM rows
        for _, row in dept_df.iterrows():
            cols = st.columns([3, 4, 2, 2, 2, 2])
            cols[0].write(row['RmCode'])
            cols[1].write(row['Description'] if row['Description'] else '-')
            cols[2].write(f"{row['Opening']:.3f}"  if row['Opening']  != 0 else '')
            cols[3].write(f"{row['Issue']:.3f}"    if row['Issue']    != 0 else '')
            cols[4].write(f"{row['Receipt']:.3f}"  if row['Receipt']  != 0 else '')
            cols[5].write(f"{row[weight_col]:.3f}")

        st.markdown("---")

        # Total row
        cols = st.columns([3, 4, 2, 2, 2, 2])
        cols[0].markdown(f"**Total For [DJ] {dept}**")
        cols[1].write('')
        cols[2].markdown(f"**{tot_opening:.3f}**" if tot_opening != 0 else '')
        cols[3].markdown(f"**{tot_issue:.3f}**"   if tot_issue   != 0 else '')
        cols[4].markdown(f"**{tot_receipt:.3f}**" if tot_receipt != 0 else '')
        cols[5].markdown(f"**{tot_closing:.3f}**")

# ========================
# GOLD SUMMARY
# ========================

if menu == "Gold Summary":

    st.title("Gold Summary")

    depts = dept_filter if dept_filter else GOLD_DEPTS

    for dept in depts:
        render_dept_table(df_gold, dept)

    st.divider()
    filtered = df_gold[df_gold['Dept'].isin(depts)]
    st.markdown(f"### Grand Total: **{round(filtered['Closing'].sum(), 3)}g**")

    st.download_button(
        "Download CSV",
        df_gold.to_csv(index=False),
        file_name="gold_stock.csv",
        mime="text/csv"
    )

# ========================
# SILVER SUMMARY
# ========================

elif menu == "Silver Summary":

    st.title("Silver Summary")

    depts = dept_filter if dept_filter else SILVER_DEPTS

    for dept in depts:
        render_dept_table(df_silver, dept)

    st.divider()
    filtered = df_silver[df_silver['Dept'].isin(depts)]
    st.markdown(f"### Grand Total: **{round(filtered['Closing'].sum(), 3)}g**")

    st.download_button(
        "Download CSV",
        df_silver.to_csv(index=False),
        file_name="silver_stock.csv",
        mime="text/csv"
    )

# ========================
# DIAMOND SUMMARY
# ========================

elif menu == "Diamond Summary":

    st.title("Diamond Summary")

    render_dept_table(df_diamond, DIAMOND_LOC, weight_col='Closing', label='Closing')

    st.divider()
    st.markdown(f"### Grand Total: **{round(df_diamond['Closing'].sum(), 3)}g**")

    st.download_button(
        "Download CSV",
        df_diamond.to_csv(index=False),
        file_name="diamond_stock.csv",
        mime="text/csv"
    )

# ========================
# PRODUCTION REPORT
# ========================

elif menu == "Production Report":

    st.title("Production Stock — All Metals")

    depts = dept_filter if dept_filter else GOLD_DEPTS

    # Summary pivot
    gold_grp = df_gold[df_gold['Dept'].isin(depts)].groupby('Dept')['Closing'].sum().round(3)
    silv_grp = df_silver[df_silver['Dept'].isin(depts)].groupby('Dept')['Closing'].sum().round(3)

    summary = pd.DataFrame({
        'Dept': depts,
    }).set_index('Dept')

    summary['Gold (g)']   = gold_grp
    summary['Silver (g)'] = silv_grp
    summary = summary.fillna(0).reset_index()

    # Add diamond row
    diamond_row = pd.DataFrame([{
        'Dept': 'SDIA-JW',
        'Gold (g)': 0,
        'Silver (g)': 0,
    }])
    summary = pd.concat([summary, diamond_row], ignore_index=True)

    st.dataframe(summary, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("Gold Total",    round(summary['Gold (g)'].sum(), 3))
    col2.metric("Silver Total",  round(summary['Silver (g)'].sum(), 3))
    col3.metric("Diamond Total", diamond_total)

    st.download_button(
        "Download CSV",
        summary.to_csv(index=False),
        file_name="production_report.csv",
        mime="text/csv"
    )
