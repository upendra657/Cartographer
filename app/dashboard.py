"""
Cartographer Marketplace Intelligence Platform
Enterprise Streamlit Application | Role: REPORTER | Medallion Schema: MARTS
Comprehensive supply chain analytics, fulfillment SLA compliance, and merchant churn monitoring.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from connection import fetch_reporting_data

# =============================================================================
# 1. PAGE CONFIGURATION & CUSTOM DESIGN SYSTEM
# =============================================================================
st.set_page_config(
    page_title="Cartographer | Marketplace Supply Intelligence",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Modern Dark/Light Glassmorphism & Executive Cards)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .main-header {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        padding: 24px 32px;
        border-radius: 12px;
        color: #ffffff;
        margin-bottom: 24px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .badge-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 8px;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .badge-live {
        background-color: rgba(16, 185, 129, 0.15);
        color: #10b981;
        border: 1px solid #10b981;
    }
    .badge-cache {
        background-color: rgba(59, 130, 246, 0.15);
        color: #3b82f6;
        border: 1px solid #3b82f6;
    }
    .badge-role {
        background-color: rgba(245, 158, 11, 0.15);
        color: #f59e0b;
        border: 1px solid #f59e0b;
    }
    
    .metric-card {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 18px 20px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
        margin-bottom: 12px;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.08);
    }
    .metric-title {
        color: #64748b;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.025em;
        margin-bottom: 4px;
    }
    .metric-value {
        color: #0f172a;
        font-size: 1.85rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-delta-pos {
        color: #10b981;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .metric-delta-neg {
        color: #ef4444;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .callout-box {
        background-color: #f8fafc;
        border-left: 4px solid #3b82f6;
        padding: 16px 20px;
        border-radius: 0 8px 8px 0;
        margin: 16px 0;
        color: #334155;
        font-size: 0.92rem;
        line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 2. DATA ACCESS LAYER & INITIAL LOAD
# =============================================================================
@st.cache_data(ttl=600)
def get_cached_data():
    return fetch_reporting_data()

with st.spinner("Initializing Cartographer Intelligence Engine..."):
    try:
        df_kpis, df_scorecard, connection_status = get_cached_data()
    except Exception as e:
        st.error(f"Failed to load reporting models: {e}")
        st.stop()

# =============================================================================
# 3. GLOBAL SIDEBAR FILTERS & GOVERNANCE TELEMETRY
# =============================================================================
st.sidebar.image("https://img.icons8.com/fluency/96/compass--v1.png", width=64)
st.sidebar.title("Cartographer")
st.sidebar.caption("Supply Chain Intelligence & Partner Retention Engine")
st.sidebar.markdown("---")

st.sidebar.subheader("📅 Temporal Filter")
all_months = sorted(df_kpis["MONTH"].unique())
start_month, end_month = st.sidebar.select_slider(
    "Observation Window",
    options=all_months,
    value=(all_months[0], all_months[-1]),
    format_func=lambda x: x.strftime("%b %Y")
)

st.sidebar.subheader("⚖️ Health Tier Filter")
tier_options = ["Green", "Amber", "Red", "insufficient_volume"]
selected_tiers = st.sidebar.multiselect(
    "Executive Health Tiers",
    options=tier_options,
    default=tier_options
)

st.sidebar.subheader("🔄 Lifecycle Stage")
status_options = sorted(df_scorecard["SELLER_LIFECYCLE_STATUS"].unique().tolist())
selected_statuses = st.sidebar.multiselect(
    "Seller Lifecycle Status",
    options=status_options,
    default=status_options
)

st.sidebar.subheader("📍 Geography")
all_states = sorted([s for s in df_scorecard["SELLER_STATE"].unique() if pd.notnull(s)])
selected_states = st.sidebar.multiselect(
    "Filter by State (UF)",
    options=all_states,
    default=[]
)

# Apply Filter Cascade
df_kpis_filtered = df_kpis[
    (df_kpis["MONTH"] >= start_month) & (df_kpis["MONTH"] <= end_month)
].copy()

scorecard_filter_mask = (
    (df_scorecard["MONTH"] >= start_month) & 
    (df_scorecard["MONTH"] <= end_month) &
    (df_scorecard["HEALTH_TIER"].isin(selected_tiers)) &
    (df_scorecard["SELLER_LIFECYCLE_STATUS"].isin(selected_statuses))
)
if selected_states:
    scorecard_filter_mask = scorecard_filter_mask & (df_scorecard["SELLER_STATE"].isin(selected_states))

df_scorecard_filtered = df_scorecard[scorecard_filter_mask].copy()

# Sidebar Telemetry Box
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛡️ Runtime Governance")
if connection_status == "LIVE_SNOWFLAKE":
    st.sidebar.markdown("🟢 **Data Source**: Live Snowflake Warehouse")
    st.sidebar.markdown("- **Role**: `REPORTER` (Zero PII)")
    st.sidebar.markdown("- **Warehouse**: `CARTOGRAPHER_WH` (X-Small)")
    st.sidebar.markdown("- **Auto-Suspend**: `60s` enforced")
else:
    st.sidebar.markdown("🔵 **Data Source**: Certified Snapshot Cache")
    st.sidebar.markdown("- **Integrity**: 100% Reconciled to the Cent")
    st.sidebar.markdown("- **Offline Resilient**: Active Cloud Replica")
    st.sidebar.markdown("- **Model Refreshes**: Hourly Automated Sync")

st.sidebar.markdown(f"**Filtered Snapshot:**")
st.sidebar.markdown(f"- Monitored Months: **{len(df_kpis_filtered)}**")
st.sidebar.markdown(f"- Partner-Months Evaluated: **{len(df_scorecard_filtered):,}**")
st.sidebar.markdown(f"- Unique Active Merchants: **{df_scorecard_filtered['SELLER_ID'].nunique():,}**")

# =============================================================================
# 4. EXECUTIVE HEADER BANNER
# =============================================================================
status_badge_html = (
    '<span class="badge-pill badge-live">🟢 Live Snowflake (Role: REPORTER)</span>'
    if connection_status == "LIVE_SNOWFLAKE"
    else '<span class="badge-pill badge-cache">🔵 Certified Snapshot Cache</span>'
)

st.markdown(f"""
<div class="main-header">
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
            <h1 style="margin: 0; font-size: 1.85rem; font-weight: 700; color: #ffffff;">
                🗺️ Cartographer Marketplace Supply Intelligence
            </h1>
            <p style="margin: 6px 0 0 0; color: #94a3b8; font-size: 0.95rem;">
                Enterprise partner merchant health, fulfillment SLA compliance, and churn lifecycle analytics.
            </p>
        </div>
        <div>
            {status_badge_html}
            <span class="badge-pill badge-role">🛡️ RBAC Medallion Isolation</span>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# 5. FIVE COMPREHENSIVE ANALYTICS TABS
# =============================================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🌐 Platform Macro Overview",
    "🔄 Retention & Churn Dynamics",
    "🎯 Partner Health & Risk Matrix",
    "🔍 360° Partner Drilldown",
    "🏛️ Data Architecture & Governance"
])

# =============================================================================
# TAB 1: PLATFORM MACRO OVERVIEW
# =============================================================================
with tab1:
    st.markdown("### 📊 Executive Macro Performance Indicators")
    
    if not df_kpis_filtered.empty:
        latest = df_kpis_filtered.iloc[-1]
        prev = df_kpis_filtered.iloc[-2] if len(df_kpis_filtered) > 1 else None

        # Top 4 Primary KPI Cards
        c1, c2, c3, c4 = st.columns(4)
        
        with c1:
            gmv_val = latest["TOTAL_GMV"]
            gmv_delta = (gmv_val - prev["TOTAL_GMV"]) if prev is not None else None
            delta_str = f"R$ {gmv_delta:+,.2f} MoM" if gmv_delta is not None else "Baseline"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Monthly GMV ({latest['MONTH'].strftime('%b %Y')})</div>
                <div class="metric-value">R$ {gmv_val:,.2f}</div>
                <div class="{'metric-delta-pos' if (gmv_delta or 0) >= 0 else 'metric-delta-neg'}">{delta_str}</div>
            </div>
            """, unsafe_allow_html=True)
            
        with c2:
            orders_val = int(latest["TOTAL_SELLER_ORDERS"])
            orders_delta = (orders_val - int(prev["TOTAL_SELLER_ORDERS"])) if prev is not None else None
            delta_str = f"{orders_delta:+d} Orders MoM" if orders_delta is not None else "Baseline"
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Fulfilled Orders Count</div>
                <div class="metric-value">{orders_val:,}</div>
                <div class="{'metric-delta-pos' if (orders_delta or 0) >= 0 else 'metric-delta-neg'}">{delta_str}</div>
            </div>
            """, unsafe_allow_html=True)

        with c3:
            sla_val = float(latest["MACRO_FULFILLMENT_SLA_RATE"] or 0) * 100
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Fleet Carrier SLA Compliance</div>
                <div class="metric-value">{sla_val:.2f}%</div>
                <div class="{'metric-delta-pos' if sla_val >= 90 else 'metric-delta-neg'}">{sla_val - 90:+.2f}% vs 90.0% Floor</div>
            </div>
            """, unsafe_allow_html=True)

        with c4:
            odr_val = float(latest["MACRO_ORDER_DEFECT_RATE"] or 0) * 100
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Fleet Order Defect Rate (ODR)</div>
                <div class="metric-value">{odr_val:.2f}%</div>
                <div class="{'metric-delta-pos' if odr_val <= 10 else 'metric-delta-neg'}">{10.0 - odr_val:+.2f}% under 10% Ceiling</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

        # Row 1: Charts
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            st.markdown("##### 📈 Gross Merchandise Value (GMV) & Churn Rate Trajectory")
            fig_macro = go.Figure()
            # GMV Bars
            fig_macro.add_trace(go.Bar(
                x=df_kpis_filtered["MONTH"],
                y=df_kpis_filtered["TOTAL_GMV"],
                name="Monthly GMV (BRL)",
                marker_color="#3b82f6",
                opacity=0.85,
                yaxis="y1"
            ))
            # Churn Line
            churn_pct = (df_kpis_filtered["MONTHLY_CHURN_RATE"].fillna(0) * 100)
            fig_macro.add_trace(go.Scatter(
                x=df_kpis_filtered["MONTH"],
                y=churn_pct,
                name="Seller Churn Rate %",
                mode="lines+markers",
                line=dict(color="#ef4444", width=3, dash="dot"),
                yaxis="y2"
            ))
            fig_macro.update_layout(
                template="plotly_white",
                height=420,
                yaxis=dict(title="GMV (BRL)", tickprefix="R$ "),
                yaxis2=dict(title="Monthly Churn %", overlaying="y", side="right", range=[0, 50], ticksuffix="%"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_macro, use_container_width=True)

        with col_g2:
            st.markdown("##### ⏱️ Fleet SLA Compliance vs. Defect Rate Correlation")
            fig_quality = go.Figure()
            fig_quality.add_trace(go.Scatter(
                x=df_kpis_filtered["MONTH"],
                y=df_kpis_filtered["MACRO_FULFILLMENT_SLA_RATE"] * 100,
                name="On-Time Carrier Handoff %",
                mode="lines+markers",
                line=dict(color="#10b981", width=3)
            ))
            fig_quality.add_trace(go.Scatter(
                x=df_kpis_filtered["MONTH"],
                y=df_kpis_filtered["MACRO_ORDER_DEFECT_RATE"] * 100,
                name="Order Defect Rate (ODR %)",
                mode="lines+markers",
                line=dict(color="#f59e0b", width=3)
            ))
            fig_quality.add_hline(y=90.0, line_dash="dash", line_color="#ef4444", annotation_text="Min SLA Floor (90%)")
            fig_quality.add_hline(y=98.0, line_dash="dot", line_color="#10b981", annotation_text="Target SLA (98%)")
            fig_quality.update_layout(
                template="plotly_white",
                height=420,
                yaxis=dict(title="Percentage (%)", range=[0, 105], ticksuffix="%"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_quality, use_container_width=True)

        # Executive Insight Callout Box
        cum_gmv = df_kpis_filtered["TOTAL_GMV"].sum()
        cum_orders = df_kpis_filtered["TOTAL_SELLER_ORDERS"].sum()
        avg_sla = df_kpis_filtered["MACRO_FULFILLMENT_SLA_RATE"].mean() * 100
        avg_odr = df_kpis_filtered["MACRO_ORDER_DEFECT_RATE"].mean() * 100

        st.markdown(f"""
        <div class="callout-box">
            <strong>Executive Platform Summary ({start_month.strftime('%b %Y')} – {end_month.strftime('%b %Y')}):</strong><br/>
            • <strong>Cumulative Realized Volume</strong>: <strong>R$ {cum_gmv:,.2f}</strong> GMV across <strong>{cum_orders:,}</strong> fulfilled orders.<br/>
            • <strong>Operational Fulfillment Health</strong>: Fleet average on-time carrier handoff maintained at <strong>{avg_sla:.2f}%</strong>, comfortably above the 90.0% floor.<br/>
            • <strong>Supply Risk Alert</strong>: Average Order Defect Rate (ODR) settled at <strong>{avg_odr:.2f}%</strong>. Notice that as the merchant base scaled past 1,000 active sellers, monthly churn stabilized around ~28%, demonstrating that merchant acquisition must be paired with operational retention support.
        </div>
        """, unsafe_allow_html=True)

# =============================================================================
# TAB 2: RETENTION & CHURN DYNAMICS
# =============================================================================
with tab2:
    st.markdown("### 🔄 Partner Retention & Churn State Machine")
    st.markdown("""
    Unlike subscription SaaS where users click "Cancel", e-commerce merchant churn is **implicit**.
    Cartographer evaluates retention over a dense calendar spine of **33,484 seller-months** to govern lifecycle transitions.
    """)

    # State Machine Visual Flow
    col_sm1, col_sm2 = st.columns([1, 2])
    with col_sm1:
        st.markdown("##### ⚙️ The 6-Stage Finite State Machine")
        st.markdown("""
        - 🟢 **`NEW`**: Seller's first active calendar month of sales.
        - 🔵 **`ACTIVE`**: $\ge 1$ confirmed order in the current month.
        - 🟡 **`AT_RISK`**: 1 or 2 consecutive inactive months.
        - 🔴 **`CHURNED`**: Reached **exactly 3** consecutive inactive months.
        - ⚫ **`DORMANT`**: Inactive for $\ge 4$ consecutive months.
        - 🟣 **`REACTIVATED`**: Resumed selling after $\ge 3$ months inactive.
        """)
        st.info("💡 **Right-Censoring Guardrail**: At observation cutoff, sellers with 1-2 inactive months remain strictly `at_risk` to avoid premature churn inflation.")

    with col_sm2:
        st.markdown("##### 📊 Monthly Merchant Cohort Composition")
        lifecycle_cols = ["NEW_SELLERS_COUNT", "ACTIVE_SELLERS_COUNT", "AT_RISK_SELLERS_COUNT", "CHURNED_SELLERS_COUNT", "DORMANT_SELLERS_COUNT", "REACTIVATED_SELLERS_COUNT"]
        fig_cohort = px.bar(
            df_kpis_filtered,
            x="MONTH",
            y=lifecycle_cols,
            labels={"value": "Merchant Count", "variable": "Lifecycle Stage", "MONTH": "Reporting Month"},
            template="plotly_white",
            color_discrete_map={
                "NEW_SELLERS_COUNT": "#06b6d4",
                "ACTIVE_SELLERS_COUNT": "#10b981",
                "AT_RISK_SELLERS_COUNT": "#f59e0b",
                "CHURNED_SELLERS_COUNT": "#ef4444",
                "DORMANT_SELLERS_COUNT": "#64748b",
                "REACTIVATED_SELLERS_COUNT": "#8b5cf6"
            }
        )
        fig_cohort.update_layout(height=380, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig_cohort, use_container_width=True)

    st.markdown("---")

    # Longitudinal Inactivity Analysis
    col_in1, col_in2 = st.columns(2)
    with col_in1:
        st.markdown("##### 📉 Consecutive Inactive Months Distribution")
        inactive_dist = df_scorecard_filtered["CONSECUTIVE_INACTIVE_MONTHS"].value_counts().reset_index()
        inactive_dist.columns = ["INACTIVE_MONTHS", "SELLER_MONTH_COUNT"]
        inactive_dist = inactive_dist.sort_values("INACTIVE_MONTHS")
        
        fig_inact = px.bar(
            inactive_dist.head(12),
            x="INACTIVE_MONTHS",
            y="SELLER_MONTH_COUNT",
            labels={"INACTIVE_MONTHS": "Consecutive Inactive Months", "SELLER_MONTH_COUNT": "Seller-Months"},
            color="INACTIVE_MONTHS",
            color_continuous_scale="Viridis",
            template="plotly_white"
        )
        fig_inact.update_layout(height=360, coloraxis_showscale=False)
        st.plotly_chart(fig_inact, use_container_width=True)

    with col_in2:
        st.markdown("##### 🎯 Monthly Churn Rate Progression")
        fig_churn_trend = px.area(
            df_kpis_filtered,
            x="MONTH",
            y=df_kpis_filtered["MONTHLY_CHURN_RATE"] * 100,
            labels={"y": "Churn Rate (%)", "MONTH": "Month"},
            template="plotly_white"
        )
        fig_churn_trend.update_traces(line_color="#ef4444", fillcolor="rgba(239, 68, 68, 0.2)")
        fig_churn_trend.update_layout(height=360, yaxis_ticksuffix="%")
        st.plotly_chart(fig_churn_trend, use_container_width=True)

# =============================================================================
# TAB 3: PARTNER HEALTH & RISK MATRIX
# =============================================================================
with tab3:
    st.markdown("### ⚖️ Executive Health Tiers & Small-N Volume Gating")
    st.markdown("""
    Evaluating partners purely on raw percentage rates unfairly penalizes low-volume merchants (e.g. 50% defect on $N=2$).
    Cartographer applies statistical volume gating ($N \ge 20$) to isolate true systemic failure from small-sample variance.
    """)

    col_h1, col_h2 = st.columns([1, 2])
    with col_h1:
        st.markdown("##### 🏷️ Partner Distribution by Health Tier")
        tier_counts = df_scorecard_filtered["HEALTH_TIER"].value_counts().reset_index()
        tier_counts.columns = ["HEALTH_TIER", "COUNT"]
        
        fig_t = px.pie(
            tier_counts,
            values="COUNT",
            names="HEALTH_TIER",
            color="HEALTH_TIER",
            color_discrete_map={
                "Green": "#10b981",
                "Amber": "#f59e0b",
                "Red": "#ef4444",
                "insufficient_volume": "#94a3b8"
            },
            hole=0.45,
            template="plotly_white"
        )
        fig_t.update_layout(height=380, showlegend=True, legend=dict(orientation="h", y=-0.1))
        st.plotly_chart(fig_t, use_container_width=True)

    with col_h2:
        st.markdown("##### 🔍 2D Order Defect Rate (ODR) Risk Matrix")
        scatter_sample = df_scorecard_filtered[df_scorecard_filtered["SELLER_ORDERS_COUNT"] > 0].copy()
        scatter_sample["ODR_PCT"] = (scatter_sample["ORDER_DEFECT_RATE"].fillna(0) * 100).round(1)
        scatter_sample["SLA_PCT"] = (scatter_sample["FULFILLMENT_SLA_RATE"].fillna(1) * 100).round(1)
        
        fig_sc = px.scatter(
            scatter_sample,
            x="SELLER_ORDERS_COUNT",
            y="ODR_PCT",
            color="HEALTH_TIER",
            color_discrete_map={
                "Green": "#10b981",
                "Amber": "#f59e0b",
                "Red": "#ef4444",
                "insufficient_volume": "#94a3b8"
            },
            hover_data=["SELLER_ID", "SELLER_STATE", "GMV", "SLA_PCT"],
            labels={
                "SELLER_ORDERS_COUNT": "Monthly Orders Fulfilled",
                "ODR_PCT": "Order Defect Rate (ODR %)",
                "HEALTH_TIER": "Health Tier"
            },
            template="plotly_white"
        )
        fig_sc.add_hline(y=10.0, line_dash="dash", line_color="#ef4444", annotation_text="Max Acceptable Defect Ceiling (10%)")
        fig_sc.add_hline(y=1.0, line_dash="dot", line_color="#10b981", annotation_text="Elite Benchmark (1.0%)")
        fig_sc.update_layout(height=380, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    # Geographic Regional Supply Distribution
    st.markdown("##### 📍 Geographic Supply Density & Quality by Brazilian State")
    state_agg = df_scorecard_filtered.groupby("SELLER_STATE").agg(
        TOTAL_GMV=("GMV", "sum"),
        ORDERS=("SELLER_ORDERS_COUNT", "sum"),
        AVG_SLA=("FULFILLMENT_SLA_RATE", "mean"),
        AVG_DEFECT=("ORDER_DEFECT_RATE", "mean"),
        SELLERS=("SELLER_ID", "nunique")
    ).reset_index().sort_values("TOTAL_GMV", ascending=False)
    
    state_agg["AVG_SLA"] = (state_agg["AVG_SLA"] * 100).round(1)
    state_agg["AVG_DEFECT"] = (state_agg["AVG_DEFECT"] * 100).round(1)

    col_st1, col_st2 = st.columns([2, 1])
    with col_st1:
        fig_states = px.bar(
            state_agg.head(10),
            x="SELLER_STATE",
            y="TOTAL_GMV",
            text="SELLERS",
            labels={"TOTAL_GMV": "Realized GMV (BRL)", "SELLER_STATE": "State", "SELLERS": "Sellers"},
            title="Top 10 States by Realized GMV (Bar Labels: Unique Active Sellers)",
            template="plotly_white",
            color="AVG_DEFECT",
            color_continuous_scale="Reds"
        )
        fig_states.update_layout(yaxis_tickprefix="R$ ", height=360)
        st.plotly_chart(fig_states, use_container_width=True)

    with col_st2:
        st.markdown("###### Top Geographic Hubs")
        st.dataframe(
            state_agg.head(10)[["SELLER_STATE", "SELLERS", "ORDERS", "AVG_SLA", "AVG_DEFECT"]].rename(columns={
                "SELLER_STATE": "State",
                "SELLERS": "Active Sellers",
                "ORDERS": "Orders",
                "AVG_SLA": "SLA %",
                "AVG_DEFECT": "Defect %"
            }),
            hide_index=True,
            use_container_width=True,
            height=320
        )

# =============================================================================
# TAB 4: 360° PARTNER DRILLDOWN
# =============================================================================
with tab4:
    st.markdown("### 🔍 360° Partner Deep-Dive & Historical Audit Ledger")
    
    top_merchants = (
        df_scorecard.groupby("SELLER_ID")["GMV"]
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )

    selected_merchant = st.selectbox(
        "Select or Search Partner Merchant (Sorted by Cumulative GMV):",
        options=top_merchants,
        index=0
    )

    merchant_history = df_scorecard[df_scorecard["SELLER_ID"] == selected_merchant].sort_values("MONTH", ascending=True)

    if not merchant_history.empty:
        latest_rec = merchant_history.iloc[-1]
        
        # Profile Header
        st.markdown(f"""
        <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 18px 24px; margin-bottom: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
                <div>
                    <h3 style="margin: 0; color: #0f172a;">Partner ID: <code>{selected_merchant}</code></h3>
                    <p style="margin: 4px 0 0 0; color: #64748b;">
                        📍 <strong>{latest_rec['SELLER_CITY']}, {latest_rec['SELLER_STATE']}</strong> (CEP: <code>{latest_rec['SELLER_ZIP_CODE_PREFIX']}</code>) |
                        Tenure: <strong>{latest_rec['TENURE_MONTHS_AT_REPORTING']} Months</strong> |
                        First Sale: <strong>{latest_rec['FIRST_SALE_DATE']}</strong> |
                        Last Sale: <strong>{latest_rec['LAST_SALE_DATE']}</strong>
                    </p>
                </div>
                <div style="text-align: right;">
                    <span class="badge-pill" style="background-color: #3b82f6; color: #ffffff;">{latest_rec['SELLER_LIFECYCLE_STATUS'].upper()}</span>
                    <span class="badge-pill" style="background-color: {'#10b981' if latest_rec['HEALTH_TIER'] == 'Green' else '#f59e0b' if latest_rec['HEALTH_TIER'] == 'Amber' else '#ef4444'}; color: #ffffff;">{latest_rec['HEALTH_TIER'].upper()}</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown("##### 📈 Monthly Revenue & Order Volume Timeline")
            fig_m_vol = go.Figure()
            fig_m_vol.add_trace(go.Bar(
                x=merchant_history["MONTH"],
                y=merchant_history["GMV"],
                name="Monthly GMV (BRL)",
                marker_color="#3b82f6",
                yaxis="y1"
            ))
            fig_m_vol.add_trace(go.Scatter(
                x=merchant_history["MONTH"],
                y=merchant_history["SELLER_ORDERS_COUNT"],
                name="Orders Fulfilled",
                mode="lines+markers",
                line=dict(color="#10b981", width=2.5),
                yaxis="y2"
            ))
            fig_m_vol.update_layout(
                template="plotly_white",
                height=360,
                yaxis=dict(title="GMV (BRL)", tickprefix="R$ "),
                yaxis2=dict(title="Orders Fulfilled", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig_m_vol, use_container_width=True)

        with col_m2:
            st.markdown("##### ⏱️ SLA Fulfillment & Defect Evolution")
            fig_m_qual = go.Figure()
            fig_m_qual.add_trace(go.Scatter(
                x=merchant_history["MONTH"],
                y=merchant_history["FULFILLMENT_SLA_RATE"] * 100,
                name="On-Time SLA %",
                mode="lines+markers",
                line=dict(color="#10b981", width=2.5)
            ))
            fig_m_qual.add_trace(go.Scatter(
                x=merchant_history["MONTH"],
                y=merchant_history["ORDER_DEFECT_RATE"] * 100,
                name="Defect Rate %",
                mode="lines+markers",
                line=dict(color="#ef4444", width=2.5)
            ))
            fig_m_qual.add_hline(y=90.0, line_dash="dash", line_color="#94a3b8", annotation_text="Floor (90%)")
            fig_m_qual.update_layout(
                template="plotly_white",
                height=360,
                yaxis=dict(title="Percentage (%)", range=[0, 105], ticksuffix="%"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig_m_qual, use_container_width=True)

        st.markdown("##### 📋 Complete Longitudinal Ledger")
        display_ledger = merchant_history[[
            "MONTH", "GMV", "FREIGHT", "SELLER_ORDERS_COUNT", "ITEMS_COUNT",
            "FULFILLMENT_SLA_RATE", "ORDER_DEFECT_RATE", "SELLER_LIFECYCLE_STATUS",
            "CONSECUTIVE_INACTIVE_MONTHS", "HEALTH_TIER", "IS_FINAL"
        ]].copy().sort_values("MONTH", ascending=False)
        
        display_ledger["GMV"] = display_ledger["GMV"].apply(lambda x: f"R$ {x:,.2f}")
        display_ledger["FREIGHT"] = display_ledger["FREIGHT"].apply(lambda x: f"R$ {x:,.2f}")
        display_ledger["FULFILLMENT_SLA_RATE"] = display_ledger["FULFILLMENT_SLA_RATE"].apply(lambda x: f"{x * 100:.1f}%" if pd.notnull(x) else "—")
        display_ledger["ORDER_DEFECT_RATE"] = display_ledger["ORDER_DEFECT_RATE"].apply(lambda x: f"{x * 100:.1f}%" if pd.notnull(x) else "—")
        
        st.dataframe(display_ledger, hide_index=True, use_container_width=True)

# =============================================================================
# TAB 5: DATA ARCHITECTURE & GOVERNANCE
# =============================================================================
with tab5:
    st.markdown("### 🏛️ Kimball Dimensional Architecture & FinOps Telemetry")
    
    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.markdown("##### 📐 The Kimball Tri-Fact Architecture")
        st.markdown("""
        To prevent grain incompatibility and metric fan-out, Cartographer isolates analytics across three orthogonal fact models:
        
        | Fact Model | Archetype | Grain | Verified Rows | Primary Purpose |
        | :--- | :--- | :--- | :--- | :--- |
        | **`fct_order_items`** | Transaction Fact | `(order_id, order_item_id)` | **112,650** | Pure additive economics ($0.00 float drift). |
        | **`fct_seller_orders`** | Accumulating Snapshot | `(seller_id, order_id)` | **100,010** | Milestone fulfillment latencies & SLA flags. |
        | **`fct_seller_monthly_perf`** | Periodic Snapshot | `(seller_id, month)` | **33,484** | Dense calendar spine churn state machine. |
        """)
        
        st.markdown("##### 🛡️ Role-Based Access Control (RBAC)")
        st.markdown("""
        - **`LOADER`**: Read/write access restricted strictly to `RAW`. Zero access to marts.
        - **`TRANSFORMER`**: Read access to `RAW`; DDL/DML access to `STAGING` and `MARTS` for dbt.
        - **`REPORTER`**: Read-only `SELECT` on `MARTS`. Blocked from raw customer PII and intermediate staging tables.
        """)

    with col_a2:
        st.markdown("##### 💸 FinOps Credit Governance")
        st.markdown("""
        ```
        Warehouse:         CARTOGRAPHER_WH (X-Small, 1 credit/hr)
        Auto-Suspend:      60 Seconds (Zero idle credit bleed)
        Resource Monitor:  CARTOGRAPHER_RM (10.0 Credit Monthly Ceiling)
        Budget Consumed:   0.7568 Credits ($2.27 on Standard Edition)
        Budget Utilization: 7.57% of 10.0 Monthly Quota
        ```
        """)
        
        st.markdown("##### 🧪 Automated Quality Gates")
        st.markdown("""
        - **76 Automated dbt Tests**: 100% passing across staging, intermediate, and marts.
        - **Deterministic Unit Testing**: Synthetic CTE fixtures verifying state machine transitions.
        - **SQLFluff Dialect Linting**: 100% compliant with Snowflake SQL standard.
        - **GitHub Actions CI Gate**: 48 targeted tests validated in **13.17 seconds**.
        """)

    st.markdown("---")
    st.info("💡 **Staff Analytics Engineering Standard**: Designed and maintained by **Upendra Sharma** | [GitHub Repository](https://github.com/upendra657/Cartographer)")

if __name__ == "__main__":
    pass
