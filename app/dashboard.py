"""
Cartographer Marketplace Intelligence Dashboard
Enterprise Streamlit application powered by Snowflake CARTOGRAPHER_DB.MARTS under role REPORTER.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from connection import get_reporter_connection

# 1. Page Configuration
st.set_page_config(
    page_title="Cartographer Marketplace Intelligence",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Data Access Layer with Caching
@st.cache_data(ttl=600)
def load_executive_kpis():
    """Load platform-level monthly KPIs from rpt_executive_kpis_monthly."""
    conn = get_reporter_connection()
    cur = conn.cursor()
    query = """
        SELECT 
            month,
            total_gmv,
            total_freight,
            total_seller_orders,
            total_items_sold,
            total_monitored_sellers,
            active_sellers_count,
            new_sellers_count,
            at_risk_sellers_count,
            churned_sellers_count,
            dormant_sellers_count,
            reactivated_sellers_count,
            total_sla_hits,
            total_sla_eligible,
            macro_fulfillment_sla_rate,
            total_defects,
            total_defect_eligible,
            macro_order_defect_rate,
            green_sellers_count,
            amber_sellers_count,
            red_sellers_count,
            low_volume_sellers_count,
            is_final,
            monthly_churn_rate
        FROM CARTOGRAPHER_DB.MARTS.RPT_EXECUTIVE_KPIS_MONTHLY
        ORDER BY month ASC;
    """
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    df["MONTH"] = pd.to_datetime(df["MONTH"]).dt.date
    return df

@st.cache_data(ttl=600)
def load_seller_scorecard():
    """Load partner-level monthly scorecard from rpt_seller_health_scorecard."""
    conn = get_reporter_connection()
    cur = conn.cursor()
    query = """
        SELECT 
            seller_monthly_key,
            seller_id,
            month,
            seller_city,
            seller_state,
            seller_zip_code_prefix,
            first_sale_date,
            last_sale_date,
            tenure_months_at_reporting,
            seller_orders_count,
            items_count,
            gmv,
            freight,
            sla_hits,
            sla_eligible,
            fulfillment_sla_rate,
            defects_count,
            defect_eligible,
            order_defect_rate,
            is_active_month,
            consecutive_inactive_months,
            seller_lifecycle_status,
            t3m_seller_orders,
            health_tier,
            is_final
        FROM CARTOGRAPHER_DB.MARTS.RPT_SELLER_HEALTH_SCORECARD;
    """
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    df["MONTH"] = pd.to_datetime(df["MONTH"]).dt.date
    df["FIRST_SALE_DATE"] = pd.to_datetime(df["FIRST_SALE_DATE"]).dt.date
    df["LAST_SALE_DATE"] = pd.to_datetime(df["LAST_SALE_DATE"]).dt.date
    return df

# 3. Main Dashboard Application
def main():
    st.title("🗺️ Cartographer Marketplace Intelligence")
    st.caption("Executive Supply Analytics & Operational Seller Health Engine | Role: `REPORTER`")

    with st.spinner("Connecting to Snowflake and retrieving analytics catalog..."):
        try:
            df_kpis = load_executive_kpis()
            df_scorecard = load_seller_scorecard()
        except Exception as e:
            st.error(f"Failed to connect to Snowflake or load reporting views: {e}")
            st.stop()

    # --- SIDEBAR FILTERS ---
    st.sidebar.header("Global Filters")

    # Date Slider
    all_months = sorted(df_kpis["MONTH"].unique())
    start_month, end_month = st.sidebar.select_slider(
        "Reporting Period",
        options=all_months,
        value=(all_months[0], all_months[-1]),
        format_func=lambda x: x.strftime("%Y-%m")
    )

    # Health Tier Multiselect
    tier_options = ["Green", "Amber", "Red", "insufficient_volume"]
    selected_tiers = st.sidebar.multiselect(
        "Executive Health Tiers",
        options=tier_options,
        default=tier_options
    )

    # Filter Dataframes
    df_kpis_filtered = df_kpis[
        (df_kpis["MONTH"] >= start_month) & (df_kpis["MONTH"] <= end_month)
    ]

    df_scorecard_filtered = df_scorecard[
        (df_scorecard["MONTH"] >= start_month) & 
        (df_scorecard["MONTH"] <= end_month) &
        (df_scorecard["HEALTH_TIER"].isin(selected_tiers))
    ]

    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Filtered Snapshot:**")
    st.sidebar.markdown(f"- Total Months: **{len(df_kpis_filtered)}**")
    st.sidebar.markdown(f"- Monitored Partner Rows: **{len(df_scorecard_filtered):,}**")
    st.sidebar.markdown(f"- Active Wh: `CARTOGRAPHER_WH`")
    st.sidebar.markdown(f"- Medallion Schema: `MARTS`")

    # --- TABS LAYOUT ---
    tab1, tab2, tab3 = st.tabs([
        "📊 Executive Marketplace Health",
        "🎯 Seller Performance & Health Tiers",
        "🔍 Individual Seller Drilldown"
    ])

    # =========================================================================
    # TAB 1: EXECUTIVE MARKETPLACE HEALTH
    # =========================================================================
    with tab1:
        st.subheader("Platform Supply & Quality Macro KPIs")

        if not df_kpis_filtered.empty:
            latest_row = df_kpis_filtered.iloc[-1]
            prev_row = df_kpis_filtered.iloc[-2] if len(df_kpis_filtered) > 1 else None

            # Metric Scorecards
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                gmv_val = latest_row["TOTAL_GMV"]
                gmv_delta = (gmv_val - prev_row["TOTAL_GMV"]) if prev_row is not None else None
                st.metric(
                    label=f"Monthly GMV ({latest_row['MONTH'].strftime('%b %Y')})",
                    value=f"R$ {gmv_val:,.2f}",
                    delta=f"R$ {gmv_delta:,.2f} MoM" if gmv_delta is not None else None
                )

            with col2:
                active_val = int(latest_row["ACTIVE_SELLERS_COUNT"])
                active_delta = (active_val - int(prev_row["ACTIVE_SELLERS_COUNT"])) if prev_row is not None else None
                st.metric(
                    label="Active Sellers",
                    value=f"{active_val:,}",
                    delta=f"{active_delta:+d} MoM" if active_delta is not None else None
                )

            with col3:
                sla_val = float(latest_row["MACRO_FULFILLMENT_SLA_RATE"] or 0) * 100
                st.metric(
                    label="Fleet On-Time SLA Rate",
                    value=f"{sla_val:.2f}%",
                    delta=f"{sla_val - 90.0:+.2f}% vs 90% SLA floor"
                )

            with col4:
                odr_val = float(latest_row["MACRO_ORDER_DEFECT_RATE"] or 0) * 100
                st.metric(
                    label="Fleet Order Defect Rate (ODR)",
                    value=f"{odr_val:.2f}%",
                    delta=f"{10.0 - odr_val:+.2f}% under 10% ceiling",
                    delta_color="normal"
                )

            st.markdown("---")

            # Charts Row
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("##### Monthly Gross Merchandise Value (GMV) Trend")
                fig_gmv = px.line(
                    df_kpis_filtered,
                    x="MONTH",
                    y="TOTAL_GMV",
                    labels={"TOTAL_GMV": "GMV (BRL)", "MONTH": "Month"},
                    markers=True,
                    template="plotly_white"
                )
                fig_gmv.update_traces(line_color="#1f77b4", line_width=3)
                fig_gmv.update_layout(yaxis_tickprefix="R$ ", height=400, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_gmv, use_container_width=True)

            with c2:
                st.markdown("##### Fleet Fulfillment SLA Compliance vs. Benchmark")
                sla_pct_series = df_kpis_filtered["MACRO_FULFILLMENT_SLA_RATE"] * 100
                fig_sla = go.Figure()
                fig_sla.add_trace(go.Scatter(
                    x=df_kpis_filtered["MONTH"],
                    y=sla_pct_series,
                    mode="lines+markers",
                    name="Actual SLA %",
                    line=dict(color="#2ca02c", width=3)
                ))
                # 98% aspirational target / benchmark line
                fig_sla.add_hline(
                    y=98.0,
                    line_dash="dash",
                    line_color="#d62728",
                    annotation_text="Target Benchmark (98.0%)",
                    annotation_position="bottom right"
                )
                # 90.0% operational floor
                fig_sla.add_hline(
                    y=90.0,
                    line_dash="dot",
                    line_color="#ff7f0e",
                    annotation_text="SLA Operational Floor (90.0%)",
                    annotation_position="bottom left"
                )
                fig_sla.update_layout(
                    template="plotly_white",
                    height=400,
                    yaxis=dict(title="On-Time Carrier Handoff %", range=[80, 102]),
                    margin=dict(l=20, r=20, t=30, b=20)
                )
                st.plotly_chart(fig_sla, use_container_width=True)

            # Partner Lifecycle Churn Breakdown Chart
            st.markdown("##### Monthly Partner Cohort Composition & Churn Dynamics")
            fig_lifecycle = px.bar(
                df_kpis_filtered,
                x="MONTH",
                y=["NEW_SELLERS_COUNT", "ACTIVE_SELLERS_COUNT", "AT_RISK_SELLERS_COUNT", "CHURNED_SELLERS_COUNT", "REACTIVATED_SELLERS_COUNT"],
                labels={"value": "Partner Count", "variable": "Lifecycle Stage", "MONTH": "Reporting Month"},
                title="Partner Lifecycle Progression Across Contiguous Months",
                template="plotly_white",
                color_discrete_map={
                    "NEW_SELLERS_COUNT": "#17becf",
                    "ACTIVE_SELLERS_COUNT": "#2ca02c",
                    "AT_RISK_SELLERS_COUNT": "#ffbb78",
                    "CHURNED_SELLERS_COUNT": "#d62728",
                    "REACTIVATED_SELLERS_COUNT": "#9467bd"
                }
            )
            fig_lifecycle.update_layout(height=420, legend_title_text="Lifecycle Status")
            st.plotly_chart(fig_lifecycle, use_container_width=True)

    # =========================================================================
    # TAB 2: SELLER PERFORMANCE & HEALTH TIERS
    # =========================================================================
    with tab2:
        st.subheader("Partner Health Distribution & Defect Risk Matrix")

        col_b1, col_b2 = st.columns([1, 2])

        with col_b1:
            st.markdown("##### Seller Count by Health Tier (D7)")
            tier_counts = df_scorecard_filtered["HEALTH_TIER"].value_counts().reset_index()
            tier_counts.columns = ["HEALTH_TIER", "SELLER_COUNT"]

            color_map = {
                "Green": "#2ca02c",
                "Amber": "#ff7f0e",
                "Red": "#d62728",
                "insufficient_volume": "#7f7f7f"
            }

            fig_tier = px.bar(
                tier_counts,
                x="HEALTH_TIER",
                y="SELLER_COUNT",
                color="HEALTH_TIER",
                color_discrete_map=color_map,
                text="SELLER_COUNT",
                template="plotly_white"
            )
            fig_tier.update_layout(
                showlegend=False,
                height=450,
                xaxis_title="Executive Health Tier",
                yaxis_title="Partner-Months Count",
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig_tier, use_container_width=True)

        with col_b2:
            st.markdown("##### Volume vs. Order Defect Rate (ODR) Risk Matrix")
            # Sample or filter active sellers with volume to optimize scatter rendering
            scatter_df = df_scorecard_filtered[df_scorecard_filtered["SELLER_ORDERS_COUNT"] > 0].copy()
            scatter_df["ODR_PCT"] = (scatter_df["ORDER_DEFECT_RATE"].fillna(0) * 100).round(2)
            scatter_df["SLA_PCT"] = (scatter_df["FULFILLMENT_SLA_RATE"].fillna(1.0) * 100).round(2)

            fig_scatter = px.scatter(
                scatter_df,
                x="SELLER_ORDERS_COUNT",
                y="ODR_PCT",
                color="HEALTH_TIER",
                color_discrete_map=color_map,
                hover_data=["SELLER_ID", "SELLER_STATE", "GMV", "SLA_PCT"],
                labels={
                    "SELLER_ORDERS_COUNT": "Monthly Orders Count",
                    "ODR_PCT": "Order Defect Rate (ODR %)",
                    "HEALTH_TIER": "Health Tier"
                },
                template="plotly_white"
            )
            # Reference benchmark line at 10% ODR threshold (Tier ceiling per D7)
            fig_scatter.add_hline(
                y=10.0,
                line_dash="dash",
                line_color="#d62728",
                annotation_text="Max Acceptable Defect Ceiling (10%)",
                annotation_position="top right"
            )
            # Reference benchmark line at 1.0% elite defect threshold
            fig_scatter.add_hline(
                y=1.0,
                line_dash="dot",
                line_color="#2ca02c",
                annotation_text="Elite Quality Benchmark (1.0%)",
                annotation_position="bottom right"
            )
            fig_scatter.update_layout(
                height=450,
                margin=dict(l=20, r=20, t=30, b=20)
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

    # =========================================================================
    # TAB 3: INDIVIDUAL SELLER DRILLDOWN
    # =========================================================================
    with tab3:
        st.subheader("Partner Operational Drilldown & Historical Ledger")

        # Top sellers by lifetime GMV for selection dropdown
        top_sellers = (
            df_scorecard.groupby("SELLER_ID")["GMV"]
            .sum()
            .sort_values(ascending=False)
            .index
            .tolist()
        )

        selected_seller = st.selectbox(
            "Select or Search Partner (Sorted by Realized Volume):",
            options=top_sellers,
            index=0
        )

        seller_history = df_scorecard[df_scorecard["SELLER_ID"] == selected_seller].sort_values("MONTH", ascending=False)

        if not seller_history.empty:
            latest_seller_record = seller_history.iloc[0]

            # Seller Profile Header Cards
            h1, h2, h3, h4, h5 = st.columns(5)
            with h1:
                st.markdown(f"**Location:**")
                st.write(f"{latest_seller_record['SELLER_CITY']}, {latest_seller_record['SELLER_STATE']}")
            with h2:
                st.markdown(f"**Postal Code (CEP):**")
                st.write(f"`{latest_seller_record['SELLER_ZIP_CODE_PREFIX']}`")
            with h3:
                st.markdown(f"**Active Tenure:**")
                st.write(f"{latest_seller_record['TENURE_MONTHS_AT_REPORTING']} Months")
            with h4:
                st.markdown(f"**Current Status:**")
                status = latest_seller_record['SELLER_LIFECYCLE_STATUS']
                st.write(f"`{status.upper()}`")
            with h5:
                st.markdown(f"**Current Health Tier:**")
                tier = latest_seller_record['HEALTH_TIER']
                st.write(f"**{tier}**")

            st.markdown("---")
            st.markdown("##### Historical Monthly Performance Ledger")

            # Formatted Display Table
            display_cols = [
                "MONTH",
                "GMV",
                "SELLER_ORDERS_COUNT",
                "ITEMS_COUNT",
                "FULFILLMENT_SLA_RATE",
                "ORDER_DEFECT_RATE",
                "SELLER_LIFECYCLE_STATUS",
                "CONSECUTIVE_INACTIVE_MONTHS",
                "HEALTH_TIER",
                "IS_FINAL"
            ]
            display_df = seller_history[display_cols].copy()
            display_df["GMV"] = display_df["GMV"].apply(lambda x: f"R$ {x:,.2f}")
            display_df["FULFILLMENT_SLA_RATE"] = display_df["FULFILLMENT_SLA_RATE"].apply(
                lambda x: f"{x * 100:.1f}%" if pd.notnull(x) else "N/A"
            )
            display_df["ORDER_DEFECT_RATE"] = display_df["ORDER_DEFECT_RATE"].apply(
                lambda x: f"{x * 100:.1f}%" if pd.notnull(x) else "N/A"
            )

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )

if __name__ == "__main__":
    main()
