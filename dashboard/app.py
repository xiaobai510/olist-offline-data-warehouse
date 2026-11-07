from pathlib import Path
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


PROJECT_DIR = Path(__file__).resolve().parents[1]
ADS_DIR = Path(
    os.environ.get("OLIST_ADS_DIR", str(PROJECT_DIR / "data" / "ads"))
)

st.set_page_config(
    page_title="Olist 电商经营分析看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=300, show_spinner=False)
def load_ads(table_name: str) -> pd.DataFrame:
    path = ADS_DIR / table_name
    if not path.exists():
        raise FileNotFoundError(f"找不到 ADS 数据：{path}")

    frame = pd.read_parquet(path)

    date_columns = [
        "analysis_start_date",
        "analysis_end_date",
        "month_start_date",
    ]
    for column in date_columns:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")

    return frame


def as_float(value, default=0.0) -> float:
    if pd.isna(value):
        return default
    return float(value)


def money(value) -> str:
    return f"BRL {as_float(value):,.2f}"


def percent(value, digits: int = 2) -> str:
    return f"{as_float(value) * 100:.{digits}f}%"


def apply_chart_style(figure, height: int = 420):
    figure.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
        ),
    )
    return figure


try:
    overview = load_ads("business_overview").iloc[0]
    sales_trend = load_ads("sales_trend")
    category_ranking = load_ads("category_ranking")
    seller_ranking = load_ads("seller_ranking")
    rfm_summary = load_ads("rfm_summary")
    delivery_summary = load_ads("delivery_summary")
except (FileNotFoundError, OSError, ValueError) as error:
    st.error(str(error))
    st.info(
        "请先运行 build_dws.py 和 build_ads.py，确认 "
        "D:\\pycharm\\PythonProject\\data\\ads 已生成。"
    )
    st.stop()


st.markdown(
    """
    <style>
        .block-container {padding-top: 1.8rem; padding-bottom: 2rem;}
        [data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e9edf2;
            border-radius: 12px;
            padding: 14px 16px;
            box-shadow: 0 2px 8px rgba(20, 37, 63, 0.05);
        }
        [data-testid="stMetricLabel"] {font-weight: 600;}
    </style>
    """,
    unsafe_allow_html=True,
)


st.title("📊 Olist 电商经营分析看板")
st.caption(
    "离线数仓项目 · "
    f"{overview['analysis_start_date']:%Y-%m-%d} 至 "
    f"{overview['analysis_end_date']:%Y-%m-%d} · "
    "指标数据来自 ADS 层"
)


with st.sidebar:
    st.header("筛选条件")

    min_month = sales_trend["month_start_date"].min().date()
    max_month = sales_trend["month_start_date"].max().date()
    selected_range = st.date_input(
        "销售趋势时间范围",
        value=(min_month, max_month),
        min_value=min_month,
        max_value=max_month,
        help="时间筛选只作用于月度趋势；其他排行展示整个分析周期。",
    )

    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        start_date, end_date = selected_range
    else:
        start_date, end_date = min_month, max_month

    st.divider()
    st.subheader("项目信息")
    st.write("数据仓库：`ODS → DWD → DWS → ADS`")
    st.write("计算引擎：`Spark SQL`")
    st.write("展示框架：`Streamlit + Plotly`")
    st.write(f"ADS 目录：`{ADS_DIR}`")
    st.caption(
        "取消和不可用订单计入订单总量，但不计入有效 GMV。"
    )


overview_tab, sales_tab, merchant_tab, customer_tab, delivery_tab, glossary_tab = (
    st.tabs(
        [
            "经营总览",
            "销售分析",
            "商品与卖家",
            "客户运营",
            "履约质量",
            "数据口径",
        ]
    )
)


with overview_tab:
    st.subheader("核心经营指标")

    first_row = st.columns(4)
    first_row[0].metric(
        "有效 GMV",
        money(overview["product_gmv"]),
        help="排除取消和不可用订单后的商品销售额。",
    )
    first_row[1].metric(
        "有效订单",
        f"{int(overview['valid_order_count']):,}",
    )
    first_row[2].metric(
        "平均客单价",
        money(overview["average_order_value"]),
    )
    first_row[3].metric(
        "商品件数",
        f"{int(overview['item_count']):,}",
    )

    second_row = st.columns(4)
    second_row[0].metric(
        "取消率",
        percent(overview["cancel_rate"]),
    )
    second_row[1].metric(
        "独立客户",
        f"{int(overview['customer_count']):,}",
    )
    second_row[2].metric(
        "复购率",
        percent(overview["repeat_customer_rate"]),
    )
    second_row[3].metric(
        "延迟送达率",
        percent(overview["late_delivery_rate"]),
    )

    st.divider()
    highlight_columns = st.columns(3)
    highlight_columns[0].metric(
        "销售第一品类",
        str(overview["top_category"]).replace("_", " ").title(),
        money(overview["top_category_gmv"]),
    )
    highlight_columns[1].metric(
        "销售第一卖家",
        str(overview["top_seller_id"])[:8] + "…",
        money(overview["top_seller_gmv"]),
    )
    highlight_columns[2].metric(
        "平均配送时长",
        f"{as_float(overview['average_delivery_days']):.1f} 天",
    )

    st.subheader("业务观察")
    st.markdown(
        f"""
        - 分析周期共 **{int(overview['analysis_days'])} 天**，
          产生 **{int(overview['order_count']):,}** 个订单。
        - 有效支付金额为 **{money(overview['payment_amount'])}**，
          平均每笔支付 **{money(overview['average_payment_value'])}**。
        - 客户复购率为 **{percent(overview['repeat_customer_rate'])}**，
          客户运营仍有提升空间。
        - 已送达订单中延迟率为
          **{percent(overview['late_delivery_rate'])}**，
          平均配送需要
          **{as_float(overview['average_delivery_days']):.1f} 天**。
        """
    )


with sales_tab:
    st.subheader("月度销售趋势")

    trend = sales_trend[
        sales_trend["month_start_date"].dt.date.between(start_date, end_date)
    ].copy()

    for column in [
        "product_gmv",
        "payment_amount",
        "average_order_value",
    ]:
        trend[column] = pd.to_numeric(trend[column], errors="coerce")

    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(
        go.Bar(
            x=trend["month_start_date"],
            y=trend["product_gmv"],
            name="有效 GMV",
            marker_color="#2F6BFF",
            hovertemplate="%{x|%Y-%m}<br>有效 GMV：BRL %{y:,.2f}<extra></extra>",
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=trend["month_start_date"],
            y=trend["valid_order_count"],
            name="有效订单",
            mode="lines+markers",
            line=dict(color="#F59E0B", width=3),
            hovertemplate="%{x|%Y-%m}<br>有效订单：%{y:,.0f}<extra></extra>",
        ),
        secondary_y=True,
    )
    figure.update_yaxes(
        title_text="有效 GMV（BRL）",
        tickformat=",.0f",
        secondary_y=False,
    )
    figure.update_yaxes(
        title_text="有效订单",
        tickformat=",.0f",
        secondary_y=True,
    )
    figure.update_xaxes(title_text="月份")
    figure.update_layout(
        title="有效 GMV 与订单量",
        hovermode="x unified",
    )
    st.plotly_chart(
        apply_chart_style(figure, 460),
        use_container_width=True,
    )

    latest = trend.sort_values("month_start_date").tail(1)
    if not latest.empty:
        latest_row = latest.iloc[0]
        columns = st.columns(3)
        columns[0].metric(
            "最新月有效 GMV",
            money(latest_row["product_gmv"]),
        )
        columns[1].metric(
            "最新月有效订单",
            f"{int(latest_row['valid_order_count']):,}",
        )
        columns[2].metric(
            "最新月 GMV 环比",
            percent(latest_row["gmv_mom_rate"])
            if pd.notna(latest_row["gmv_mom_rate"])
            else "无上月数据",
        )

    trend_table = trend[
        [
            "year_month",
            "valid_order_count",
            "product_gmv",
            "payment_amount",
            "average_order_value",
            "gmv_mom_rate",
        ]
    ].copy()
    trend_table["gmv_mom_rate"] = (
        pd.to_numeric(trend_table["gmv_mom_rate"], errors="coerce") * 100
    )
    st.dataframe(
        trend_table.rename(
            columns={
                "year_month": "月份",
                "valid_order_count": "有效订单",
                "product_gmv": "有效 GMV（BRL）",
                "payment_amount": "支付金额（BRL）",
                "average_order_value": "平均客单价（BRL）",
                "gmv_mom_rate": "GMV 环比（%）",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            "GMV 环比（%）": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )


with merchant_tab:
    st.subheader("品类销售排行")

    category = category_ranking.copy()
    category["product_gmv"] = pd.to_numeric(
        category["product_gmv"], errors="coerce"
    )
    category["gmv_share"] = pd.to_numeric(
        category["gmv_share"], errors="coerce"
    )
    category["category_display"] = (
        category["category_name_english"]
        .astype(str)
        .str.replace("_", " ")
        .str.title()
    )

    top_categories = category.nlargest(15, "product_gmv").sort_values(
        "product_gmv"
    )
    category_figure = px.bar(
        top_categories,
        x="product_gmv",
        y="category_display",
        orientation="h",
        color="product_gmv",
        color_continuous_scale="Blues",
        title="品类有效 GMV Top 15",
        labels={
            "product_gmv": "有效 GMV（BRL）",
            "category_display": "品类",
        },
        hover_data={
            "item_count": ":,",
            "gmv_share": ":.2%",
            "product_gmv": ":,.2f",
            "category_display": False,
        },
    )
    category_figure.update_coloraxes(showscale=False)
    st.plotly_chart(
        apply_chart_style(category_figure, 540),
        use_container_width=True,
    )

    st.divider()
    st.subheader("卖家销售排行")
    top_n = st.slider("显示卖家数量", 10, 50, 20, 10)
    seller = seller_ranking.nsmallest(top_n, "sales_rank").copy()
    for column in [
        "product_gmv",
        "freight_revenue",
        "average_order_value",
        "gmv_share",
        "late_delivery_rate",
    ]:
        seller[column] = pd.to_numeric(seller[column], errors="coerce")
    seller["gmv_share_pct"] = seller["gmv_share"] * 100
    seller["late_delivery_rate_pct"] = seller["late_delivery_rate"] * 100

    st.dataframe(
        seller[
            [
                "sales_rank",
                "seller_id",
                "seller_state",
                "order_count",
                "item_count",
                "product_gmv",
                "average_order_value",
                "gmv_share_pct",
                "late_delivery_rate_pct",
            ]
        ].rename(
            columns={
                "sales_rank": "排名",
                "seller_id": "卖家 ID",
                "seller_state": "州",
                "order_count": "订单数",
                "item_count": "商品件数",
                "product_gmv": "有效 GMV（BRL）",
                "average_order_value": "平均客单价（BRL）",
                "gmv_share_pct": "GMV 占比（%）",
                "late_delivery_rate_pct": "延迟率（%）",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            "有效 GMV（BRL）": st.column_config.NumberColumn(format="%.2f"),
            "平均客单价（BRL）": st.column_config.NumberColumn(format="%.2f"),
            "GMV 占比（%）": st.column_config.ProgressColumn(
                format="%.3f%%",
                min_value=0,
                max_value=max(as_float(seller["gmv_share_pct"].max()), 0.1),
            ),
            "延迟率（%）": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )


with customer_tab:
    st.subheader("客户 RFM 分层")

    rfm = rfm_summary.copy()
    for column in [
        "customer_count",
        "total_monetary",
        "average_monetary",
        "average_frequency",
        "average_recency_days",
        "customer_share",
        "monetary_share",
    ]:
        rfm[column] = pd.to_numeric(rfm[column], errors="coerce")
    rfm["customer_share_pct"] = rfm["customer_share"] * 100
    rfm["monetary_share_pct"] = rfm["monetary_share"] * 100

    rfm["segment_display"] = (
        rfm["rfm_segment"]
        .astype(str)
        .str.replace("_", " ")
        .str.title()
    )

    left, right = st.columns([1, 1.4])
    with left:
        pie_figure = px.pie(
            rfm,
            names="segment_display",
            values="customer_count",
            title="客户数量分布",
            hole=0.52,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        pie_figure.update_traces(
            textposition="inside",
            textinfo="percent+label",
        )
        st.plotly_chart(
            apply_chart_style(pie_figure, 440),
            use_container_width=True,
        )

    with right:
        monetary_figure = px.bar(
            rfm.sort_values("total_monetary"),
            x="total_monetary",
            y="segment_display",
            orientation="h",
            title="各客户分层贡献金额",
            color="total_monetary",
            color_continuous_scale="Teal",
            labels={
                "total_monetary": "支付金额（BRL）",
                "segment_display": "客户分层",
            },
            hover_data={
                "customer_count": ":,",
                "monetary_share": ":.2%",
                "total_monetary": ":,.2f",
                "segment_display": False,
            },
        )
        monetary_figure.update_coloraxes(showscale=False)
        st.plotly_chart(
            apply_chart_style(monetary_figure, 440),
            use_container_width=True,
        )

    st.dataframe(
        rfm[
            [
                "rfm_segment",
                "customer_count",
                "customer_share_pct",
                "total_monetary",
                "monetary_share_pct",
                "average_frequency",
                "average_recency_days",
                "average_order_value",
            ]
        ].rename(
            columns={
                "rfm_segment": "客户分层",
                "customer_count": "客户数",
                "customer_share_pct": "客户占比（%）",
                "total_monetary": "支付金额（BRL）",
                "monetary_share_pct": "金额占比（%）",
                "average_frequency": "平均订单频次",
                "average_recency_days": "平均最近购买间隔（天）",
                "average_order_value": "平均客单价（BRL）",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            "客户占比（%）": st.column_config.NumberColumn(format="%.2f%%"),
            "支付金额（BRL）": st.column_config.NumberColumn(format="%.2f"),
            "金额占比（%）": st.column_config.NumberColumn(format="%.2f%%"),
            "平均最近购买间隔（天）": st.column_config.NumberColumn(
                format="%.1f"
            ),
        },
    )

    st.info(
        "RFM 分析截止日取数据集中的最后购买日期。"
        "R、F、M 均使用五分位评分，5 分代表表现最好。"
    )


with delivery_tab:
    st.subheader("各州履约质量")

    delivery = delivery_summary.copy()
    for column in [
        "order_count",
        "delivered_order_count",
        "late_order_count",
        "late_delivery_rate",
        "average_delivery_days",
    ]:
        delivery[column] = pd.to_numeric(delivery[column], errors="coerce")
    delivery["late_delivery_rate_pct"] = (
        delivery["late_delivery_rate"] * 100
    )
    delivery["on_time_delivery_rate_pct"] = (
        delivery["on_time_delivery_rate"] * 100
    )

    selected_states = st.multiselect(
        "选择州",
        options=sorted(delivery["customer_state"].dropna().astype(str)),
        default=sorted(
            delivery.nlargest(10, "order_count")["customer_state"]
            .dropna()
            .astype(str)
        ),
    )
    delivery_view = (
        delivery[delivery["customer_state"].astype(str).isin(selected_states)]
        if selected_states
        else delivery
    )

    delivery_figure = px.scatter(
        delivery_view,
        x="average_delivery_days",
        y="late_delivery_rate_pct",
        size="delivered_order_count",
        color="late_delivery_rate",
        hover_name="customer_state",
        color_continuous_scale="RdYlGn_r",
        title="配送时长与延迟率",
        labels={
            "average_delivery_days": "平均配送时长（天）",
            "late_delivery_rate_pct": "延迟送达率（%）",
            "delivered_order_count": "已送达订单",
        },
    )
    delivery_figure.update_yaxes(ticksuffix="%")
    st.plotly_chart(
        apply_chart_style(delivery_figure, 500),
        use_container_width=True,
    )

    delivery_table = (
        delivery_view.sort_values("late_delivery_rate", ascending=False)
        .head(20)
        .copy()
    )
    st.dataframe(
        delivery_table[
            [
                "customer_state",
                "order_count",
                "delivered_order_count",
                "late_order_count",
                "late_delivery_rate_pct",
                "on_time_delivery_rate_pct",
                "average_approval_minutes",
                "average_carrier_days",
                "average_delivery_days",
            ]
        ].rename(
            columns={
                "customer_state": "州",
                "order_count": "订单数",
                "delivered_order_count": "已送达订单",
                "late_order_count": "延迟订单",
                "late_delivery_rate_pct": "延迟率（%）",
                "on_time_delivery_rate_pct": "准时率（%）",
                "average_approval_minutes": "平均支付确认（分钟）",
                "average_carrier_days": "平均发货（天）",
                "average_delivery_days": "平均配送（天）",
            }
        ),
        width="stretch",
        hide_index=True,
        column_config={
            "延迟率（%）": st.column_config.NumberColumn(format="%.2f%%"),
            "准时率（%）": st.column_config.NumberColumn(format="%.2f%%"),
            "平均支付确认（分钟）": st.column_config.NumberColumn(
                format="%.1f"
            ),
            "平均发货（天）": st.column_config.NumberColumn(format="%.1f"),
            "平均配送（天）": st.column_config.NumberColumn(format="%.1f"),
        },
    )


with glossary_tab:
    st.subheader("指标口径与数据架构")

    st.markdown(
        """
        ### 数据链路

        ```text
        Olist CSV
           ↓
        ODS 原始层
           ↓
        DWD 明细层：订单、订单商品、支付
           ↓
        DWS 汇总层：日交易、商品、卖家、RFM、履约
           ↓
        ADS 应用层：经营总览与排行榜
           ↓
        Streamlit + Plotly
        ```

        ### 关键口径

        - **有效订单**：排除 `canceled` 和 `unavailable` 状态。
        - **有效 GMV**：有效订单中的商品 `price` 合计，不含运费。
        - **运费收入**：有效订单中的 `freight_value` 合计。
        - **支付金额**：有效订单的支付记录 `payment_value` 合计。
        - **平均客单价**：有效 GMV / 有效订单数。
        - **商品件数**：订单商品明细行数；Olist 无独立数量字段。
        - **独立客户**：按 `customer_unique_id` 去重。
        - **复购客户**：有效订单数大于 1 的客户。
        - **延迟送达**：实际送达日期晚于预计送达日期。
        - **配送时长**：购买日期到客户签收日期的自然日差。

        ### 数据限制

        - 数据集是历史公开数据，不是实时生产交易。
        - 金额单位为巴西雷亚尔 BRL。
        - ADS 排行表展示完整分析周期，时间筛选只应用于月度趋势。
        - 取消订单可能仍有支付记录，因此支付金额和有效 GMV 不完全相同。
        """
    )

    quality_path = ADS_DIR / "_quality_checks"
    if quality_path.exists():
        checks = pd.read_parquet(quality_path)
        passed = int((checks["status"] == "PASS").sum())
        failed = int((checks["status"] == "FAIL").sum())

        quality_columns = st.columns(3)
        quality_columns[0].metric("质量检查总数", len(checks))
        quality_columns[1].metric("通过", passed)
        quality_columns[2].metric("失败", failed)

        st.dataframe(
            checks.rename(
                columns={
                    "check_name": "检查项",
                    "failed_rows": "异常数量",
                    "status": "状态",
                }
            ),
            width="stretch",
            hide_index=True,
        )

