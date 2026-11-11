"""EzyShop live dashboard (Streamlit + Plotly, reads MongoDB only).

Tabs: Marketing (speed layer) · Operations (forecast + reorder) · Customers (at-risk +
sentiment) · Pipeline (latency + throughput). Each tab body is a st.fragment that re-runs
every REFRESH_S seconds, so the page updates without a full reload.
"""
from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

REFRESH_S = 15
FRESHNESS_TARGET_S = 300  # case-study requirement: dashboard lag <= 5 minutes

# Reference data-viz palette: fixed categorical order (never cycled), status colours reserved.
CATEGORIES = ["electronics", "computing", "phones", "apparel", "footwear", "home", "kitchen", "beauty"]
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
CAT_COLOR = dict(zip(CATEGORIES, SERIES))
INK, INK2, MUTED, GRID, AXIS = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}
DIVERGE_POS, DIVERGE_NEG = "#2a78d6", "#e34948"

st.set_page_config(page_title="EzyShop Live", page_icon="🛒", layout="wide")
st.markdown("""
<style>
  .block-container {padding-top: 1.6rem; padding-bottom: 1rem;}
  div[data-testid="stMetricValue"] {font-size: 1.7rem;}
  .badge {display:inline-flex; align-items:center; gap:.45rem; padding:.35rem .75rem; border-radius:999px;
          font-weight:600; font-size:.95rem; border:1px solid rgba(11,11,11,.10); background:#fcfcfb; color:#0b0b0b;}
  .dot {width:.7rem; height:.7rem; border-radius:50%; display:inline-block;}
  .muted {color:#52514e; font-size:.85rem;}
</style>""", unsafe_allow_html=True)


@st.cache_resource
def db():
    uri = (f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWORD']}@"
           f"{os.getenv('MONGO_HOST', 'mongo')}:{os.getenv('MONGO_PORT', '27017')}/?authSource=admin")
    return MongoClient(uri, serverSelectionTimeoutMS=5000)[os.getenv("MONGO_DB", "ezyshop")]


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc(ts):
    return ts.replace(tzinfo=dt.timezone.utc) if ts is not None and ts.tzinfo is None else ts


def style(fig: go.Figure, height: int = 300, title: str | None = None, y_title: str | None = None) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=10, r=10, t=40 if title else 10, b=10 if len(fig.data) < 2 else 75),
        title=dict(text=title, font=dict(size=15, color=INK), x=0, xanchor="left") if title else None,
        paper_bgcolor="#fcfcfb", plot_bgcolor="#fcfcfb", font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif",
                                                                   color=INK2, size=12),
        # legend under the plot so it never collides with the title (legend shown only for >= 2 series)
        legend=dict(orientation="h", yanchor="top", y=-0.26, xanchor="left", x=0, font=dict(color=INK2)),
        showlegend=len(fig.data) >= 2,
        hovermode="x unified", hoverlabel=dict(bgcolor="#ffffff", font_color=INK))
    fig.update_xaxes(showgrid=False, linecolor=AXIS, tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=AXIS, tickfont=dict(color=MUTED),
                     title=dict(text=y_title, font=dict(color=MUTED)) if y_title else None)
    return fig


def badge(age_s: float | None) -> str:
    if age_s is None:
        return '<span class="badge"><span class="dot" style="background:#898781"></span>⏸ No live data yet</span>'
    if age_s < FRESHNESS_TARGET_S:
        color, icon, label = STATUS["good"], "✓", "Fresh"
    elif age_s < 3 * FRESHNESS_TARGET_S:
        color, icon, label = STATUS["warning"], "!", "Delayed"
    else:
        color, icon, label = STATUS["critical"], "✕", "Stale"
    age = f"{age_s:.0f} s" if age_s < 120 else f"{age_s / 60:.1f} min"
    return (f'<span class="badge"><span class="dot" style="background:{color}"></span>{icon} {label}: newest data '
            f'{age} old <span class="muted">(target &lt; 5 min)</span></span>')


# ----------------------------------------------------------------------------- header
left, right = st.columns([3, 2])
left.markdown("## EzyShop · customer & demand analytics")
left.caption("Lambda prototype · speed layer: Kafka → Spark Structured Streaming → MongoDB · "
             "batch layer: Delta Lake → Spark MLlib → MongoDB · customer IDs pseudonymised")
tab_mkt, tab_ops, tab_cust, tab_pipe = st.tabs(["📈 Marketing (live)", "📦 Operations", "👥 Customers", "⚙️ Pipeline"])


# ----------------------------------------------------------------------------- marketing
@st.fragment(run_every=REFRESH_S)
def marketing():
    c1, c2 = st.columns([3, 1])
    minutes = c2.selectbox("Time window", [15, 30, 60, 180], index=2, format_func=lambda m: f"Last {m} min", key="win")
    since = now() - dt.timedelta(minutes=minutes)
    rows = list(db().live_metrics.find({"window_start": {"$gte": since}}, {"_id": 0}))
    newest = db().live_metrics.find_one(sort=[("max_event_time", -1)], projection={"max_event_time": 1})
    age = (now() - utc(newest["max_event_time"])).total_seconds() if newest and newest.get("max_event_time") else None
    c1.markdown(badge(age), unsafe_allow_html=True)
    if not rows:
        st.info("No live windows in this time range yet. Start `make stream` and `make generate`.")
        return
    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    tot = df[df.category == "__all__"].sort_values("window_start")
    cats = df[df.category != "__all__"]
    # minute grid: minutes with no data show as gaps instead of being bridged by a straight line
    grid = pd.date_range(tot.window_start.min(), tot.window_start.max(), freq="1min")
    line = tot.set_index("window_start")[["revenue", "orders"]].reindex(grid).rename_axis("window_start").reset_index()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"Revenue ({minutes} min)", f"${tot.revenue.sum():,.0f}")
    k2.metric(f"Orders ({minutes} min)", f"{int(tot.orders.sum()):,}")
    views = tot.views.sum()
    k3.metric("Conversion (orders / views)", f"{tot.orders.sum() / views:.2%}" if views else "n/a")
    last_full = tot[tot.window_start <= now() - dt.timedelta(minutes=1)].tail(1)
    k4.metric("Orders / min (last full min)", f"{int(last_full.orders.iloc[0]):,}" if len(last_full) else "n/a")

    a, b = st.columns(2)
    fig = go.Figure(go.Scatter(x=line.window_start, y=line.revenue, mode="lines", line=dict(color=SERIES[0], width=2),
                               connectgaps=False, name="Revenue", hovertemplate="$%{y:,.0f}<extra></extra>"))
    a.plotly_chart(style(fig, 280, "Revenue per minute", "$"), use_container_width=True)
    fig = go.Figure(go.Scatter(x=line.window_start, y=line.orders, mode="lines", line=dict(color=SERIES[0], width=2),
                               connectgaps=False, name="Orders", hovertemplate="%{y:,.0f} orders<extra></extra>"))
    b.plotly_chart(style(fig, 280, "Orders per minute", "orders"), use_container_width=True)

    a, b, c = st.columns(3)
    funnel = pd.DataFrame({"stage": ["Product views", "Add to cart", "Orders"],
                           "n": [tot.views.sum(), tot.add_to_cart.sum(), tot.orders.sum()]})
    fig = go.Figure(go.Bar(x=funnel.n, y=funnel.stage, orientation="h", marker=dict(color=["#86b6ef", "#3987e5", "#1c5cab"]),
                           text=[f"{v:,.0f}" for v in funnel.n], textposition="outside", textfont=dict(color=INK2),
                           cliponaxis=False, hovertemplate="%{y}: %{x:,}<extra></extra>"))
    fig.update_yaxes(autorange="reversed")
    fig.update_xaxes(range=[0, funnel.n.max() * 1.25])
    a.plotly_chart(style(fig, 280, "Conversion funnel"), use_container_width=True)

    rev = cats.groupby("category", as_index=False).revenue.sum().sort_values("revenue")
    fig = go.Figure(go.Bar(x=rev.revenue, y=rev.category, orientation="h",
                           marker=dict(color=[CAT_COLOR.get(c, MUTED) for c in rev.category]),
                           hovertemplate="%{y}: $%{x:,.0f}<extra></extra>"))
    b.plotly_chart(style(fig, 280, "Revenue by category"), use_container_width=True)

    sent = list(db().live_sentiment.find({"window_start": {"$gte": since}}, {"_id": 0}))
    if sent:
        s = pd.DataFrame(sent)
        s["w"] = s.avg_sentiment * s.reviews
        s = s.groupby("category", as_index=False).agg(w=("w", "sum"), reviews=("reviews", "sum"))
        s["avg"] = s.w / s.reviews
        s = s.sort_values("avg")
        fig = go.Figure(go.Bar(x=s.avg, y=s.category, orientation="h",
                               marker=dict(color=[DIVERGE_POS if v >= 0 else DIVERGE_NEG for v in s.avg]),
                               customdata=s.reviews, hovertemplate="%{y}: %{x:.2f} (%{customdata} reviews)<extra></extra>"))
        fig.update_xaxes(range=[-1, 1], zeroline=True, zerolinecolor=AXIS)
        c.plotly_chart(style(fig, 280, "Live review sentiment (VADER, −1…+1)"), use_container_width=True)
    else:
        c.info("No reviews in this window yet.")


# ----------------------------------------------------------------------------- operations
@st.fragment(run_every=REFRESH_S * 4)
def operations():
    m = db().forecasts.find_one({"_id": "__metrics__"})
    if not m:
        st.info("No forecast yet. Run `make nightly`.")
        return
    best = m["best_model"]
    nice = {"seasonal_naive": "Seasonal naive", "linear_regression": "Linear regression", "gbt": "Gradient-boosted trees"}
    k1, k2, k3, k4 = st.columns(4)
    k1.metric(f"Accuracy, {nice[best]} (1-step)", f"{m['one_step'][best]['accuracy']:.1%}",
              f"{(m['one_step'][best]['accuracy'] - 0.90) * 100:+.1f} pts vs 90% target")
    k2.metric("Accuracy, recursive 8 weeks", f"{m['recursive_8w']['accuracy']:.1%}")
    k3.metric("Seasonal-naive baseline", f"{m['one_step']['seasonal_naive']['accuracy']:.1%}")
    k4.metric("Data as of", m["as_of"], f"{m['weeks']} weeks of history", delta_color="off")

    a, b = st.columns([3, 2])
    cat = a.selectbox("Category", CATEGORIES, key="fc_cat")
    d = db().forecasts.find_one({"_id": cat})
    h = pd.DataFrame(d["history"]).tail(30)
    t = pd.DataFrame(d["test"])
    f = pd.DataFrame(d["future"])
    fig = go.Figure()
    fig.add_vrect(x0=t.week_start.min(), x1=t.week_start.max(), fillcolor=GRID, opacity=0.35, line_width=0,
                  annotation_text="8-week test", annotation_position="top left", annotation_font_color=MUTED)
    fig.add_trace(go.Scatter(x=h.week_start, y=h.units, name="Actual", mode="lines", line=dict(color=INK2, width=2)))
    fig.add_trace(go.Scatter(x=t.week_start, y=t[best], name=f"{nice[best]} (1-step)", mode="lines+markers",
                             line=dict(color=SERIES[0], width=2), marker=dict(size=8)))
    fig.add_trace(go.Scatter(x=f.week_start, y=f.forecast_units, name="Next 4 weeks", mode="lines+markers",
                             line=dict(color=SERIES[1], width=2, dash="dash"), marker=dict(size=8)))
    a.plotly_chart(style(fig, 340, f"Weekly units: {cat}", "units / week"), use_container_width=True)

    acc = pd.DataFrame([{"category": c, "model": m["one_step"][best]["per_category_accuracy"].get(c),
                         "naive": m["one_step"]["seasonal_naive"]["per_category_accuracy"].get(c)} for c in CATEGORIES])
    fig = go.Figure()
    fig.add_trace(go.Bar(x=acc.category, y=acc.naive, name="Seasonal naive", marker_color=SERIES[1],
                         hovertemplate="%{x}: %{y:.1%}<extra>naive</extra>"))
    fig.add_trace(go.Bar(x=acc.category, y=acc.model, name=nice[best], marker_color=SERIES[0],
                         hovertemplate="%{x}: %{y:.1%}<extra>model</extra>"))
    fig.add_hline(y=0.90, line=dict(color=INK, width=1.5, dash="dot"), annotation_text="90% target",
                  annotation_position="top left", annotation_font_color=INK)
    fig.update_layout(barmode="group", bargap=0.35, bargroupgap=0.08)
    fig.update_yaxes(range=[0.5, 1.0], tickformat=".0%")
    fig = style(fig, 340, "Forecast accuracy by category (1 − MAPE)")
    fig.update_xaxes(tickangle=-35)
    fig.update_layout(legend=dict(y=-0.38), margin=dict(b=95))
    b.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Reorder alerts")
    alerts = pd.DataFrame(list(db().reorder_alerts.find({}, {"_id": 0})))
    if not alerts.empty:
        s1, s2, s3 = st.columns(3)
        s1.metric("🔴 Reorder now", int((alerts.status == "REORDER").sum()))
        s2.metric("🟢 OK", int((alerts.status == "OK").sum()))
        s3.metric("🟡 Overstock", int((alerts.status == "OVERSTOCK").sum()))
        view = alerts[alerts.status != "OK"].sort_values(["status", "days_of_cover"])
        st.dataframe(view[["status", "product_id", "name", "category", "on_hand", "demand_4w", "lead_time_days",
                           "days_of_cover", "suggested_qty"]], hide_index=True, use_container_width=True, height=280)
        st.caption("Demand = 4-week category forecast × the product's share of the last 8 weeks. "
                   "REORDER when stock < demand over the supplier lead time + safety stock.")


# ----------------------------------------------------------------------------- customers
@st.fragment(run_every=REFRESH_S * 4)
def customers():
    risk = pd.DataFrame(list(db().at_risk_customers.find({}, {"_id": 0}).sort("score", -1)))
    a, b = st.columns([3, 2])
    if risk.empty:
        a.info("No at-risk list yet. Run `make nightly`.")
    else:
        k1, k2, k3 = a.columns(3)
        k1.metric("Customers at risk", f"{len(risk):,}")
        k2.metric("Buying less (≥50% drop)", f"{int(risk.freq_flag.sum()):,}")
        k3.metric("Negative reviews", f"{int(risk.voice_flag.sum()):,}")
        view = risk.head(200).copy()
        view["customer"] = view.customer_pid.str[:12] + "…"
        a.dataframe(view[["customer", "score", "reason", "orders_30d", "orders_prior_90d", "lifetime_value"]],
                    hide_index=True, use_container_width=True, height=380,
                    column_config={"score": st.column_config.ProgressColumn("score", min_value=0, max_value=100, format="%.0f"),
                                   "lifetime_value": st.column_config.NumberColumn("lifetime $", format="$%.0f")})
        a.caption("Pseudonymised IDs only (salted SHA-256). The raw customer ID never enters the platform.")
    rs = pd.DataFrame(list(db().review_sentiment.find({}, {"_id": 0})))
    if not rs.empty:
        worst = rs[rs.reviews >= 20].sort_values("avg_compound").head(12).iloc[::-1]
        fig = go.Figure(go.Bar(x=worst.avg_compound, y=worst.product_id + " · " + worst.subcategory, orientation="h",
                               marker=dict(color=[CAT_COLOR.get(c, MUTED) for c in worst.category]),
                               customdata=worst[["category", "reviews", "avg_rating"]],
                               hovertemplate="%{y}<br>%{customdata[0]} · %{customdata[1]} reviews · "
                                             "★%{customdata[2]:.2f}<br>sentiment %{x:.2f}<extra></extra>"))
        b.plotly_chart(style(fig, 300, "Lowest-sentiment products (≥20 reviews)", None), use_container_width=True)
        cw = pd.DataFrame(list(db().category_week_sentiment.find({}, {"_id": 0})))
        if not cw.empty:
            cw = cw.sort_values("week_start")
            fig = go.Figure()
            for c in CATEGORIES:
                s = cw[cw.category == c].tail(26)
                fig.add_trace(go.Scatter(x=s.week_start, y=s.avg_compound, name=c, mode="lines",
                                         line=dict(color=CAT_COLOR[c], width=2)))
            b.plotly_chart(style(fig, 300, "Weekly review sentiment by category (26 weeks)"), use_container_width=True)


# ----------------------------------------------------------------------------- pipeline
@st.fragment(run_every=REFRESH_S)
def pipeline():
    since = now() - dt.timedelta(hours=2)
    prog = pd.DataFrame(list(db().pipeline_metrics.find({"type": "progress", "ts": {"$gte": since}}, {"_id": 0})))
    batch = pd.DataFrame(list(db().pipeline_metrics.find({"type": "batch", "ts": {"$gte": since}}, {"_id": 0})))
    if prog.empty:
        st.info("No streaming progress in the last 2 hours.")
    else:
        # MongoDB returns naive UTC datetimes; make them tz-aware before comparing with now()
        prog["ts"] = pd.to_datetime(prog["ts"], utc=True)
        live = prog[prog["query"] == "live_metrics"].sort_values("ts")
        recent = live[live.ts >= now() - dt.timedelta(minutes=30)]
        k1, k2, k3, k4 = st.columns(4)
        avg_lat, new_lat = recent.latency_avg_s.dropna(), recent.latency_newest_s.dropna()
        k1.metric("Avg-event latency p95 (30 min)", f"{avg_lat.quantile(.95):.0f} s" if len(avg_lat) else "n/a",
                  "target ≤ 300 s", delta_color="off")
        k2.metric("Newest-event latency p95 (30 min)", f"{new_lat.quantile(.95):.0f} s" if len(new_lat) else "n/a")
        k3.metric("Processed rows / s (last batch)", f"{live.processed_rows_per_s.iloc[-1]:,.0f}" if len(live) else "n/a")
        k4.metric("Micro-batches (2 h)", f"{len(live):,}")
        st.caption("Latency = time the micro-batch became visible in MongoDB − event time. 'Oldest event' includes the "
                   "~0.5% of events the generator deliberately back-dates by 2–10 min (simulated late arrivals).")
        a, b = st.columns(2)
        fig = go.Figure()
        for col, name, color in [("latency_oldest_s", "Oldest event in batch", SERIES[1]),
                                 ("latency_avg_s", "Average event", SERIES[0]),
                                 ("latency_newest_s", "Newest event", SERIES[2])]:
            fig.add_trace(go.Scatter(x=live.ts, y=live[col], name=name, mode="lines", line=dict(color=color, width=2)))
        fig.add_hline(y=FRESHNESS_TARGET_S, line=dict(color=INK, width=1.5, dash="dot"), annotation_text="5-min target",
                      annotation_font_color=INK)
        a.plotly_chart(style(fig, 300, "Event time → visible in MongoDB", "seconds"), use_container_width=True)
        fig = go.Figure()
        for q, color in [("live_metrics", SERIES[0]), ("bronze_events", SERIES[1])]:
            s = prog[prog["query"] == q].sort_values("ts")
            fig.add_trace(go.Scatter(x=s.ts, y=s.num_input_rows, name=q, mode="lines", line=dict(color=color, width=2)))
        b.plotly_chart(style(fig, 300, "Events per micro-batch", "events"), use_container_width=True)
    if not batch.empty:
        st.caption(f"Last live micro-batch: compute {batch.sort_values('ts').batch_compute_s.iloc[-1]:.1f} s, "
                   f"Mongo write {batch.sort_values('ts').mongo_write_s.iloc[-1] * 1000:.0f} ms")
    nb = db().batch_metrics.find_one({"_id": "latest"})
    if nb:
        st.markdown("#### Last nightly batch")
        steps = {"bronze → silver": nb["silver"]["seconds"], "sentiment": nb["sentiment"]["seconds"],
                 "forecast": nb["forecast"]["seconds"], "at-risk": nb["at_risk"]["seconds"]}
        cols = st.columns(len(steps) + 1)
        for col, (k, v) in zip(cols, steps.items()):
            col.metric(k, f"{v:.0f} s")
        cols[-1].metric("total", f"{nb['total_seconds']:.0f} s", f"as of {nb['as_of']}", delta_color="off")


with tab_mkt:
    marketing()
with tab_ops:
    operations()
with tab_cust:
    customers()
with tab_pipe:
    pipeline()
