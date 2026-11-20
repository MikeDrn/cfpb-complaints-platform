"""Payday-lending complaints explorer (option 11). Reads only the MongoDB serving layer and
shows aggregates, never individual complaint records."""
import json
import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import settings
from dashboard import data as D

st.set_page_config(page_title="Payday lending complaints", page_icon="📊", layout="wide")

# Fixed colour per issue group (validated categorical palette, slots 1-6; colour follows the entity)
ISSUE_COLORS = {"Unexpected fees or interest": "#2a78d6", "Repayment and payoff": "#eb6834",
                "Bank account withdrawals": "#1baf7a", "Loan origination": "#eda100",
                "Contacting the lender": "#e87ba4", "Credit reporting": "#008300", "Other / unmapped": "#8a8986"}
PERIOD_COLORS = {"v1_2013_2017": "#9ec5f4", "v2_2017_2023": "#4f94e6", "v3_2023_now": "#1f5ca8"}  # ordinal: one hue
BLUE, INK_MUTED = "#2a78d6", "#52514e"
BASELINES = json.load(open(os.path.join("config", "context_baselines.json"), encoding="utf-8"))


def style(fig, height=380, room=None, axis="x"):
    """Common look: recessive grid, legend outside the plot, room for value labels outside the bars."""
    fig.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=50, b=10),
                      legend=dict(orientation="v", x=1.01, y=1, title_text=""), font=dict(size=13),
                      bargap=0.35)
    fig.update_traces(cliponaxis=False, selector=dict(type="bar"))
    if room is not None:
        (fig.update_xaxes if axis == "x" else fig.update_yaxes)(range=[0, room * 1.18])
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#ececea")
    return fig


@st.cache_resource(show_spinner=False)
def get_db():
    return D.connect(settings.MONGO_URI, settings.MONGO_DB)


@st.cache_data(ttl=300, show_spinner=False)
def cached(name, *args, **kwargs):
    return getattr(D, name)(get_db(), *args, **kwargs)


# ------------------------------------------------------------------ connection and metadata
try:
    freshness, quality = cached("meta")
    opts = cached("options")
except D.DataUnavailable:
    st.title("Payday lending complaints")
    st.error("The analytical database is not reachable right now, so no figures can be shown. "
             "Check that the platform is running (`docker compose ps`) and reload the page.")
    st.stop()

latest = freshness.get("latest_date_received")
latest_day = latest.date() if latest else date.today()
first_day = date(2013, 11, 1)

# ------------------------------------------------------------------ sidebar filters
st.sidebar.header("Filters")
period = st.sidebar.date_input("Date received", (date(2014, 1, 1), latest_day),
                               min_value=first_day, max_value=latest_day, format="YYYY-MM-DD")
issue_groups = st.sidebar.multiselect("Issue group", opts["issue_groups"])
states = st.sidebar.multiselect("State", opts["states"])
companies = st.sidebar.multiselect("Company (40 with most complaints)", opts["companies"])
channels = st.sidebar.multiselect("Submitted via", opts["channels"])
periods = st.sidebar.multiselect("Product-label period", list(D.LABEL_PERIODS), format_func=D.LABEL_PERIODS.get)
st.sidebar.caption("Empty filter = all values.")

start, end = (period if isinstance(period, (tuple, list)) and len(period) == 2 else (None, None))
try:
    match = D.build_match(start, end, issue_groups, states, companies, channels, periods)
except D.InvalidFilter as exc:
    st.title("Payday lending complaints")
    st.warning(f"{exc} Adjust the date filter in the sidebar.")
    st.stop()

# ------------------------------------------------------------------ header
st.title("Payday lending complaints")
st.caption(f"Option {settings.OPTION_NUMBER} · {settings.OPTION_NAME} · CFPB Consumer Complaint Database · "
           f"period {start} → {end}")
q_status = quality.get("status", "unknown")
icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(q_status, "❔")
refreshed = freshness.get("serving_refreshed_at")
st.info(f"**Latest complaint in the data:** {latest_day} · **Last database refresh:** "
        f"{refreshed:%Y-%m-%d %H:%M} UTC · **Quality check of last run:** {icon} {q_status} · "
        f"**Records reconciled:** {'yes' if freshness.get('reconciled') else 'no'}" if refreshed else
        "Freshness information is not available yet.")

try:
    h = D.headline(get_db(), match)
except D.DataUnavailable:
    st.error("The database stopped responding while loading figures. Reload the page in a moment.")
    st.stop()
if not h:
    st.warning("No complaints match these filters. Widen the date range or remove a filter.")
    st.stop()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Complaints (distinct IDs)", f"{h['complaints']:,}")
k2.metric("Marked timely", f"{h['timely_pct']:.1f} %" if h["timely_pct"] is not None else "n/a",
          help=f"Valid Yes ÷ (valid Yes + No). {h['timely_invalid_or_missing']} records missing or invalid, excluded. "
               "Not a measure of satisfaction.")
k3.metric("Avg. days receipt → referral", f"{h['avg_lag']:.1f}" if h["avg_lag"] is not None else "n/a",
          help=f"Days from CFPB receipt to referral to the company; {h['lag_missing_or_invalid']} records with a "
               "missing or negative interval excluded. Not the company's resolution time.")
k4.metric("Tagged Servicemember", f"{100 * h['servicemember'] / h['complaints']:.1f} %",
          help=f"All CFPB complaints: {BASELINES['servicemember_share_pct']} %")

tabs = st.tabs(["Overview & trends", "Issues", "Geography, channel & companies", "Company responses",
                "Servicemembers & older consumers", "Data quality & limitations"])

# ------------------------------------------------------------------ 1. overview and trends
with tabs[0]:
    q = D.quarterly(get_db(), match)
    partial = D.partial_quarters(q["quarter"], latest_day, start, end)
    q["status"] = q["quarter"].map(lambda x: "partial quarter" if x in partial else "complete quarter")
    fig = go.Figure()
    fig.add_scatter(x=q["quarter"], y=q["complaints"], mode="lines", line=dict(color=BLUE, width=2),
                    name="Complaints", hovertemplate="%{x}: %{y:,} complaints<extra></extra>", showlegend=False)
    comp, part = q[q["status"] == "complete quarter"], q[q["status"] == "partial quarter"]
    fig.add_scatter(x=comp["quarter"], y=comp["complaints"], mode="markers", marker=dict(color=BLUE, size=8),
                    name="Complete quarter", hoverinfo="skip")
    fig.add_scatter(x=part["quarter"], y=part["complaints"], mode="markers",
                    marker=dict(color="white", size=9, line=dict(color=BLUE, width=2)),
                    name="Partial quarter", hovertemplate="%{x} (partial): %{y:,}<extra></extra>")
    for quarter, label in D.LABEL_CHANGES:
        if quarter in set(q["quarter"]):
            fig.add_vline(x=quarter, line_dash="dot", line_color=INK_MUTED)
            fig.add_annotation(x=quarter, y=q["complaints"].max(), text=label, showarrow=False,
                               xanchor="left", font=dict(size=11, color=INK_MUTED))
    st.plotly_chart(style(fig.update_layout(title="Complaints per quarter (date received)",
                                            yaxis_title="Complaints", xaxis_title=None)), use_container_width=True)
    st.caption("Quarters are used because monthly counts are small (about 150–300). Hollow markers are partial "
               "quarters: recent complaints are still being published, so the last point will rise.")

    y = D.yearly(get_db(), match)
    y["period covered"] = y["year"].map(lambda yr: "partial year" if date(yr, 1, 1) < start or
                                        date(yr, 12, 31) > min(end, latest_day) else "complete year")
    y["change vs previous year"] = y["complaints"].pct_change().mul(100).round(1)
    y.loc[(y["period covered"] == "partial year") | (y["period covered"].shift() == "partial year")
          | (y["year"].diff() != 1), "change vs previous year"] = None
    y["year"] = y["year"].astype(str)
    y["change vs previous year"] = y["change vs previous year"].map(lambda v: "—" if pd.isna(v) else f"{v:+.1f} %")
    st.dataframe(y[["year", "complaints", "change vs previous year", "period covered"]].rename(columns={
                     "year": "Year", "complaints": "Complaints",
                     "change vs previous year": "Change vs previous year", "period covered": "Coverage"}),
                 hide_index=True, use_container_width=True)
    st.caption("Year-on-year change is shown only between two complete years.")

# ------------------------------------------------------------------ 2. issues
with tabs[1]:
    s = D.share_by(get_db(), match, "issue.group")
    fig = px.bar(s.sort_values("complaints"), x="complaints", y="value", orientation="h",
                 color="value", color_discrete_map=ISSUE_COLORS,
                 text=s.sort_values("complaints")["share_pct"].map(lambda v: f"{v:.1f} %"),
                 labels={"complaints": "Complaints", "value": ""},
                 title="Leading issue groups — count and share of the selection")
    fig.update_traces(hovertemplate="%{y}: %{x:,} complaints<extra></extra>", textposition="outside")
    st.plotly_chart(style(fig, 360, room=s["complaints"].max()).update_layout(showlegend=False),
                    use_container_width=True)

    m = D.mix(get_db(), match, "product.label_version", "issue.group")
    m["period"] = m["outer"].map(D.LABEL_PERIODS)
    fig = px.bar(m, x="share_pct", y="period", color="inner", orientation="h", color_discrete_map=ISSUE_COLORS,
                 category_orders={"inner": D.ISSUE_GROUPS, "period": list(D.LABEL_PERIODS.values())},
                 custom_data=["complaints", "outer_total"],
                 labels={"share_pct": "Share of complaints in the period (%)", "period": "", "inner": "Issue group"},
                 title="Q2 · Issue mix within each product-label period (100 % per period)")
    fig.update_traces(hovertemplate="%{fullData.name}: %{x:.1f} % (%{customdata[0]:,} of %{customdata[1]:,})<extra></extra>",
                      marker_line_color="white", marker_line_width=2)
    st.plotly_chart(style(fig, 330).update_layout(barmode="stack"), use_container_width=True)
    st.caption("Caution: in April 2017 the CFPB reworded the issues and added 'Struggling to pay your loan'. "
               "Part of the rise of 'Repayment and payoff' after 2017 reflects the new category, not only "
               "consumer experience. Original CFPB labels are kept and listed below.")
    with st.expander("Original CFPB issues and sub-issues in this selection"):
        detail = D.mix(get_db(), match, "issue.name", "issue.sub_issue").rename(columns={
            "outer": "Issue (original label)", "inner": "Sub-issue", "complaints": "Complaints",
            "share_pct": "Share within issue (%)"})
        st.dataframe(detail.drop(columns=["outer_total"]).sort_values("Complaints", ascending=False),
                     hide_index=True, use_container_width=True)
        st.caption("'Not provided' sub-issue: most payday issues have no sub-issue by CFPB design.")

# ------------------------------------------------------------------ 3. geography, channel, companies
with tabs[2]:
    c1, c2 = st.columns([3, 2])
    st_df = D.share_by(get_db(), match, "location.state")
    known = st_df[st_df["value"] != D.NOT_PROVIDED]
    fig = px.choropleth(known, locations="value", locationmode="USA-states", scope="usa", color="complaints",
                        color_continuous_scale=["#dfeafb", "#1f5ca8"], hover_data={"share_pct": ":.1f"},
                        labels={"complaints": "Complaints", "value": "State", "share_pct": "Share (%)"},
                        title="Complaints by state (counts, not adjusted for population)")
    c1.plotly_chart(style(fig, 380), use_container_width=True)
    missing_state = int(st_df.loc[st_df["value"] == D.NOT_PROVIDED, "complaints"].sum())
    c1.caption(f"{missing_state:,} complaints have no state and are not on the map. Large states have more "
               "residents and more borrowers, so counts alone do not compare states fairly.")
    ch = D.share_by(get_db(), match, "submitted_via")
    fig = px.bar(ch, x="value", y="share_pct", text=ch["share_pct"].map(lambda v: f"{v:.1f} %"),
                 labels={"value": "", "share_pct": "Share (%)"}, title="Submission channel",
                 color_discrete_sequence=[BLUE], custom_data=["complaints"])
    fig.update_traces(hovertemplate="%{x}: %{customdata[0]:,} (%{y:.1f} %)<extra></extra>", textposition="outside")
    c2.plotly_chart(style(fig, 380, room=ch["share_pct"].max(), axis="y"), use_container_width=True)

    st.subheader("Q4 · Do the leading states have different issue profiles?")
    min_n = st.slider("Only states with at least this many complaints", 50, 1000, 300, step=50)
    sm = D.mix(get_db(), match, "location.state", "issue.group")
    national = sm.groupby("inner")["complaints"].sum() / sm["complaints"].sum() * 100
    sm = sm[(sm["outer_total"] >= min_n) & (sm["outer"] != D.NOT_PROVIDED)].copy()
    if sm.empty:
        st.info("No state reaches that minimum in this selection. Lower the minimum or widen the filters.")
    else:
        sm["difference vs national (points)"] = sm["share_pct"] - sm["inner"].map(national)
        table = sm.pivot_table(index="outer", columns="inner", values="share_pct").round(1)
        table.insert(0, "Complaints", sm.groupby("outer")["outer_total"].first())
        table.loc["National (selection)"] = [sm["outer_total"].drop_duplicates().sum()] + \
            [round(national.get(c, 0), 1) for c in table.columns[1:]]
        table.index.name = "State"
        table.columns.name = None
        st.dataframe(table.sort_values("Complaints", ascending=False), use_container_width=True)
        st.caption("Cells are the share (%) of each state's complaints in each issue group. Compare a state's row "
                   "with the national row. Shares avoid population size, but still describe complaints, not "
                   "how often borrowers have problems.")

    st.subheader("Companies with the largest share of complaints")
    co = D.share_by(get_db(), match, "company.name", top=10)
    fig = px.bar(co.sort_values("complaints"), x="share_pct", y="value", orientation="h",
                 text=co.sort_values("complaints")["complaints"].map(lambda v: f"{v:,}"),
                 labels={"share_pct": "Share of complaints in the selection (%)", "value": ""},
                 color_discrete_sequence=[BLUE])
    fig.update_traces(hovertemplate="%{y}: %{x:.1f} %<extra></extra>", textposition="outside")
    st.plotly_chart(style(fig, 400, room=co["share_pct"].max()), use_container_width=True)
    st.caption("Not a quality ranking: companies with more customers or loans receive more complaints. "
               "The data has no number of customers per company.")

# ------------------------------------------------------------------ 4. responses
with tabs[3]:
    r = D.share_by(get_db(), match, "company.response_to_consumer")
    fig = px.bar(r.sort_values("complaints"), x="share_pct", y="value", orientation="h",
                 text=r.sort_values("complaints")["share_pct"].map(lambda v: f"{v:.1f} %"),
                 labels={"share_pct": "Share (%)", "value": ""}, color_discrete_sequence=[BLUE],
                 title="Company response to consumer", custom_data=["complaints"])
    fig.update_traces(hovertemplate="%{y}: %{customdata[0]:,} (%{x:.1f} %)<extra></extra>", textposition="outside")
    st.plotly_chart(style(fig, 330, room=r["share_pct"].max()), use_container_width=True)

    t = D.timely_by_issue(get_db(), match)
    t["valid"] = t["yes"] + t["no"]
    c1, c2 = st.columns(2)
    fig = px.bar(t, x="issue_group", y="timely_pct", color="issue_group", color_discrete_map=ISSUE_COLORS,
                 text=t["timely_pct"].map(lambda v: f"{v:.1f} %" if pd.notna(v) else "n/a"),
                 custom_data=["yes", "valid"], labels={"issue_group": "", "timely_pct": "Marked timely (%)"},
                 title="Q3 · Timely response by issue group")
    fig.update_traces(hovertemplate="%{x}: %{y:.1f} % (%{customdata[0]:,} of %{customdata[1]:,} valid)<extra></extra>",
                      textposition="outside")
    c1.plotly_chart(style(fig, 380).update_layout(showlegend=False, yaxis_range=[0, 105]), use_container_width=True)
    fig = px.bar(t, x="issue_group", y="monetary_relief_pct", color="issue_group", color_discrete_map=ISSUE_COLORS,
                 text=t["monetary_relief_pct"].map(lambda v: f"{v:.1f} %"), custom_data=["monetary", "complaints"],
                 labels={"issue_group": "", "monetary_relief_pct": "Closed with monetary relief (%)"},
                 title="Q3 · Monetary relief by issue group")
    fig.update_traces(hovertemplate="%{x}: %{y:.1f} % (%{customdata[0]:,} of %{customdata[1]:,})<extra></extra>",
                      textposition="outside")
    c2.plotly_chart(style(fig, 380, room=t["monetary_relief_pct"].max(), axis="y").update_layout(showlegend=False),
                    use_container_width=True)
    st.caption(f"Timely % = valid Yes ÷ (valid Yes + No); {h['timely_invalid_or_missing']} records with a missing "
               "or invalid value are excluded. A timely response is not a satisfactory one, and response "
               "categories describe what the company reported, not the outcome for the consumer.")

# ------------------------------------------------------------------ 5. servicemembers / older consumers
with tabs[4]:
    sm_share = 100 * h["servicemember"] / h["complaints"]
    oa_share = 100 * h["older_american"] / h["complaints"]
    c1, c2 = st.columns(2)
    c1.metric("Tagged Servicemember — payday selection", f"{sm_share:.1f} %",
              delta=f"{sm_share - BASELINES['servicemember_share_pct']:+.1f} pts vs all CFPB complaints",
              delta_color="off")
    c2.metric("Tagged Older American — payday selection", f"{oa_share:.1f} %",
              delta=f"{oa_share - BASELINES['older_american_share_pct']:+.1f} pts vs all CFPB complaints",
              delta_color="off")
    st.caption(f"All CFPB complaints: Servicemember {BASELINES['servicemember_share_pct']} %, Older American "
               f"{BASELINES['older_american_share_pct']} % ({BASELINES['source']}).")
    tg = D.tags_by_issue(get_db(), match)
    long = tg.melt(id_vars=["issue_group", "complaints"], value_vars=["servicemember_pct", "older_american_pct"],
                   var_name="tag", value_name="pct")
    long["tag"] = long["tag"].map({"servicemember_pct": "Servicemember", "older_american_pct": "Older American"})
    fig = px.bar(long, x="issue_group", y="pct", color="tag", barmode="group",
                 color_discrete_map={"Servicemember": "#6250d6", "Older American": "#1baf7a"},
                 custom_data=["complaints"], labels={"issue_group": "", "pct": "Share of the group's complaints (%)",
                                                     "tag": ""},
                 title="Q5 · Tagged share within each issue group")
    fig.update_traces(hovertemplate="%{fullData.name}: %{y:.1f} % of %{customdata[0]:,}<extra></extra>",
                      marker_line_color="white", marker_line_width=2)
    fig.add_hline(y=sm_share, line_dash="dot", line_color="#6250d6",
                  annotation_text="Servicemember share, whole selection", annotation_position="top left")
    st.plotly_chart(style(fig, 400), use_container_width=True)
    st.caption("Tags depend on what the person submitting the complaint reported (age 62+, or a servicemember, "
               "veteran or their family). A higher share among complaints does not show that these consumers "
               "are more likely to have a problem; it may also reflect who uses payday loans or who files complaints.")

# ------------------------------------------------------------------ 6. quality and limitations
with tabs[5]:
    c1, c2 = st.columns(2)
    c1.subheader("Freshness")
    c1.write(pd.DataFrame({"Item": ["Latest complaint date", "Latest successful ingestion", "Database refreshed",
                                    "Last run", "Curated records", "Documents in MongoDB", "Reconciled"],
                           "Value": [str(latest_day), str(freshness.get("latest_successful_ingestion")),
                                     str(refreshed), freshness.get("last_run_id"),
                                     f"{freshness.get('curated_records', 0):,}",
                                     f"{freshness.get('mongo_documents', 0):,}",
                                     "yes" if freshness.get("reconciled") else "no"]}).set_index("Item"))
    c2.subheader("Field completeness (last run)")
    comp = quality.get("completeness_pct", {})
    if comp:
        c2.dataframe(pd.DataFrame(sorted(comp.items()), columns=["Field", "% filled"]),
                     hide_index=True, use_container_width=True)
        c2.caption("Sub-product does not exist for 2013–2017 records and most issues have no sub-issue by design.")
    st.subheader("Quality checks (last run)")
    if quality.get("checks"):
        st.dataframe(pd.DataFrame(quality["checks"]), hide_index=True, use_container_width=True)
    st.subheader("How to read these figures")
    st.markdown("""
- Figures count **complaints published by the CFPB**, not borrowers, loans, fraud or harm. The data is not a
  representative sample of payday borrowers.
- **Company comparisons are not quality rankings.** The data has no number of customers per company.
- **State counts are not adjusted for population**; issue *shares* are used to compare states.
- **'Timely response'** says whether the company answered in time, not whether the consumer was satisfied.
- **Receipt-to-referral days** measure CFPB handling, not the company's resolution time.
- **Recent quarters are incomplete**: complaints are published after the company responds or after 15 days.
- **Labels changed** in April 2017 and August 2023. Original labels are kept; issue groups harmonise them.
- No individual complaint is shown; small groups should be read with their counts.
""")
