"""Reproducible analysis for option 11 (payday lending): common measures + the five analytical questions.
Reads the MongoDB serving layer, writes CSV outputs to analytics/outputs/ and docs/findings.md.

docker compose run --rm jobs python -m analytics.analysis
"""
import json
import os
from datetime import date, datetime, timezone

import pandas as pd
from pymongo import MongoClient

from config import settings

OUT = "analytics/outputs"
BASE = json.load(open("config/context_baselines.json", encoding="utf-8"))
MIN_GROUP = 100        # percentages for groups smaller than this are flagged as unstable
MIN_STATE = 300        # states compared in Q4


def load(db):
    proj = {"_id": 0, "complaint_id": 1, "date_received": 1, "year": 1, "quarter": 1, "year_quarter": 1,
            "product.label_version": 1, "issue.name": 1, "issue.sub_issue": 1, "issue.group": 1,
            "company.name": 1, "company.response_to_consumer": 1, "company.timely_response": 1,
            "location.state": 1, "submitted_via": 1, "is_servicemember": 1, "is_older_american": 1,
            "processing_lag_days": 1, "lag_status": 1}
    df = pd.json_normalize(list(db.complaints.find({}, proj)))
    return df.rename(columns={"product.label_version": "label_version", "issue.name": "issue",
                              "issue.sub_issue": "sub_issue", "issue.group": "issue_group",
                              "company.name": "company", "company.response_to_consumer": "response",
                              "company.timely_response": "timely", "location.state": "state"})


def pct(a, b):
    return round(100.0 * a / b, 1) if b else None


def save(df, name):
    df.to_csv(f"{OUT}/{name}.csv", index=False)
    return df


def md(df, floatfmt=1):
    df = df.copy()
    for c in df.select_dtypes("float").columns:
        df[c] = df[c].round(floatfmt)
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join("" if pd.isna(v) else f"{v:,}" if isinstance(v, int) and c != "year" else str(v)
                                for c, v in zip(cols, row)) + " |"
              for row in df.itertuples(index=False)]
    return "\n".join(lines)


def main():
    os.makedirs(OUT, exist_ok=True)
    db = MongoClient(settings.MONGO_URI)[settings.MONGO_DB]
    fresh = db.meta.find_one({"_id": "freshness"}) or {}
    quality = db.meta.find_one({"_id": "quality_latest"}) or {}
    df = load(db)
    n = df["complaint_id"].nunique()
    assert n == len(df), "complaint IDs must be unique in the serving layer"
    latest = fresh.get("latest_date_received") or df["date_received"].max()
    latest_d = latest.date() if hasattr(latest, "date") else latest
    first_d = df["date_received"].min().date()
    cur_q = f"{latest_d.year}-Q{(latest_d.month - 1) // 3 + 1}"

    # ---------------------------------------------------------------- common measures
    years = df.groupby("year").size().rename("complaints").reset_index()
    years["coverage"] = years["year"].map(lambda y: "partial" if y in (first_d.year, latest_d.year) else "complete")
    years["change_vs_previous_pct"] = years["complaints"].pct_change().mul(100).round(1)
    years.loc[(years["coverage"] == "partial") | (years["coverage"].shift() == "partial"), "change_vs_previous_pct"] = None
    save(years, "yearly_counts")

    quarters = df.groupby("year_quarter").size().rename("complaints").reset_index()
    quarters["coverage"] = quarters["year_quarter"].map(
        lambda q: "partial" if q in (cur_q, f"{first_d.year}-Q{(first_d.month - 1) // 3 + 1}") else "complete")
    save(quarters, "quarterly_counts")

    issues = (df.groupby(["issue_group", "issue"]).size().rename("complaints").reset_index()
              .sort_values("complaints", ascending=False))
    issues["share_pct"] = (100 * issues["complaints"] / n).round(2)
    save(issues, "issue_breakdown")
    sub = df[df["sub_issue"].notna()].groupby(["issue", "sub_issue"]).size().rename("complaints").reset_index()
    save(sub.sort_values("complaints", ascending=False), "sub_issue_breakdown")

    states = df["state"].fillna("Not provided").value_counts().rename_axis("state").reset_index(name="complaints")
    states["share_pct"] = (100 * states["complaints"] / n).round(2)
    save(states, "state_counts")
    channels = df["submitted_via"].fillna("Not provided").value_counts().rename_axis("channel").reset_index(name="complaints")
    channels["share_pct"] = (100 * channels["complaints"] / n).round(2)
    save(channels, "channel_counts")
    responses = df["response"].fillna("Not provided").value_counts().rename_axis("response").reset_index(name="complaints")
    responses["share_pct"] = (100 * responses["complaints"] / n).round(2)
    save(responses, "response_counts")

    yes, no = int((df["timely"] == True).sum()), int((df["timely"] == False).sum())  # noqa: E712
    timely_pct = pct(yes, yes + no)
    lag = df.loc[df["lag_status"] == "ok", "processing_lag_days"]
    lag_excluded = int((df["lag_status"] != "ok").sum())

    # ---------------------------------------------------------------- Q1 trend
    complete_q = quarters[quarters["coverage"] == "complete"].set_index("year_quarter")["complaints"]
    base_q = complete_q[(complete_q.index >= "2019-Q1") & (complete_q.index <= "2024-Q4")]
    recent_q = complete_q[complete_q.index >= "2025-Q1"]
    low_q, high_q = complete_q.idxmin(), complete_q.idxmax()
    q1 = {"avg_quarter_2019_2024": round(base_q.mean(), 1), "avg_quarter_since_2025": round(recent_q.mean(), 1),
          "ratio": round(recent_q.mean() / base_q.mean(), 2), "lowest_quarter": low_q,
          "lowest_value": int(complete_q.min()), "highest_quarter": high_q, "highest_value": int(complete_q.max())}

    # ---------------------------------------------------------------- Q2 issue mix by label period
    q2 = df.groupby(["label_version", "issue_group"]).size().rename("complaints").reset_index()
    q2["period_total"] = q2.groupby("label_version")["complaints"].transform("sum")
    q2["share_pct"] = 100 * q2["complaints"] / q2["period_total"]
    save(q2, "q2_issue_mix_by_label_period")
    q2w = q2.pivot(index="issue_group", columns="label_version", values="share_pct").round(1).reset_index()

    # ---------------------------------------------------------------- Q3 responses and timeliness by issue group
    g = df.groupby("issue_group")
    q3 = pd.DataFrame({
        "complaints": g.size(),
        "timely_yes": g["timely"].apply(lambda s: int((s == True).sum())),          # noqa: E712
        "timely_valid": g["timely"].apply(lambda s: int(s.isin([True, False]).sum())),
        "monetary_relief": g["response"].apply(lambda s: int((s == "Closed with monetary relief").sum())),
        "non_monetary_relief": g["response"].apply(lambda s: int((s == "Closed with non-monetary relief").sum())),
    }).reset_index()
    q3["timely_pct"] = 100 * q3["timely_yes"] / q3["timely_valid"]
    q3["monetary_relief_pct"] = 100 * q3["monetary_relief"] / q3["complaints"]
    q3["small_group"] = q3["complaints"] < MIN_GROUP
    save(q3.sort_values("complaints", ascending=False), "q3_response_by_issue_group")

    # ---------------------------------------------------------------- Q4 state issue profiles
    national = df["issue_group"].value_counts(normalize=True)
    big = df[df["state"].notna()].groupby("state").filter(lambda s: len(s) >= MIN_STATE)
    rows = []
    for st, s in big.groupby("state"):
        share = s["issue_group"].value_counts(normalize=True).reindex(national.index, fill_value=0)
        diff = (share - national) * 100
        rows.append({"state": st, "complaints": len(s),
                     "issue_mix_distance": round(0.5 * (share - national).abs().sum(), 3),
                     "most_over_represented": f"{diff.idxmax()} (+{diff.max():.1f} pts)",
                     "most_under_represented": f"{diff.idxmin()} ({diff.min():.1f} pts)"})
    q4 = save(pd.DataFrame(rows).sort_values("issue_mix_distance", ascending=False), "q4_state_issue_profiles")

    # ---------------------------------------------------------------- Q5 tagged consumers
    sm, oa = int(df["is_servicemember"].sum()), int(df["is_older_american"].sum())
    q5 = df.groupby("issue_group").agg(complaints=("complaint_id", "size"), servicemember=("is_servicemember", "sum"),
                                       older_american=("is_older_american", "sum")).reset_index()
    q5["servicemember_pct"] = 100 * q5["servicemember"] / q5["complaints"]
    q5["older_american_pct"] = 100 * q5["older_american"] / q5["complaints"]
    save(q5.sort_values("complaints", ascending=False), "q5_tags_by_issue_group")
    q5p = df.groupby("label_version").agg(complaints=("complaint_id", "size"), servicemember=("is_servicemember", "mean"),
                                          older_american=("is_older_american", "mean")).reset_index()
    q5p[["servicemember", "older_american"]] = (q5p[["servicemember", "older_american"]] * 100).round(1)
    save(q5p, "q5_tags_by_label_period")

    # ---------------------------------------------------------------- findings document
    top_issue = issues.groupby("issue_group")["complaints"].sum().sort_values(ascending=False)
    lines = [
        f"# Findings — option {settings.OPTION_NUMBER}: {settings.OPTION_NAME}",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `analytics/analysis.py` from the MongoDB "
        f"serving layer (run `{fresh.get('last_run_id')}`, quality status `{quality.get('status')}`). "
        "All CSV outputs are in `analytics/outputs/`.",
        "",
        "## Scope and metric definitions",
        f"- **Scope:** {n:,} distinct curated Complaint IDs about payday loans, received {first_d} → {latest_d}.",
        "- **Complaints:** count of distinct curated Complaint IDs (never rows, never raw records).",
        "- **Share:** complaints in a category ÷ complaints in the group it is compared within (stated each time).",
        f"- **Timely %:** valid Yes ÷ (valid Yes + valid No); {n - yes - no} missing/invalid values reported separately.",
        f"- **Receipt-to-referral days:** date sent to company − date received, only for valid non-negative intervals "
        f"({lag_excluded} excluded). It is CFPB handling time, not company resolution time.",
        f"- **Partial periods:** {first_d.year} and {latest_d.year} are partial years; {cur_q} is a partial quarter.",
        "",
        "## Common measures",
        f"- Total complaints: **{n:,}**. Timely: **{timely_pct} %** ({yes:,} of {yes + no:,} valid).",
        f"- Receipt-to-referral: median **{lag.median():.0f}** days, mean {lag.mean():.1f}, 90th percentile "
        f"{lag.quantile(0.9):.0f} (n = {len(lag):,}).",
        f"- Leading issue group: **{top_issue.index[0]}** ({pct(top_issue.iloc[0], n)} %).",
        f"- Channel: Web {pct(int(channels.loc[channels['channel'] == 'Web', 'complaints'].sum()), n)} %; "
        f"state missing for {int(states.loc[states['state'] == 'Not provided', 'complaints'].sum())} complaints.",
        f"- Freshness: latest complaint {latest_d}; latest successful ingestion "
        f"{fresh.get('latest_successful_ingestion')}; serving layer refreshed {fresh.get('serving_refreshed_at')}.",
        "",
        "### Yearly counts",
        md(years.rename(columns={"change_vs_previous_pct": "change vs previous year (%)"})),
        "",
        "## Q1 — How have payday complaints changed per quarter?",
        f"Complete quarters averaged **{q1['avg_quarter_2019_2024']}** complaints in 2019–2024 and "
        f"**{q1['avg_quarter_since_2025']}** since 2025 (×{q1['ratio']}). The lowest complete quarter was "
        f"{q1['lowest_quarter']} ({q1['lowest_value']}), the highest {q1['highest_quarter']} ({q1['highest_value']}).",
        "",
        "*Caution:* counts describe published complaints, not the number of borrowers or loans; a rise can reflect more "
        "lending, more awareness of the CFPB or changes in how complaints are routed. Product labels changed in "
        "April 2017 and August 2023; the series is continuous because the three labels do not overlap.",
        "",
        "## Q2 — Has the issue mix changed across the three label periods? *(demo question)*",
        md(q2w.fillna("—").rename(columns={"issue_group": "issue group (% of period)"})),
        "",
        "— = the category did not exist in that period.",
        "",
        "*Caution:* the April 2017 form added 'Struggling to pay your loan' and 'Problem with the payoff process', and "
        "seven credit-reporting issues. The jump in 'Repayment and payoff' and the appearance of 'Credit reporting' "
        "after 2017 are largely category changes, so comparisons across 2017 should focus on groups that existed in "
        "every period (unexpected fees, withdrawals, origination, contacting the lender).",
        "",
        "## Q3 — Do company responses and timeliness differ by issue group?",
        md(q3[["issue_group", "complaints", "timely_pct", "monetary_relief_pct", "monetary_relief"]]
           .sort_values("complaints", ascending=False)),
        "",
        "*Caution:* response categories are reported by companies; 'monetary relief' is rare, so differences of one or "
        "two points rest on small counts. A timely response is not a satisfactory one.",
        "",
        f"## Q4 — Do states with at least {MIN_STATE} complaints have different issue profiles?",
        "`issue_mix_distance` = half the sum of absolute differences between a state's issue-group shares and the "
        "national shares (0 = identical mix, 1 = completely different).",
        "",
        md(q4.head(10), floatfmt=3),
        "",
        "*Caution:* shares avoid population size but still describe complaints, not how often borrowers have problems; "
        "payday lending is regulated differently across states, which affects how many loans exist at all.",
        "",
        "## Q5 — Are Servicemember and Older American complaints over-represented? *(demo question)*",
        f"- Servicemember-tagged: **{pct(sm, n)} %** of payday complaints vs {BASE['servicemember_share_pct']} % of all "
        f"CFPB complaints.",
        f"- Older American-tagged: **{pct(oa, n)} %** vs {BASE['older_american_share_pct']} %.",
        "",
        md(q5[["issue_group", "complaints", "servicemember_pct", "older_american_pct"]].sort_values("complaints", ascending=False)),
        "",
        "By label period:",
        "",
        md(q5p.rename(columns={"servicemember": "servicemember %", "older_american": "older american %"})),
        "",
        "*Caution:* tags rely on what the submitter reported. A higher share among complaints does not mean these "
        "consumers are more likely to have problems; it may reflect who uses payday loans or who files complaints.",
        "",
        "## Implications for the intended user",
        "- Monitor the 2025–2026 rise quarter by quarter, reading the latest quarter as partial.",
        "- Fee-related complaints remain the largest stable category across all label periods.",
        "- The Servicemember over-representation is a lead for further investigation with external data on payday "
        "borrowing among servicemembers, not a conclusion on its own.",
    ]
    os.makedirs("docs", exist_ok=True)
    with open("docs/findings.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print("\n".join(lines))
    print("\nwritten docs/findings.md and analytics/outputs/*.csv")


if __name__ == "__main__":
    main()
