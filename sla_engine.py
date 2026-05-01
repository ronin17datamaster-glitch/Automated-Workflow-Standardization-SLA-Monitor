"""
=============================================================
  AUTOMATED WORKFLOW STANDARDIZATION & SLA MONITOR ENGINE
  Author  : Analytics Engineering
  Version : 1.0.0
  Stack   : Python · Pandas · openpyxl
=============================================================
  Architecture
  ─────────────────────────────────────────────────────────
  Layer 1 – Ingestion   : Load workflow logs + SLA catalog
  Layer 2 – Calculation : Merge & compute Actual_TAT
  Layer 3 – Analytics   : Compare TAT vs Target, flag status
  Layer 4 – Alerting    : Generate breach report + summary
  Champion  – Extras    : Team benchmarking, trend (7d MA)
=============================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import warnings
import os

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────
# CONFIGURATION — Business Rules live here.
# Add new services to SLA_CATALOG and they are tracked
# automatically with no further code changes.
# ─────────────────────────────────────────────────────────
SLA_CATALOG = {
    # service_id : { sla_hours, priority, owner_team }
    "SVC-001": {"service_name": "Data Ingestion",      "sla_hours": 2,  "priority": "P1", "team": "Alpha"},
    "SVC-002": {"service_name": "Report Generation",   "sla_hours": 4,  "priority": "P1", "team": "Beta"},
    "SVC-003": {"service_name": "Reconciliation",      "sla_hours": 6,  "priority": "P2", "team": "Gamma"},
    "SVC-004": {"service_name": "Client Delivery",     "sla_hours": 3,  "priority": "P1", "team": "Alpha"},
    "SVC-005": {"service_name": "Audit Validation",    "sla_hours": 8,  "priority": "P2", "team": "Beta"},
    "SVC-006": {"service_name": "Data Transformation", "sla_hours": 5,  "priority": "P2", "team": "Gamma"},
}

OWNERS = {
    "Alpha": ["Priya S.", "Arjun M.", "Meera K."],
    "Beta":  ["Ravi T.", "Sneha P.", "Kiran L."],
    "Gamma": ["Divya R.", "Nikhil V.", "Lakshmi J."],
}

WARNING_THRESHOLD = 0.80   # flag Warning if TAT > 80% of SLA
TREND_WINDOW      = 7      # days for moving average
REPORT_DATE       = datetime(2026, 5, 1)
OUTPUT_DIR        = "output"


# ─────────────────────────────────────────────────────────
# LAYER 1 – INGESTION
# In production: replace simulate_*() with pd.read_csv() /
# pd.read_sql() calls pointing at your live data sources.
# ─────────────────────────────────────────────────────────

def simulate_workflow_logs(n_tasks: int = 60, n_days: int = 7) -> pd.DataFrame:
    """Generate synthetic workflow event logs.

    Columns produced:
        task_id, service_id, owner, start_time, end_time
    """
    random.seed(42)
    np.random.seed(42)

    service_ids = list(SLA_CATALOG.keys())
    records = []

    for i in range(1, n_tasks + 1):
        svc_id  = service_ids[i % len(service_ids)]
        team    = SLA_CATALOG[svc_id]["team"]
        owner   = random.choice(OWNERS[team])
        sla_hrs = SLA_CATALOG[svc_id]["sla_hours"]

        # Skew distribution: ~30% tasks breach SLA
        actual_hrs = abs(np.random.normal(loc=sla_hrs * 0.95, scale=sla_hrs * 0.45))
        actual_hrs = round(max(0.25, actual_hrs), 2)

        day_offset = timedelta(days=random.randint(0, n_days - 1))
        start_time = REPORT_DATE - timedelta(days=n_days) + day_offset + \
                     timedelta(hours=random.randint(7, 18))
        end_time   = start_time + timedelta(hours=actual_hrs)

        records.append({
            "task_id":    f"TSK-{i:04d}",
            "service_id": svc_id,
            "owner":      owner,
            "start_time": start_time,
            "end_time":   end_time,
        })

    return pd.DataFrame(records)


def load_sla_catalog() -> pd.DataFrame:
    """Convert the SLA_CATALOG dict to a DataFrame.

    Modular design: any new service added to SLA_CATALOG
    above is automatically tracked — no code changes needed here.
    """
    rows = [{"service_id": k, **v} for k, v in SLA_CATALOG.items()]
    return pd.DataFrame(rows)


def load_data(n_tasks: int = 60) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ingestion layer entry point.

    Returns (workflow_logs, sla_catalog).
    Wraps simulate_*() — swap for read_csv / read_sql in prod.
    """
    logs    = simulate_workflow_logs(n_tasks)
    catalog = load_sla_catalog()
    return logs, catalog


# ─────────────────────────────────────────────────────────
# LAYER 2 – CALCULATION
# ─────────────────────────────────────────────────────────

def compute_tat(logs: pd.DataFrame) -> pd.DataFrame:
    """Calculate Actual_TAT in hours from timestamps.

    Exception handling: rows with null or malformed timestamps
    are flagged rather than dropped, so data engineers can fix them.
    """
    df = logs.copy()

    # Robust timestamp coercion — won't crash on bad strings
    for col in ("start_time", "end_time"):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    bad_mask = df["start_time"].isna() | df["end_time"].isna()
    if bad_mask.any():
        print(f"  [WARNING] {bad_mask.sum()} rows have unparseable timestamps — flagged as NaN.")

    df["actual_tat_hrs"] = (
        (df["end_time"] - df["start_time"])
        .dt.total_seconds()
        .div(3600)
        .round(2)
    )

    # Guard against negative durations (data-entry error)
    neg_mask = df["actual_tat_hrs"] < 0
    if neg_mask.any():
        print(f"  [WARNING] {neg_mask.sum()} rows have end_time < start_time — set to NaN.")
        df.loc[neg_mask, "actual_tat_hrs"] = np.nan

    return df


def merge_with_catalog(logs: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    """Join workflow logs with SLA catalog on service_id.

    Left join ensures no task is silently dropped even if its
    service_id is missing from the catalog (it will show NaN SLA).
    """
    merged = logs.merge(catalog, on="service_id", how="left")

    # Detect tasks for services not in catalog
    unknown = merged["sla_hours"].isna()
    if unknown.any():
        print(f"  [WARNING] {unknown.sum()} tasks reference unknown service_ids — SLA unavailable.")

    return merged


# ─────────────────────────────────────────────────────────
# LAYER 3 – ANALYTICS
# ─────────────────────────────────────────────────────────

def evaluate_sla(df: pd.DataFrame) -> pd.DataFrame:
    """Core SLA evaluation — vectorised, no row-by-row loops."""
    df = df.copy()

    df["tat_variance_hrs"]  = (df["actual_tat_hrs"] - df["sla_hours"]).round(2)
    df["tat_variance_pct"]  = ((df["tat_variance_hrs"] / df["sla_hours"]) * 100).round(1)
    df["utilisation_ratio"] = (df["actual_tat_hrs"] / df["sla_hours"]).round(3)

    # Status flag — vectorised with np.select for clarity & speed
    conditions = [
        df["actual_tat_hrs"].isna() | df["sla_hours"].isna(),
        df["actual_tat_hrs"] > df["sla_hours"],
        df["actual_tat_hrs"] > df["sla_hours"] * WARNING_THRESHOLD,
    ]
    choices = ["Data Error", "SLA Breached", "At Risk"]
    df["sla_status"] = np.select(conditions, choices, default="On Track")

    df["is_breach"] = df["sla_status"] == "SLA Breached"

    return df


def compute_moving_average(df: pd.DataFrame, window: int = TREND_WINDOW) -> pd.DataFrame:
    """7-day rolling mean TAT per service — trend prediction layer.

    If a service is getting slower, the moving average rises.
    """
    df = df.copy()
    df["task_date"] = df["start_time"].dt.normalize()

    daily = (
        df.groupby(["service_id", "service_name", "task_date"])["actual_tat_hrs"]
        .mean()
        .reset_index()
        .sort_values(["service_id", "task_date"])
    )

    daily["rolling_avg_tat"] = (
        daily.groupby("service_id")["actual_tat_hrs"]
        .transform(lambda x: x.rolling(window=window, min_periods=1).mean())
        .round(2)
    )

    return daily


# ─────────────────────────────────────────────────────────
# LAYER 4 – ALERTING & REPORTING
# ─────────────────────────────────────────────────────────

def build_breach_list(df: pd.DataFrame) -> pd.DataFrame:
    """High-priority breach list for management intervention."""
    breaches = df[df["is_breach"]].copy()
    breaches = breaches.sort_values("tat_variance_pct", ascending=False)

    alert_cols = [
        "task_id", "service_name", "priority", "team", "owner",
        "actual_tat_hrs", "sla_hours", "tat_variance_hrs", "tat_variance_pct",
        "sla_status", "start_time", "end_time",
    ]
    return breaches[alert_cols].reset_index(drop=True)


def build_daily_summary(df: pd.DataFrame) -> dict:
    """Compute the numbers that go into the Daily Performance Summary."""
    total     = len(df)
    breached  = df["is_breach"].sum()
    at_risk   = (df["sla_status"] == "At Risk").sum()
    on_track  = (df["sla_status"] == "On Track").sum()
    errors    = (df["sla_status"] == "Data Error").sum()
    compliance = round(on_track / total * 100, 1) if total else 0

    # Per-service summary
    svc_summary = (
        df.groupby(["service_id", "service_name", "sla_hours"])
        .agg(
            total_tasks=("task_id", "count"),
            breached_tasks=("is_breach", "sum"),
            avg_actual_tat=("actual_tat_hrs", "mean"),
            max_actual_tat=("actual_tat_hrs", "max"),
        )
        .reset_index()
    )
    svc_summary["compliance_pct"] = (
        (1 - svc_summary["breached_tasks"] / svc_summary["total_tasks"]) * 100
    ).round(1)
    svc_summary["avg_actual_tat"] = svc_summary["avg_actual_tat"].round(2)
    svc_summary["max_actual_tat"] = svc_summary["max_actual_tat"].round(2)

    # Per-team performance benchmark — Champion differentiator
    team_summary = (
        df.groupby("team")
        .agg(
            total_tasks=("task_id", "count"),
            breached_tasks=("is_breach", "sum"),
            avg_actual_tat=("actual_tat_hrs", "mean"),
        )
        .reset_index()
    )
    team_summary["compliance_pct"] = (
        (1 - team_summary["breached_tasks"] / team_summary["total_tasks"]) * 100
    ).round(1)
    team_summary["avg_actual_tat"] = team_summary["avg_actual_tat"].round(2)
    team_summary = team_summary.sort_values("compliance_pct", ascending=False)

    # Per-owner benchmark
    owner_summary = (
        df.groupby(["owner", "team"])
        .agg(
            total_tasks=("task_id", "count"),
            breached_tasks=("is_breach", "sum"),
        )
        .reset_index()
    )
    owner_summary["compliance_pct"] = (
        (1 - owner_summary["breached_tasks"] / owner_summary["total_tasks"]) * 100
    ).round(1)
    owner_summary = owner_summary.sort_values("compliance_pct", ascending=False)

    return {
        "summary_kpis": {
            "report_date":      REPORT_DATE.strftime("%Y-%m-%d"),
            "total_tasks":      total,
            "sla_breached":     int(breached),
            "at_risk":          int(at_risk),
            "on_track":         int(on_track),
            "data_errors":      int(errors),
            "compliance_rate":  f"{compliance}%",
            "highest_breach_svc": svc_summary.sort_values("breached_tasks", ascending=False)
                                              .iloc[0]["service_name"],
        },
        "service_summary": svc_summary,
        "team_summary":    team_summary,
        "owner_summary":   owner_summary,
    }


# ─────────────────────────────────────────────────────────
# REPORT GENERATION — Excel + CSV outputs
# ─────────────────────────────────────────────────────────

def export_reports(
    full_df:      pd.DataFrame,
    breach_list:  pd.DataFrame,
    summary:      dict,
    trend_df:     pd.DataFrame,
    output_dir:   str = OUTPUT_DIR,
) -> dict[str, str]:
    """Write all outputs and return a dict of {label: filepath}."""
    os.makedirs(output_dir, exist_ok=True)
    date_str  = REPORT_DATE.strftime("%Y%m%d")
    paths     = {}

    # ── 1. Full task log CSV (raw audit trail)
    csv_path  = os.path.join(output_dir, f"workflow_log_{date_str}.csv")
    full_df.to_csv(csv_path, index=False)
    paths["Full Log (CSV)"] = csv_path

    # ── 2. High-priority breach alert CSV
    breach_csv = os.path.join(output_dir, f"breach_alerts_{date_str}.csv")
    breach_list.to_csv(breach_csv, index=False)
    paths["Breach Alerts (CSV)"] = breach_csv

    # ── 3. Daily Performance Summary — multi-sheet Excel
    excel_path = os.path.join(output_dir, f"daily_performance_summary_{date_str}.xlsx")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        # Sheet 1: KPI dashboard row
        kpi_df = pd.DataFrame([summary["summary_kpis"]])
        kpi_df.to_excel(writer, sheet_name="KPI Summary", index=False)

        # Sheet 2: Service-level breakdown
        summary["service_summary"].to_excel(writer, sheet_name="By Service", index=False)

        # Sheet 3: Team benchmarking
        summary["team_summary"].to_excel(writer, sheet_name="By Team", index=False)

        # Sheet 4: Owner performance
        summary["owner_summary"].to_excel(writer, sheet_name="By Owner", index=False)

        # Sheet 5: Breach alerts (for management)
        breach_list.to_excel(writer, sheet_name="Breach Alerts", index=False)

        # Sheet 6: Trend data (moving average)
        trend_df.to_excel(writer, sheet_name="Trend Analysis", index=False)

        # Sheet 7: Full log
        full_df.to_excel(writer, sheet_name="Full Log", index=False)

    paths["Daily Summary (Excel)"] = excel_path
    return paths


# ─────────────────────────────────────────────────────────
# CONSOLE PRINTER — human-readable run summary
# ─────────────────────────────────────────────────────────

def print_run_summary(summary: dict, breach_list: pd.DataFrame) -> None:
    kpis = summary["summary_kpis"]
    sep  = "─" * 55

    print(f"\n{'═'*55}")
    print(f"  AUTOMATED SLA MONITOR — DAILY RUN REPORT")
    print(f"  Run date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Period   : {kpis['report_date']}")
    print(f"{'═'*55}")

    print(f"\n  KEY PERFORMANCE INDICATORS")
    print(sep)
    print(f"  {'Total Tasks Monitored':<30} {kpis['total_tasks']:>6}")
    print(f"  {'SLA Breached':<30} {kpis['sla_breached']:>6}  ← ACTION REQUIRED")
    print(f"  {'At Risk (>80% SLA)':<30} {kpis['at_risk']:>6}")
    print(f"  {'On Track':<30} {kpis['on_track']:>6}")
    print(f"  {'Overall Compliance Rate':<30} {kpis['compliance_rate']:>6}")
    print(f"  {'Highest Breach Service':<30} {kpis['highest_breach_svc']}")

    print(f"\n  SERVICE PERFORMANCE BREAKDOWN")
    print(sep)
    svc = summary["service_summary"]
    print(f"  {'Service':<24} {'SLA':>5} {'Avg TAT':>8} {'Breach':>7} {'Comply':>7}")
    print(f"  {'─'*24} {'─'*5} {'─'*8} {'─'*7} {'─'*7}")
    for _, row in svc.iterrows():
        flag = " ◄" if row["compliance_pct"] < 75 else ""
        print(
            f"  {row['service_name']:<24} "
            f"{row['sla_hours']:>4}h "
            f"{row['avg_actual_tat']:>7.2f}h "
            f"{int(row['breached_tasks']):>7} "
            f"{row['compliance_pct']:>6.1f}%{flag}"
        )

    print(f"\n  TEAM BENCHMARKING")
    print(sep)
    teams = summary["team_summary"]
    for _, row in teams.iterrows():
        bar_len = int(row["compliance_pct"] / 5)
        bar     = "█" * bar_len + "░" * (20 - bar_len)
        print(f"  {row['team']:<8}  {bar}  {row['compliance_pct']:.1f}%")

    if not breach_list.empty:
        print(f"\n  HIGH-PRIORITY BREACH ALERTS  (top 5)")
        print(sep)
        top5 = breach_list.head(5)
        for _, row in top5.iterrows():
            print(
                f"  {row['task_id']}  {row['service_name']:<22} "
                f"TAT {row['actual_tat_hrs']:.2f}h / SLA {row['sla_hours']}h  "
                f"(+{row['tat_variance_pct']:.1f}%)  [{row['owner']}]"
            )
    else:
        print(f"\n  No SLA breaches today. All systems compliant.")

    print(f"\n{'═'*55}\n")


# ─────────────────────────────────────────────────────────
# ORCHESTRATOR — wires all layers together
# ─────────────────────────────────────────────────────────

def run_sla_monitor(n_tasks: int = 60) -> dict[str, str]:
    """End-to-end pipeline entry point.

    Returns a dict of output file paths so downstream systems
    (dashboards, email triggers) can consume the results.
    """
    print("\n[1/5]  Ingesting workflow logs & SLA catalog …")
    logs, catalog = load_data(n_tasks)

    print("[2/5]  Computing Actual TAT & merging with SLA targets …")
    logs = compute_tat(logs)
    df   = merge_with_catalog(logs, catalog)

    print("[3/5]  Evaluating SLA compliance & flagging status …")
    df = evaluate_sla(df)

    print("[4/5]  Building breach list, summaries & trend analysis …")
    breach_list = build_breach_list(df)
    summary     = build_daily_summary(df)
    trend_df    = compute_moving_average(df)

    print("[5/5]  Exporting Daily Performance Summary reports …")
    output_paths = export_reports(df, breach_list, summary, trend_df)

    print_run_summary(summary, breach_list)

    for label, path in output_paths.items():
        print(f"  ✓  {label:<28}  →  {path}")
    print()

    return output_paths


# ─────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_sla_monitor(n_tasks=60)
