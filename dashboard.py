"""
Dashboard data aggregation for real-time sales signal dashboard.
Computes territory views, pipeline velocity, rep activity, and alert tiles.
"""
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from stream_processor import SalesDB, SalesStreamProcessor, _generate_demo_events
    PROCESSOR_AVAILABLE = True
except ImportError:
    PROCESSOR_AVAILABLE = False


@dataclass
class AlertTile:
    title: str
    message: str
    severity: str
    campaign_or_rep: str
    created_at: float = field(default_factory=time.time)


@dataclass
class DashboardSnapshot:
    generated_at: float
    period_hours: float
    total_events: int
    total_pipeline_value: float
    won_deals: int
    won_value: float
    conversion_rate: float
    avg_stage_score: float
    high_intent_events: int
    top_territory: str
    top_rep: str
    alerts: List[AlertTile] = field(default_factory=list)


class AlertEngine:
    """Generates alert tiles based on sales signal anomalies."""

    STALE_LEAD_HOURS = 48.0
    LOW_ACTIVITY_THRESHOLD = 3
    HIGH_VALUE_DEAL_THRESHOLD = 100000.0

    def generate_alerts(self, events_df: pd.DataFrame) -> List[AlertTile]:
        alerts = []
        if events_df.empty:
            return alerts

        now = time.time()
        cutoff = now - self.STALE_LEAD_HOURS * 3600

        if "server_ts" in events_df.columns:
            recent = events_df[events_df["server_ts"] >= cutoff]
            if "rep_id" in events_df.columns:
                all_reps = set(events_df["rep_id"].unique())
                active_reps = set(recent["rep_id"].unique()) if not recent.empty else set()
                inactive = all_reps - active_reps
                for rep in list(inactive)[:3]:
                    alerts.append(AlertTile(
                        title="Inactive Rep",
                        message=f"Rep {rep} has no logged activity in {int(self.STALE_LEAD_HOURS)}h.",
                        severity="warning",
                        campaign_or_rep=rep,
                    ))

        if "deal_value" in events_df.columns and "event_type" in events_df.columns:
            high_value = events_df[
                (events_df["deal_value"] >= self.HIGH_VALUE_DEAL_THRESHOLD)
                & (events_df["event_type"].isin(["proposal_sent", "deal_won"]))
            ]
            for _, row in high_value.head(3).iterrows():
                alerts.append(AlertTile(
                    title="High-Value Opportunity",
                    message=f"Deal value Rs {row['deal_value']:,.0f} at stage {row['event_type']}.",
                    severity="info",
                    campaign_or_rep=row.get("rep_id", "unknown"),
                ))

        if "event_type" in events_df.columns:
            lost = events_df[events_df["event_type"] == "deal_lost"]
            if len(lost) > 5:
                alerts.append(AlertTile(
                    title="Deal Loss Spike",
                    message=f"{len(lost)} deals lost in current window. Review competitive intel.",
                    severity="critical",
                    campaign_or_rep="all",
                ))

        return alerts


class DashboardAggregator:
    """
    Computes all dashboard metrics from the events database.
    """

    def __init__(self, db: "SalesDB"):
        self.db = db
        self.alert_engine = AlertEngine()

    def _query_window(self, hours: float) -> pd.DataFrame:
        since = time.time() - hours * 3600
        return self.db.query_df(
            f"SELECT * FROM events WHERE server_ts >= {since}"
        )

    def snapshot(self, period_hours: float = 24.0) -> DashboardSnapshot:
        df = self._query_window(period_hours)
        if df.empty:
            return DashboardSnapshot(
                generated_at=time.time(),
                period_hours=period_hours,
                total_events=0,
                total_pipeline_value=0.0,
                won_deals=0,
                won_value=0.0,
                conversion_rate=0.0,
                avg_stage_score=0.0,
                high_intent_events=0,
                top_territory="n/a",
                top_rep="n/a",
            )

        won = df[df["event_type"] == "deal_won"]
        leads = df[df["event_type"] == "lead_created"]
        conversion_rate = len(won) / len(leads) if len(leads) else 0.0

        territory_vol = (df.groupby("territory")["deal_value"].sum()
                           .sort_values(ascending=False))
        top_territory = territory_vol.index[0] if not territory_vol.empty else "n/a"

        rep_vol = (df.groupby("rep_id")["deal_value"].sum()
                     .sort_values(ascending=False))
        top_rep = rep_vol.index[0] if not rep_vol.empty else "n/a"

        alerts = self.alert_engine.generate_alerts(df)

        return DashboardSnapshot(
            generated_at=time.time(),
            period_hours=period_hours,
            total_events=len(df),
            total_pipeline_value=round(float(df["deal_value"].sum()), 2),
            won_deals=len(won),
            won_value=round(float(won["deal_value"].sum()), 2),
            conversion_rate=round(float(conversion_rate), 4),
            avg_stage_score=round(float(df["stage_score"].mean()), 4),
            high_intent_events=int(df["is_high_intent"].sum()),
            top_territory=top_territory,
            top_rep=top_rep,
            alerts=alerts,
        )

    def territory_detail(self, period_hours: float = 24.0) -> pd.DataFrame:
        df = self._query_window(period_hours)
        if df.empty:
            return pd.DataFrame()
        return (
            df.groupby("territory")
            .agg(
                events=("event_id", "count"),
                reps=("rep_id", "nunique"),
                pipeline_value=("deal_value", "sum"),
                high_intent=("is_high_intent", "sum"),
                avg_stage=("stage_score", "mean"),
            )
            .round(3)
            .sort_values("pipeline_value", ascending=False)
            .reset_index()
        )

    def funnel_metrics(self, period_hours: float = 24.0) -> Dict[str, int]:
        df = self._query_window(period_hours)
        if df.empty:
            return {}
        funnel_stages = [
            "lead_created", "meeting_logged", "demo_completed",
            "proposal_sent", "deal_won",
        ]
        return {
            stage: int((df["event_type"] == stage).sum())
            for stage in funnel_stages
        }

    def daily_trend(self) -> pd.DataFrame:
        return self.db.query_df("""
            SELECT DATE(server_ts, 'unixepoch') AS day,
                   COUNT(*) AS events,
                   SUM(deal_value) AS pipeline_value,
                   SUM(CASE WHEN event_type='deal_won' THEN 1 ELSE 0 END) AS wins
            FROM events
            GROUP BY day
            ORDER BY day
        """)

    def product_mix(self, period_hours: float = 24.0) -> pd.DataFrame:
        df = self._query_window(period_hours)
        if df.empty:
            return pd.DataFrame()
        return (
            df.groupby("product_line")
            .agg(events=("event_id", "count"),
                 pipeline_value=("deal_value", "sum"),
                 wins=("event_type", lambda x: (x == "deal_won").sum()))
            .sort_values("pipeline_value", ascending=False)
            .reset_index()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not PROCESSOR_AVAILABLE:
        print("stream_processor not available; run with both files present.")
    else:
        from stream_processor import SalesDB, SalesStreamProcessor, _generate_demo_events

        db = SalesDB()
        processor = SalesStreamProcessor(db=db)
        events = _generate_demo_events(200)
        processor.process_batch(events)

        agg = DashboardAggregator(db=db)

        snap = agg.snapshot(period_hours=48)
        print("Dashboard Snapshot:")
        print(f"  Total events: {snap.total_events}")
        print(f"  Pipeline value: Rs {snap.total_pipeline_value:,.0f}")
        print(f"  Won deals: {snap.won_deals} | Won value: Rs {snap.won_value:,.0f}")
        print(f"  Conversion rate: {snap.conversion_rate:.1%}")
        print(f"  High-intent events: {snap.high_intent_events}")
        print(f"  Top territory: {snap.top_territory} | Top rep: {snap.top_rep}")
        print(f"  Alerts: {len(snap.alerts)}")
        for alert in snap.alerts[:3]:
            print(f"    [{alert.severity.upper()}] {alert.title}: {alert.message}")

        print("\nFunnel metrics (48h):")
        funnel = agg.funnel_metrics(48)
        for stage, count in funnel.items():
            print(f"  {stage}: {count}")

        print("\nTerritory detail:")
        print(agg.territory_detail(48).to_string(index=False))

        print("\nProduct mix:")
        print(agg.product_mix(48).to_string(index=False))
