"""
Streaming processor for real-time sales signal dashboard.
Validates, deduplicates, enriches, and persists sales events from mobile field inputs.
"""
import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    LEAD_CREATED = "lead_created"
    MEETING_LOGGED = "meeting_logged"
    DEMO_COMPLETED = "demo_completed"
    PROPOSAL_SENT = "proposal_sent"
    DEAL_WON = "deal_won"
    DEAL_LOST = "deal_lost"
    FOLLOW_UP = "follow_up"
    PRODUCT_INTEREST = "product_interest"


@dataclass
class SalesEvent:
    event_id: str
    rep_id: str
    lead_id: str
    event_type: EventType
    territory: str
    product_line: str
    deal_value: float
    payload: Dict[str, Any]
    client_ts: float
    server_ts: float = field(default_factory=time.time)


@dataclass
class EnrichedEvent:
    event_id: str
    rep_id: str
    lead_id: str
    event_type: str
    territory: str
    product_line: str
    deal_value: float
    stage_score: float
    is_high_intent: bool
    time_drift_s: float
    day_of_week: int
    hour_of_day: int
    server_ts: float
    payload_json: str


class EventValidator:
    """Validates incoming sales events before processing."""

    REQUIRED_FIELDS = ["event_id", "rep_id", "lead_id", "event_type", "territory"]

    def validate(self, raw: Dict[str, Any]) -> Tuple[bool, str]:
        for f in self.REQUIRED_FIELDS:
            if f not in raw or raw[f] is None:
                return False, f"Missing required field: {f}"
        if raw.get("event_type") not in [e.value for e in EventType]:
            return False, f"Unknown event_type: {raw.get('event_type')}"
        deal_value = raw.get("deal_value", 0)
        if not isinstance(deal_value, (int, float)) or deal_value < 0:
            return False, "deal_value must be non-negative number"
        return True, "ok"


class EventDeduplicator:
    """Tracks processed event IDs to prevent duplicates."""

    def __init__(self):
        self._seen: set = set()

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            return True
        self._seen.add(event_id)
        return False

    def seen_count(self) -> int:
        return len(self._seen)


class StageScorer:
    """Assigns a pipeline stage score based on event type."""

    STAGE_SCORES: Dict[str, float] = {
        EventType.LEAD_CREATED.value: 0.10,
        EventType.MEETING_LOGGED.value: 0.25,
        EventType.PRODUCT_INTEREST.value: 0.35,
        EventType.DEMO_COMPLETED.value: 0.50,
        EventType.FOLLOW_UP.value: 0.55,
        EventType.PROPOSAL_SENT.value: 0.75,
        EventType.DEAL_WON.value: 1.00,
        EventType.DEAL_LOST.value: 0.00,
    }

    HIGH_INTENT_THRESHOLD = 0.60

    def score(self, event_type: str) -> float:
        return self.STAGE_SCORES.get(event_type, 0.10)

    def is_high_intent(self, event_type: str) -> bool:
        return self.score(event_type) >= self.HIGH_INTENT_THRESHOLD


class SalesDB:
    """SQLite store for processed sales events."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        rep_id TEXT,
        lead_id TEXT,
        event_type TEXT,
        territory TEXT,
        product_line TEXT,
        deal_value REAL,
        stage_score REAL,
        is_high_intent INTEGER,
        time_drift_s REAL,
        day_of_week INTEGER,
        hour_of_day INTEGER,
        server_ts REAL,
        payload_json TEXT
    );
    CREATE TABLE IF NOT EXISTS ingestion_stats (
        batch_id TEXT,
        processed_at REAL,
        total_in INTEGER,
        accepted INTEGER,
        rejected_validation INTEGER,
        rejected_duplicate INTEGER
    );
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()

    def insert_events(self, events: List[EnrichedEvent]) -> int:
        rows = [
            (e.event_id, e.rep_id, e.lead_id, e.event_type, e.territory,
             e.product_line, e.deal_value, e.stage_score, int(e.is_high_intent),
             e.time_drift_s, e.day_of_week, e.hour_of_day, e.server_ts, e.payload_json)
            for e in events
        ]
        self.conn.executemany(
            "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )
        self.conn.commit()
        return len(rows)

    def log_batch(self, batch_id: str, total_in: int,
                   accepted: int, rej_val: int, rej_dup: int) -> None:
        self.conn.execute(
            "INSERT INTO ingestion_stats VALUES (?,?,?,?,?,?)",
            (batch_id, time.time(), total_in, accepted, rej_val, rej_dup),
        )
        self.conn.commit()

    def query_df(self, sql: str) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.conn)


class SalesStreamProcessor:
    """
    Processes raw sales event payloads: validates, deduplicates, enriches, and stores.
    """

    def __init__(self, db: SalesDB):
        self.db = db
        self.validator = EventValidator()
        self.deduplicator = EventDeduplicator()
        self.scorer = StageScorer()

    def _enrich(self, event: SalesEvent) -> EnrichedEvent:
        import datetime
        dt = datetime.datetime.fromtimestamp(event.server_ts)
        return EnrichedEvent(
            event_id=event.event_id,
            rep_id=event.rep_id,
            lead_id=event.lead_id,
            event_type=event.event_type.value,
            territory=event.territory,
            product_line=event.product_line,
            deal_value=event.deal_value,
            stage_score=self.scorer.score(event.event_type.value),
            is_high_intent=self.scorer.is_high_intent(event.event_type.value),
            time_drift_s=abs(event.server_ts - event.client_ts),
            day_of_week=dt.weekday(),
            hour_of_day=dt.hour,
            server_ts=event.server_ts,
            payload_json=json.dumps(event.payload),
        )

    def process_batch(self, raw_payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
        batch_id = hashlib.md5(str(time.time()).encode()).hexdigest()[:10]
        accepted: List[EnrichedEvent] = []
        rej_val = rej_dup = 0

        for raw in raw_payloads:
            valid, msg = self.validator.validate(raw)
            if not valid:
                logger.warning("Validation failed: %s | %s", msg, raw.get("event_id"))
                rej_val += 1
                continue
            if self.deduplicator.is_duplicate(raw["event_id"]):
                rej_dup += 1
                continue
            event = SalesEvent(
                event_id=raw["event_id"],
                rep_id=raw["rep_id"],
                lead_id=raw["lead_id"],
                event_type=EventType(raw["event_type"]),
                territory=raw.get("territory", "unknown"),
                product_line=raw.get("product_line", "general"),
                deal_value=float(raw.get("deal_value", 0)),
                payload=raw.get("payload", {}),
                client_ts=float(raw.get("client_ts", time.time())),
            )
            accepted.append(self._enrich(event))

        stored = self.db.insert_events(accepted)
        self.db.log_batch(batch_id, len(raw_payloads), stored, rej_val, rej_dup)

        return {
            "batch_id": batch_id,
            "total_in": len(raw_payloads),
            "accepted": stored,
            "rejected_validation": rej_val,
            "rejected_duplicate": rej_dup,
        }

    def pipeline_velocity(self, hours: float = 24.0) -> pd.DataFrame:
        since = time.time() - hours * 3600
        return self.db.query_df(f"""
            SELECT event_type,
                   COUNT(*) AS count,
                   ROUND(AVG(deal_value), 0) AS avg_deal_value,
                   SUM(is_high_intent) AS high_intent_count
            FROM events
            WHERE server_ts >= {since}
            GROUP BY event_type
            ORDER BY count DESC
        """)

    def territory_heatmap(self) -> pd.DataFrame:
        return self.db.query_df("""
            SELECT territory,
                   COUNT(*) AS total_events,
                   SUM(is_high_intent) AS high_intent,
                   ROUND(SUM(deal_value), 0) AS pipeline_value,
                   ROUND(AVG(stage_score), 3) AS avg_stage_score
            FROM events
            GROUP BY territory
            ORDER BY pipeline_value DESC
        """)

    def rep_leaderboard(self) -> pd.DataFrame:
        return self.db.query_df("""
            SELECT rep_id,
                   COUNT(*) AS activities,
                   SUM(CASE WHEN event_type='deal_won' THEN deal_value ELSE 0 END) AS won_value,
                   SUM(CASE WHEN event_type='deal_won' THEN 1 ELSE 0 END) AS wins,
                   ROUND(AVG(stage_score), 3) AS avg_stage
            FROM events
            GROUP BY rep_id
            ORDER BY won_value DESC
        """)


def _generate_demo_events(n: int = 150) -> List[Dict[str, Any]]:
    rng = np.random.default_rng(42)
    event_types = [e.value for e in EventType]
    territories = ["north", "south", "east", "west", "central"]
    products = ["product_a", "product_b", "product_c"]
    reps = [f"rep_{i:03d}" for i in range(1, 11)]
    events = []
    now = time.time()
    for i in range(n):
        event_id = f"evt_{i:05d}"
        events.append({
            "event_id": event_id,
            "rep_id": reps[rng.integers(0, len(reps))],
            "lead_id": f"lead_{rng.integers(1000, 9999)}",
            "event_type": event_types[rng.integers(0, len(event_types))],
            "territory": territories[rng.integers(0, len(territories))],
            "product_line": products[rng.integers(0, len(products))],
            "deal_value": float(rng.uniform(5000, 250000)),
            "client_ts": now - float(rng.integers(0, 86400)),
            "payload": {"notes": "field activity log"},
        })
    return events


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    db = SalesDB()
    processor = SalesStreamProcessor(db=db)

    events = _generate_demo_events(150)
    events_with_dup = events + events[:10]

    result = processor.process_batch(events_with_dup)
    print("Batch processing result:", result)

    print("\nPipeline velocity (24h):")
    print(processor.pipeline_velocity(24).to_string(index=False))

    print("\nTerritory heatmap:")
    print(processor.territory_heatmap().to_string(index=False))

    print("\nRep leaderboard:")
    print(processor.rep_leaderboard().head(5).to_string(index=False))
