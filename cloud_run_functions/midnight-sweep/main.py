"""
Midnight Sweep — marks missed Aura challenges as failed and applies aura penalty.

Runs every 15 minutes via Cloud Scheduler. Each invocation checks which IANA
timezones are currently at midnight (00:00–00:14), finds active challenges in
those timezones where "yesterday" has no check-in, marks them failed, and
deducts 50% of aura earned during that challenge.

Accepts optional ?simulate_date=YYYY-MM-DD for debug/testing.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

import functions_framework
import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)

FIND_MISSED_SQL = """
WITH midnight_challenges AS (
    SELECT
        c.id AS challenge_id,
        c.user_id,
        c.start_date,
        c.length_days,
        c.stake_cents,
        c.charity,
        u.timezone,
        u.aura_points,
        (%(ref_date)s::date - c.start_date) AS yesterday_day_number
    FROM challenges c
    JOIN users u ON u.id = c.user_id
    WHERE c.status = 'active'
      AND u.timezone = ANY(%(tz_list)s)
      AND c.start_date < %(ref_date)s::date
),
missed AS (
    SELECT mc.*
    FROM midnight_challenges mc
    LEFT JOIN check_ins ci
        ON ci.challenge_id = mc.challenge_id
       AND ci.day_number = mc.yesterday_day_number
    WHERE ci.id IS NULL
      AND mc.yesterday_day_number >= 1
      AND mc.yesterday_day_number <= mc.length_days
)
SELECT * FROM missed;
"""

MARK_FAILED_SQL = """
UPDATE challenges SET status = 'failed' WHERE id = ANY(%(failed_ids)s);
"""

APPLY_PENALTY_SQL = """
WITH penalty AS (
    SELECT
        c.user_id,
        COALESCE(SUM(ci.aura_awarded), 0) / 2 AS aura_penalty
    FROM challenges c
    LEFT JOIN check_ins ci ON ci.challenge_id = c.id
    WHERE c.id = ANY(%(failed_ids)s)
    GROUP BY c.user_id
)
UPDATE users u
SET aura_points = GREATEST(0, u.aura_points - penalty.aura_penalty)
FROM penalty
WHERE u.id = penalty.user_id
  AND penalty.aura_penalty > 0;
"""


def get_midnight_timezones(now_utc):
    """Return IANA timezone names where local time is 00:00–00:14."""
    midnight_tzs = []
    for tz_name in pytz.common_timezones:
        tz = pytz.timezone(tz_name)
        local_now = now_utc.astimezone(tz)
        if local_now.hour == 0 and local_now.minute < 15:
            midnight_tzs.append(tz_name)
    return midnight_tzs


@functions_framework.http
def midnight_sweep(request):
    """HTTP Cloud Function entry point."""
    simulate_date = request.args.get("simulate_date")

    if simulate_date:
        try:
            ref_dt = datetime.strptime(simulate_date, "%Y-%m-%d")
        except ValueError:
            return (
                json.dumps({"error": "simulate_date must be YYYY-MM-DD"}),
                400,
                {"Content-Type": "application/json"},
            )
        # When simulating, treat all timezones as eligible so we process everything
        now_utc = ref_dt.replace(hour=0, minute=0, tzinfo=timezone.utc)
        tz_list = list(pytz.common_timezones)
        ref_date = ref_dt.strftime("%Y-%m-%d")
        logger.info(f"Simulated sweep for date {ref_date}")
    else:
        now_utc = datetime.now(timezone.utc)
        tz_list = get_midnight_timezones(now_utc)
        if not tz_list:
            return (
                json.dumps({"status": "ok", "failed_count": 0, "message": "No timezones at midnight"}),
                200,
                {"Content-Type": "application/json"},
            )
        # ref_date is "today" in those midnight timezones (the day that just started).
        # Since it's midnight, "yesterday" = ref_date - 1 day, but our SQL computes
        # yesterday_day_number as (ref_date - start_date), which counts from start_date.
        # We use the local date in the first matching timezone.
        tz = pytz.timezone(tz_list[0])
        local_now = now_utc.astimezone(tz)
        ref_date = local_now.strftime("%Y-%m-%d")
        logger.info(f"Sweep for {len(tz_list)} timezones at midnight, ref_date={ref_date}")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return (
            json.dumps({"error": "DATABASE_URL not configured"}),
            500,
            {"Content-Type": "application/json"},
        )

    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Find missed challenges
        cur.execute(FIND_MISSED_SQL, {"ref_date": ref_date, "tz_list": tz_list})
        missed = cur.fetchall()

        if not missed:
            conn.close()
            return (
                json.dumps({"status": "ok", "failed_count": 0}),
                200,
                {"Content-Type": "application/json"},
            )

        failed_ids = [row["challenge_id"] for row in missed]

        # Mark challenges as failed
        cur.execute(MARK_FAILED_SQL, {"failed_ids": failed_ids})

        # Apply 50% aura penalty
        cur.execute(APPLY_PENALTY_SQL, {"failed_ids": failed_ids})

        conn.commit()
        cur.close()
        conn.close()

        # Build summary
        tz_breakdown = {}
        for row in missed:
            tz = row["timezone"]
            tz_breakdown[tz] = tz_breakdown.get(tz, 0) + 1

        summary = {
            "status": "ok",
            "failed_count": len(failed_ids),
            "timezone_breakdown": tz_breakdown,
            "failed_challenge_ids": [str(cid) for cid in failed_ids],
        }

        logger.info(f"Sweep complete: {len(failed_ids)} challenges failed", extra=summary)

        return (
            json.dumps(summary),
            200,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        logger.exception("Midnight sweep failed")
        return (
            json.dumps({"error": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )
