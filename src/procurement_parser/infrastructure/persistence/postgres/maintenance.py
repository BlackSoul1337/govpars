from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.metrics import (
    CAPTCHA_HOURLY_SPEND,
    LANE_STATE,
    QUEUE_DEAD_TUPLES,
    QUEUE_DELAYED,
    QUEUE_DEPTH,
    QUEUE_LEASED,
    QUEUE_READY,
    QUEUE_STALE_LEASES,
    SCHEDULER_LAST_SUCCESS,
    TASK_HISTORY,
)


class PostgresMaintenance:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def collect_queue_stats(self) -> dict:
        async with self.database.sessions() as session:
            depths = (
                await session.execute(
                    text(
                        """
                        SELECT se.source, count(*) AS depth
                        FROM crawl_frontier q
                        JOIN source_entities se ON se.id = q.source_entity_fk
                        GROUP BY se.source
                        """
                    )
                )
            ).mappings().all()
            source_activity = (
                await session.execute(
                    text(
                        """
                        SELECT se.source,
                               count(*) FILTER (
                                   WHERE q.available_at <= now()
                                     AND (
                                         q.leased_until IS NULL
                                         OR q.leased_until < now()
                                     )
                               ) AS ready,
                               count(*) FILTER (
                                   WHERE q.available_at > now()
                               ) AS delayed,
                               count(*) FILTER (
                                   WHERE q.leased_until >= now()
                               ) AS leased,
                               count(*) FILTER (
                                   WHERE q.leased_until < now()
                                     AND q.lease_owner IS NOT NULL
                               ) AS stale_leases
                        FROM crawl_frontier q
                        JOIN source_entities se ON se.id = q.source_entity_fk
                        GROUP BY se.source
                        """
                    )
                )
            ).mappings().all()
            queue_types = (
                await session.execute(
                    text(
                        """
                        SELECT se.source, se.entity_type, count(*) AS depth,
                               count(*) FILTER (WHERE q.attempt > 0) AS attempted,
                               count(*) FILTER (
                                   WHERE q.leased_until >= now()
                               ) AS leased,
                               count(*) FILTER (
                                   WHERE q.leased_until < now()
                                     AND q.lease_owner IS NOT NULL
                               ) AS stale_leases,
                               count(*) FILTER (
                                   WHERE q.available_at > now()
                               ) AS delayed,
                               count(*) FILTER (
                                   WHERE q.last_error IS NOT NULL
                               ) AS with_error,
                               COALESCE(max(q.attempt), 0) AS max_attempt
                        FROM crawl_frontier q
                        JOIN source_entities se ON se.id = q.source_entity_fk
                        GROUP BY se.source, se.entity_type
                        ORDER BY se.source, se.entity_type
                        """
                    )
                )
            ).mappings().all()
            entity_counts = (
                await session.execute(
                    text(
                        """
                        SELECT source, entity_type, count(*) AS count
                        FROM source_entities
                        WHERE last_success_at IS NOT NULL
                        GROUP BY source, entity_type
                        ORDER BY source, entity_type
                        """
                    )
                )
            ).mappings().all()
            history_counts = (
                await session.execute(
                    text(
                        """
                        SELECT se.source, h.outcome, count(*) AS count
                        FROM crawl_task_history h
                        JOIN source_entities se ON se.id = h.source_entity_fk
                        GROUP BY se.source, h.outcome
                        ORDER BY se.source, h.outcome
                        """
                    )
                )
            ).mappings().all()
            failure_counts = (
                await session.execute(
                    text(
                        """
                        SELECT COALESCE(se.source, 'unknown') AS source,
                               count(*) AS count,
                               count(*) FILTER (
                                   WHERE f.created_at >= now() - interval '24 hours'
                               ) AS last_24h,
                               count(*) FILTER (
                                   WHERE f.error ILIKE '%session lane%'
                               ) AS session_lane_events,
                               count(*) FILTER (
                                   WHERE f.http_status IS NOT NULL
                               ) AS http_events
                        FROM fetch_failures f
                        LEFT JOIN source_entities se ON se.id = f.source_entity_fk
                        GROUP BY COALESCE(se.source, 'unknown')
                        ORDER BY source
                        """
                    )
                )
            ).mappings().all()
            lane_states = (
                await session.execute(
                    text(
                        """
                        SELECT source, id AS lane_id, state
                        FROM session_lanes
                        """
                    )
                )
            ).mappings().all()
            captcha_spend = (
                await session.execute(
                    text(
                        """
                        SELECT provider, COALESCE(sum(cost), 0) AS cost
                        FROM captcha_challenges
                        WHERE status = 'success'
                          AND created_at >= now() - interval '1 hour'
                        GROUP BY provider
                        """
                    )
                )
            ).mappings().all()
            scheduler_success = (
                await session.execute(
                    text(
                        """
                        SELECT source, job_name,
                               extract(epoch FROM last_success_at) AS succeeded_at
                        FROM scheduler_job_state
                        WHERE last_success_at IS NOT NULL
                        """
                    )
                )
            ).mappings().all()
            table_stats = (
                await session.execute(
                    text(
                        """
                        SELECT COALESCE(n_live_tup, 0) AS live,
                               COALESCE(n_dead_tup, 0) AS dead
                        FROM pg_stat_user_tables
                        WHERE relname = 'crawl_frontier'
                        """
                    )
                )
            ).mappings().one_or_none() or {"live": 0, "dead": 0}
            dead_tuples = int(table_stats["dead"])
            live_tuples = int(table_stats["live"])
        result = {
            "queue_depth": {row["source"]: row["depth"] for row in depths},
            "queue_by_entity_type": {
                f"{row['source']}:{row['entity_type']}": {
                    "depth": row["depth"],
                    "attempted": row["attempted"],
                    "leased": row["leased"],
                    "stale_leases": row["stale_leases"],
                    "delayed": row["delayed"],
                    "with_error": row["with_error"],
                    "max_attempt": row["max_attempt"],
                }
                for row in queue_types
            },
            "persisted_entities": {
                f"{row['source']}:{row['entity_type']}": row["count"]
                for row in entity_counts
            },
            "task_history": {
                f"{row['source']}:{row['outcome']}": row["count"]
                for row in history_counts
            },
            "fetch_failures": {
                row["source"]: row["count"] for row in failure_counts
            },
            "fetch_failure_events": {
                row["source"]: {
                    "total": row["count"],
                    "last_24h": row["last_24h"],
                    "session_lane_events": row["session_lane_events"],
                    "http_events": row["http_events"],
                }
                for row in failure_counts
            },
            "fetch_failures_note": (
                "Historical fetch/retry events; permanent task failures are "
                "reported in task_history as '<source>:failed'."
            ),
            "crawl_frontier_dead_tuples": dead_tuples,
            "crawl_frontier_dead_tuple_ratio": round(
                dead_tuples / max(1, live_tuples + dead_tuples),
                4,
            ),
        }
        for source, depth in result["queue_depth"].items():
            QUEUE_DEPTH.labels(source=source).set(depth)
        activity_by_source = {
            row["source"]: row
            for row in source_activity
        }
        for source in ("eep-mitwork", "zakup-sk"):
            activity = activity_by_source.get(
                source,
                {
                    "ready": 0,
                    "delayed": 0,
                    "leased": 0,
                    "stale_leases": 0,
                },
            )
            QUEUE_DEPTH.labels(source=source).set(
                result["queue_depth"].get(source, 0)
            )
            QUEUE_READY.labels(source=source).set(activity["ready"])
            QUEUE_DELAYED.labels(source=source).set(activity["delayed"])
            QUEUE_LEASED.labels(source=source).set(activity["leased"])
            QUEUE_STALE_LEASES.labels(source=source).set(
                activity["stale_leases"]
            )
        QUEUE_DEAD_TUPLES.set(dead_tuples)
        for key, count in result["task_history"].items():
            source, outcome = key.split(":", 1)
            TASK_HISTORY.labels(source=source, outcome=outcome).set(count)
        for row in lane_states:
            for state in ("closed", "open", "half_open"):
                LANE_STATE.labels(
                    source=row["source"],
                    lane_id=row["lane_id"],
                    state=state,
                ).set(1 if row["state"] == state else 0)
        for row in captcha_spend:
            CAPTCHA_HOURLY_SPEND.labels(provider=row["provider"]).set(
                float(row["cost"])
            )
        for row in scheduler_success:
            SCHEDULER_LAST_SUCCESS.labels(
                source=row["source"],
                job=row["job_name"],
            ).set(float(row["succeeded_at"]))
        return result

    async def claim_scheduler_job(
        self,
        *,
        source: str,
        job_name: str,
        interval_seconds: int,
        lease_seconds: int,
        start_immediately: bool = True,
    ) -> bool:
        async with self.database.sessions.begin() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO scheduler_job_state (
                        source,
                        job_name,
                        last_started_at
                    )
                    VALUES (
                        :source,
                        :job_name,
                        CASE
                            WHEN :start_immediately THEN NULL
                            ELSE now()
                        END
                    )
                    ON CONFLICT (source, job_name) DO NOTHING
                    """
                ),
                {
                    "source": source,
                    "job_name": job_name,
                    "start_immediately": start_immediately,
                },
            )
            claimed = (
                await session.execute(
                    text(
                        """
                        UPDATE scheduler_job_state
                        SET last_started_at = now(),
                            leased_until = now()
                                + make_interval(secs => :lease_seconds),
                            last_error = NULL,
                            updated_at = now()
                        WHERE source = :source
                          AND job_name = :job_name
                          AND (
                              leased_until IS NULL
                              OR leased_until < now()
                          )
                          AND (
                              last_started_at IS NULL
                              OR last_started_at <= now()
                                  - make_interval(secs => :interval_seconds)
                          )
                        RETURNING id
                        """
                    ),
                    {
                        "source": source,
                        "job_name": job_name,
                        "lease_seconds": lease_seconds,
                        "interval_seconds": interval_seconds,
                    },
                )
            ).scalar_one_or_none()
        return claimed is not None

    async def complete_scheduler_job(
        self,
        *,
        source: str,
        job_name: str,
        error: str | None = None,
    ) -> None:
        async with self.database.sessions.begin() as session:
            await session.execute(
                text(
                    """
                    UPDATE scheduler_job_state
                    SET last_success_at = CASE
                            WHEN CAST(:error AS text) IS NULL THEN now()
                            ELSE last_success_at
                        END,
                        leased_until = NULL,
                        last_error = CAST(:error AS text),
                        updated_at = now()
                    WHERE source = :source
                      AND job_name = :job_name
                    """
                ),
                {
                    "source": source,
                    "job_name": job_name,
                    "error": error[:4000] if error else None,
                },
            )

    async def prune_history(self, retention_days: int) -> int:
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                text(
                    """
                    WITH deleted_tasks AS (
                        DELETE FROM crawl_task_history
                        WHERE completed_at < :cutoff
                        RETURNING 1
                    ),
                    deleted_failures AS (
                        DELETE FROM fetch_failures
                        WHERE created_at < :cutoff
                        RETURNING 1
                    ),
                    deleted_captcha AS (
                        DELETE FROM captcha_challenges
                        WHERE created_at < :cutoff
                        RETURNING 1
                    )
                    SELECT
                        (SELECT count(*) FROM deleted_tasks)
                      + (SELECT count(*) FROM deleted_failures)
                      + (SELECT count(*) FROM deleted_captcha)
                    """
                ),
                {"cutoff": cutoff},
            )
        return int(result.scalar_one())

    async def release_stale_leases(self, source: str | None = None) -> int:
        source_filter = "AND se.source = :source" if source else ""
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                text(
                    f"""
                    UPDATE crawl_frontier q
                    SET lease_owner = NULL,
                        leased_until = NULL
                    FROM source_entities se
                    WHERE se.id = q.source_entity_fk
                      AND q.lease_owner IS NOT NULL
                      AND q.leased_until < now()
                      {source_filter}
                    """
                ),
                {"source": source} if source else {},
            )
        return int(result.rowcount or 0)
