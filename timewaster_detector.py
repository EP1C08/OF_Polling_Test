"""Timewaster Detection System - Scheduled Analytics Job.

Analyzes fan behavior across all models to detect timewasters:
- Algorithm: < $50 spend, > 50 messages, < $0.05 RPM
- Track RPM per model AND per fan
- Auto-restrict fans with $0 RPM across ALL models
- Flag for review fans with varying RPM across models

Runs on configurable schedule (default: every 12 hours).
"""

import asyncio
import csv
import os
import re
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text, select, func, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from models.db_models import Message, Bundle, BundleFanInteraction
from models.timewaster_models import FanTimewasterAnalysis, FanTimewasterFlag, Base
from modules.logger import setup_logger


class TimewasterConfig:
    """Configuration for timewaster detection thresholds."""

    def __init__(
        self,
        max_spend: float = 50.00,
        min_messages: int = 50,
        max_rpm: float = 0.05,
        rpm_variance_threshold: float = 0.10,
    ):
        """Initialize timewaster configuration.

        :param max_spend: Maximum total spend to be considered timewaster.
        :param min_messages: Minimum messages required for analysis.
        :param max_rpm: Maximum RPM threshold for timewaster classification.
        :param rpm_variance_threshold: RPM variance for flagging varying behavior.
        """
        self.max_spend = max_spend
        self.min_messages = min_messages
        self.max_rpm = max_rpm
        self.rpm_variance_threshold = rpm_variance_threshold


class TimewasterDetector:
    """Analyzes fan behavior to detect timewasters across all models."""

    def __init__(
        self,
        db_url: str,
        config: Optional[TimewasterConfig] = None,
        run_interval: int = 43200,
    ):
        """Initialize timewaster detector.

        :param db_url: PostgreSQL connection string.
        :param config: TimewasterConfig with thresholds.
        :param run_interval: Seconds between analysis runs (default 12 hours).
        """
        if db_url.startswith('postgresql://'):
            db_url = db_url.replace('postgresql://', 'postgresql+asyncpg://', 1)

        unsupported_params = [
            'sslmode', 'channel_binding', 'gssencmode',
            'krbsrvname', 'passfile', 'service'
        ]
        for param in unsupported_params:
            db_url = re.sub(rf'[?&]{param}=[^&]*', '', db_url)
        db_url = db_url.rstrip('?&')

        self.db_url = db_url
        self.config = config or TimewasterConfig()
        self.run_interval = run_interval
        self.logger = setup_logger('timewaster_detector', log_type='timewaster')
        self.engine = None
        self.async_session_maker = None

    async def initialize(self) -> bool:
        """Initialize database connection and create tables.

        :return: True if successful.
        """
        try:
            self.engine = create_async_engine(
                self.db_url,
                pool_size=2,
                max_overflow=3,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )

            self.async_session_maker = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            async with self.engine.connect() as conn:
                await conn.execute(select(1))

            self.logger.info("Timewaster detector initialized")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    async def analyze_fan_model_rpm(self, session: AsyncSession) -> int:
        """Calculate and store RPM for each fan-model pair.

        :param session: Database session.
        :return: Number of records analyzed.
        """
        self.logger.info("Calculating RPM per fan per model...")

        rpm_query = text("""
            WITH message_stats AS (
                SELECT
                    fan_id,
                    model_id,
                    model_name,
                    COUNT(*) as total_messages,
                    SUM(CASE WHEN is_from_me = FALSE THEN 1 ELSE 0 END) as messages_from_fan,
                    SUM(CASE WHEN is_from_me = TRUE THEN 1 ELSE 0 END) as messages_from_creator,
                    MIN(created_at) as first_message_at,
                    MAX(created_at) as last_message_at,
                    COALESCE(SUM(CASE WHEN is_purchased = TRUE THEN price ELSE 0 END), 0) as message_spend
                FROM messages_new
                GROUP BY fan_id, model_id, model_name
            ),
            bundle_stats AS (
                SELECT
                    bfi.fan_user_id as fan_id,
                    b.creator_id as model_id,
                    COALESCE(SUM(CASE WHEN bfi.is_purchased = TRUE THEN b.total_price ELSE 0 END), 0) as bundle_spend
                FROM bundle_fan_interactions_new bfi
                JOIN bundles_new b ON bfi.bundle_id = b.bundle_id
                WHERE bfi.is_purchased = TRUE
                GROUP BY bfi.fan_user_id, b.creator_id
            )
            SELECT
                ms.fan_id,
                ms.model_id,
                ms.model_name,
                ms.total_messages,
                ms.messages_from_fan,
                ms.messages_from_creator,
                ms.first_message_at,
                ms.last_message_at,
                COALESCE(ms.message_spend, 0) as message_spend,
                COALESCE(bs.bundle_spend, 0) as bundle_spend,
                (COALESCE(ms.message_spend, 0) + COALESCE(bs.bundle_spend, 0)) as total_spend,
                CASE
                    WHEN ms.total_messages > 0
                    THEN (COALESCE(ms.message_spend, 0) + COALESCE(bs.bundle_spend, 0)) / ms.total_messages
                    ELSE 0
                END as rpm
            FROM message_stats ms
            LEFT JOIN bundle_stats bs ON ms.fan_id = bs.fan_id AND ms.model_id = bs.model_id
        """)

        result = await session.execute(rpm_query)
        rows = result.fetchall()

        self.logger.info(f"Found {len(rows)} fan-model pairs to analyze")

        count = 0
        batch_size = 500
        batch = []

        for row in rows:
            record = {
                'fan_id': str(row.fan_id),
                'model_id': str(row.model_id),
                'model_name': row.model_name,
                'total_messages': row.total_messages or 0,
                'messages_from_fan': row.messages_from_fan or 0,
                'messages_from_creator': row.messages_from_creator or 0,
                'first_message_at': row.first_message_at,
                'last_message_at': row.last_message_at,
                'message_spend': float(row.message_spend or 0),
                'bundle_spend': float(row.bundle_spend or 0),
                'total_spend': float(row.total_spend or 0),
                'rpm': float(row.rpm or 0),
                'analyzed_at': datetime.utcnow(),
            }
            batch.append(record)

            if len(batch) >= batch_size:
                await self._upsert_analysis_batch(session, batch)
                count += len(batch)
                batch = []

        if batch:
            await self._upsert_analysis_batch(session, batch)
            count += len(batch)

        await session.commit()
        self.logger.info(f"Analyzed {count} fan-model pairs")
        return count

    async def _upsert_analysis_batch(
        self, session: AsyncSession, batch: List[Dict]
    ) -> None:
        """Upsert a batch of analysis records.

        :param session: Database session.
        :param batch: List of records to upsert.
        """
        if not batch:
            return

        stmt = insert(FanTimewasterAnalysis).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=['fan_id', 'model_id'],
            set_={
                'model_name': stmt.excluded.model_name,
                'total_messages': stmt.excluded.total_messages,
                'messages_from_fan': stmt.excluded.messages_from_fan,
                'messages_from_creator': stmt.excluded.messages_from_creator,
                'first_message_at': stmt.excluded.first_message_at,
                'last_message_at': stmt.excluded.last_message_at,
                'message_spend': stmt.excluded.message_spend,
                'bundle_spend': stmt.excluded.bundle_spend,
                'total_spend': stmt.excluded.total_spend,
                'rpm': stmt.excluded.rpm,
                'analyzed_at': stmt.excluded.analyzed_at,
            },
        )
        await session.execute(stmt)

    async def detect_timewasters(self, session: AsyncSession) -> List[Dict]:
        """Identify fans meeting timewaster criteria per model.

        Criteria: < $50 spend AND > 50 messages AND < $0.05 RPM

        :param session: Database session.
        :return: List of timewaster records.
        """
        self.logger.info("Detecting per-model timewasters...")

        query = select(FanTimewasterAnalysis).where(
            and_(
                FanTimewasterAnalysis.total_spend < self.config.max_spend,
                FanTimewasterAnalysis.total_messages > self.config.min_messages,
                FanTimewasterAnalysis.rpm < self.config.max_rpm,
            )
        )

        result = await session.execute(query)
        timewasters = result.scalars().all()

        records = []
        for tw in timewasters:
            records.append({
                'fan_id': tw.fan_id,
                'model_id': tw.model_id,
                'model_name': tw.model_name,
                'total_messages': tw.total_messages,
                'total_spend': float(tw.total_spend),
                'rpm': float(tw.rpm),
                'classification': 'timewaster',
            })

        self.logger.info(f"Found {len(records)} per-model timewasters")
        return records

    async def detect_cross_model_timewasters(self, session: AsyncSession) -> List[Dict]:
        """Find fans with $0 RPM across ALL models for auto-restriction.

        :param session: Database session.
        :return: List of fans for auto-restriction.
        """
        self.logger.info("Detecting cross-model timewasters for auto-restrict...")

        cross_model_query = text("""
            WITH fan_model_summary AS (
                SELECT
                    fan_id,
                    COUNT(DISTINCT model_id) as total_models,
                    SUM(total_messages) as total_messages_all_models,
                    SUM(total_spend) as total_spend_all_models,
                    AVG(rpm) as avg_rpm_all_models,
                    MIN(rpm) as min_rpm,
                    MAX(rpm) as max_rpm,
                    SUM(CASE WHEN rpm = 0 THEN 1 ELSE 0 END) as zero_rpm_model_count
                FROM fan_timewaster_analysis
                GROUP BY fan_id
            )
            SELECT
                fan_id,
                total_models,
                total_messages_all_models,
                total_spend_all_models,
                avg_rpm_all_models,
                min_rpm,
                max_rpm,
                CASE
                    WHEN zero_rpm_model_count = total_models AND total_models > 0
                    THEN 'auto_restrict'
                    WHEN total_spend_all_models = 0 AND total_messages_all_models > :min_messages
                    THEN 'auto_restrict'
                    ELSE 'review'
                END as flag_type,
                CASE
                    WHEN zero_rpm_model_count = total_models AND total_models > 0
                    THEN '$0 RPM across all ' || total_models || ' models'
                    WHEN total_spend_all_models = 0 AND total_messages_all_models > :min_messages
                    THEN '$0 total spend with ' || total_messages_all_models || ' messages'
                    ELSE 'Varying RPM - needs review'
                END as flag_reason
            FROM fan_model_summary
            WHERE total_spend_all_models < :max_spend
              AND total_messages_all_models > :min_messages
        """)

        result = await session.execute(
            cross_model_query,
            {
                'max_spend': self.config.max_spend,
                'min_messages': self.config.min_messages,
            },
        )
        rows = result.fetchall()

        auto_restrict = []
        review = []

        for row in rows:
            record = {
                'fan_id': str(row.fan_id),
                'total_models': row.total_models,
                'total_messages_all_models': row.total_messages_all_models,
                'total_spend_all_models': float(row.total_spend_all_models or 0),
                'avg_rpm_all_models': float(row.avg_rpm_all_models or 0),
                'min_rpm': float(row.min_rpm or 0),
                'max_rpm': float(row.max_rpm or 0),
                'flag_type': row.flag_type,
                'flag_reason': row.flag_reason,
            }

            if row.flag_type == 'auto_restrict':
                auto_restrict.append(record)
            else:
                review.append(record)

        self.logger.info(f"Auto-restrict candidates: {len(auto_restrict)}")
        self.logger.info(f"Review candidates: {len(review)}")

        all_flags = auto_restrict + review
        await self._upsert_flags(session, all_flags)

        return auto_restrict

    async def detect_varying_rpm_fans(self, session: AsyncSession) -> List[Dict]:
        """Find fans with varying RPM across models for review.

        Fans who spend on some models but not others.

        :param session: Database session.
        :return: List of fans for manual review.
        """
        self.logger.info("Detecting fans with varying RPM across models...")

        varying_query = text("""
            SELECT
                fan_id,
                COUNT(DISTINCT model_id) as model_count,
                MAX(rpm) as max_rpm,
                MIN(rpm) as min_rpm,
                (MAX(rpm) - MIN(rpm)) as rpm_variance,
                AVG(rpm) as avg_rpm,
                SUM(total_messages) as total_messages,
                SUM(total_spend) as total_spend
            FROM fan_timewaster_analysis
            GROUP BY fan_id
            HAVING COUNT(DISTINCT model_id) > 1
               AND MAX(rpm) > :max_rpm
               AND MIN(rpm) = 0
            ORDER BY (MAX(rpm) - MIN(rpm)) DESC
        """)

        result = await session.execute(
            varying_query,
            {'max_rpm': self.config.max_rpm},
        )
        rows = result.fetchall()

        records = []
        for row in rows:
            records.append({
                'fan_id': str(row.fan_id),
                'model_count': row.model_count,
                'max_rpm': float(row.max_rpm or 0),
                'min_rpm': float(row.min_rpm or 0),
                'rpm_variance': float(row.rpm_variance or 0),
                'avg_rpm': float(row.avg_rpm or 0),
                'total_messages': row.total_messages,
                'total_spend': float(row.total_spend or 0),
                'flag_type': 'review',
                'flag_reason': f'Varying RPM: ${row.max_rpm:.4f} max, $0 min across {row.model_count} models',
            })

        self.logger.info(f"Found {len(records)} fans with varying RPM")
        return records

    async def _upsert_flags(self, session: AsyncSession, flags: List[Dict]) -> None:
        """Upsert timewaster flag records.

        :param session: Database session.
        :param flags: List of flag records.
        """
        if not flags:
            return

        now = datetime.utcnow()
        batch = []

        for flag in flags:
            batch.append({
                'fan_id': flag['fan_id'],
                'total_models': flag.get('total_models', 0),
                'total_messages_all_models': flag.get('total_messages_all_models', 0),
                'total_spend_all_models': flag.get('total_spend_all_models', 0),
                'avg_rpm_all_models': flag.get('avg_rpm_all_models', 0),
                'min_rpm': flag.get('min_rpm', 0),
                'max_rpm': flag.get('max_rpm', 0),
                'flag_type': flag['flag_type'],
                'flag_reason': flag.get('flag_reason'),
                'created_at': now,
                'updated_at': now,
            })

        stmt = insert(FanTimewasterFlag).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=['fan_id'],
            set_={
                'total_models': stmt.excluded.total_models,
                'total_messages_all_models': stmt.excluded.total_messages_all_models,
                'total_spend_all_models': stmt.excluded.total_spend_all_models,
                'avg_rpm_all_models': stmt.excluded.avg_rpm_all_models,
                'min_rpm': stmt.excluded.min_rpm,
                'max_rpm': stmt.excluded.max_rpm,
                'flag_type': stmt.excluded.flag_type,
                'flag_reason': stmt.excluded.flag_reason,
                'updated_at': stmt.excluded.updated_at,
            },
        )
        await session.execute(stmt)
        await session.commit()

    async def export_report(
        self,
        timewasters: List[Dict],
        auto_restrict: List[Dict],
        varying_rpm: List[Dict],
        output_dir: str = 'output',
    ) -> Dict[str, str]:
        """Export timewaster analysis to CSV files.

        :param timewasters: Per-model timewaster records.
        :param auto_restrict: Auto-restrict candidate records.
        :param varying_rpm: Varying RPM records.
        :param output_dir: Directory for output files.
        :return: Dict of report type to file path.
        """
        report_dir = Path(output_dir) / 'timewaster_reports'
        report_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now().strftime('%Y-%m-%d')
        paths = {}

        if timewasters:
            tw_path = report_dir / f'timewaster_per_model_{date_str}.csv'
            with open(tw_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=timewasters[0].keys())
                writer.writeheader()
                writer.writerows(timewasters)
            paths['per_model'] = str(tw_path)
            self.logger.info(f"Exported per-model report: {tw_path}")

        if auto_restrict:
            ar_path = report_dir / f'timewaster_auto_restrict_{date_str}.csv'
            with open(ar_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=auto_restrict[0].keys())
                writer.writeheader()
                writer.writerows(auto_restrict)
            paths['auto_restrict'] = str(ar_path)
            self.logger.info(f"Exported auto-restrict report: {ar_path}")

        if varying_rpm:
            vr_path = report_dir / f'timewaster_varying_rpm_{date_str}.csv'
            with open(vr_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=varying_rpm[0].keys())
                writer.writeheader()
                writer.writerows(varying_rpm)
            paths['varying_rpm'] = str(vr_path)
            self.logger.info(f"Exported varying RPM report: {vr_path}")

        return paths

    async def run_analysis(self) -> Dict:
        """Run complete timewaster analysis.

        :return: Summary statistics.
        """
        self.logger.info("=" * 70)
        self.logger.info("TIMEWASTER DETECTION - Starting analysis")
        self.logger.info("=" * 70)
        self.logger.info(f"Thresholds: spend < ${self.config.max_spend}, "
                        f"messages > {self.config.min_messages}, "
                        f"RPM < ${self.config.max_rpm}")

        async with self.async_session_maker() as session:
            rpm_count = await self.analyze_fan_model_rpm(session)

            timewasters = await self.detect_timewasters(session)

            auto_restrict = await self.detect_cross_model_timewasters(session)

            varying_rpm = await self.detect_varying_rpm_fans(session)

        report_paths = await self.export_report(
            timewasters, auto_restrict, varying_rpm
        )

        summary = {
            'analyzed_pairs': rpm_count,
            'timewasters_per_model': len(timewasters),
            'auto_restrict_candidates': len(auto_restrict),
            'varying_rpm_review': len(varying_rpm),
            'report_paths': report_paths,
            'analyzed_at': datetime.utcnow().isoformat(),
        }

        self.logger.info("=" * 70)
        self.logger.info("ANALYSIS COMPLETE")
        self.logger.info(f"  Fan-model pairs analyzed: {rpm_count}")
        self.logger.info(f"  Per-model timewasters: {len(timewasters)}")
        self.logger.info(f"  Auto-restrict candidates: {len(auto_restrict)}")
        self.logger.info(f"  Varying RPM (review): {len(varying_rpm)}")
        self.logger.info("=" * 70)

        return summary

    async def run(self) -> None:
        """Run timewaster detection on schedule."""
        self.logger.info(f"Timewaster Detector starting")
        self.logger.info(f"Run interval: {self.run_interval // 3600} hours")

        while True:
            try:
                summary = await self.run_analysis()
                self.logger.info(f"Next analysis in {self.run_interval // 3600} hours")
            except Exception as e:
                self.logger.error(f"Analysis failed: {str(e)}")
                import traceback
                traceback.print_exc()

            await asyncio.sleep(self.run_interval)

    async def cleanup(self) -> None:
        """Close database connection."""
        if self.engine:
            await self.engine.dispose()
            self.logger.info("Database connection closed")


async def main():
    """Main entry point."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL environment variable not set")
        sys.exit(1)

    run_interval = int(os.getenv('RUN_INTERVAL', '43200'))

    config = TimewasterConfig(
        max_spend=float(os.getenv('TW_MAX_SPEND', '50.00')),
        min_messages=int(os.getenv('TW_MIN_MESSAGES', '50')),
        max_rpm=float(os.getenv('TW_MAX_RPM', '0.05')),
    )

    detector = TimewasterDetector(db_url, config, run_interval)

    if not await detector.initialize():
        print("=" * 70)
        print("FATAL: Failed to initialize timewaster detector")
        print("Database connection or table creation failed")
        print("Container will exit now")
        print("=" * 70)
        sys.exit(1)

    try:
        await detector.run()
    finally:
        await detector.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
