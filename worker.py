import ydb
import logging
import time
from random import randint
from constants import TELLERS_PER_BRANCH, ACCOUNTS_PER_BRANCH
from metrics import MetricsCollector

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, bid_from: int, bid_to: int, count: int, metrics_collector: MetricsCollector = None):
        """
        Initialize a worker that executes transactions.
        
        Args:
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            count: Number of transactions to execute
            metrics_collector: Optional metrics collector for tracking performance
        """
        self._bid_from = bid_from
        self._bid_to = bid_to
        self._count = count
        self._bid = None
        self._metrics = metrics_collector

    async def execute_pooled(self, pool: ydb.aio.QuerySessionPool):
        """
        Execute transactions using the pool's retry mechanism.
        
        Args:
            pool: YDB query session pool
        """
        logger.info(f"Worker [{self._bid_from}, {self._bid_to}] started")
        for i in range(self._count):
            self._bid = randint(self._bid_from, self._bid_to)
            await pool.retry_operation_async(self._execute_workload)
        logger.info(f"Worker [{self._bid_from}, {self._bid_to}] completed")

    async def execute_single_session(self, pool: ydb.aio.QuerySessionPool):
        """
        Execute transactions using a single acquired session.
        
        Args:
            pool: YDB query session pool
        """
        logger.info(f"Worker [{self._bid_from}, {self._bid_to}] started")
        session = await pool.acquire()
        try:
            for i in range(self._count):
                self._bid = randint(self._bid_from, self._bid_to)
                await self._execute_workload(session)
        finally:
            await pool.release(session)
        logger.info(f"Worker [{self._bid_from}, {self._bid_to}] completed")

    async def _execute_workload(self, session: ydb.aio.QuerySession):
        """
        Execute a single pgbench-like transaction.
        
        Args:
            session: YDB query session
        """
        bid = self._bid
        tid = (bid - 1) * TELLERS_PER_BRANCH + randint(1, TELLERS_PER_BRANCH)
        aid = (bid - 1) * ACCOUNTS_PER_BRANCH + randint(1, ACCOUNTS_PER_BRANCH)
        delta = randint(1, 1000)

        start_time = time.time()
        success = False
        error_message = ""
        total_duration_us = 0
        total_cpu_time_us = 0
        
        try:
            async with session.transaction() as tx:
                async with await tx.execute(
                    """
                        UPDATE `pgbench/accounts` SET abalance = abalance + $delta WHERE aid = $aid;
                        SELECT abalance FROM `pgbench/accounts` WHERE aid = $aid;
                        UPDATE `pgbench/tellers` SET tbalance = tbalance + $delta WHERE tid = $tid;
                        UPDATE `pgbench/branches` SET bbalance = bbalance + $delta WHERE bid = $bid;
                        INSERT INTO `pgbench/history` (tid, bid, aid, delta, mtime)
                        VALUES ($tid, $bid, $aid, $delta, CurrentUtcTimestamp());
                    """,
                    parameters={
                        "$tid": ydb.TypedValue(tid, ydb.PrimitiveType.Int32),
                        "$bid": ydb.TypedValue(bid, ydb.PrimitiveType.Int32),
                        "$aid": ydb.TypedValue(aid, ydb.PrimitiveType.Int32),
                        "$delta": ydb.TypedValue(delta, ydb.PrimitiveType.Int32),
                    },
                    commit_tx=True,
                    stats_mode=ydb.QueryStatsMode.BASIC,
                ) as results:
                    async for result in results:
                        # All resultsets should be obtained to get last_query_stats
                        pass
                    total_duration_us = tx.last_query_stats.total_duration_us
                    total_cpu_time_us = tx.last_query_stats.total_cpu_time_us
            success = True
        except Exception as e:
            error_message = str(e)
            logger.error(f"Transaction failed for bid={self._bid}: {e}", exc_info=True)
            raise
        finally:
            end_time = time.time()
            if self._metrics:
                self._metrics.record_transaction(
                    start_time,
                    end_time,
                    success,
                    error_message,
                    total_duration_us,
                    total_cpu_time_us
                )