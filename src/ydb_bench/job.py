import logging
import re
import time
from random import randint
from typing import Any, Dict, Optional

import ydb

from .base_executor import BaseExecutor
from .constants import ACCOUNTS_PER_BRANCH, DEFAULT_SCRIPT, TELLERS_PER_BRANCH
from .constants import DurationUnit
from .metrics import MetricsCollector
from .workload import WeightedScriptSelector, WorkloadScript

logger = logging.getLogger(__name__)


class Job(BaseExecutor):
    """
    Executes pgbench-like workload transactions.

    Uses random branch selection within the range for each transaction.
    """

    def __init__(
        self,
        workload_start_time: float,
        duration: int,
        duration_unit: DurationUnit,
        bid_from: int,
        bid_to: int,
        metrics_collector: Optional[MetricsCollector] = None,
        table_folder: str = "pgbench",
        use_single_session: bool = False,
        script_selector: Optional[WeightedScriptSelector] = None,
    ):
        """
        Initialize a job that executes transactions.

        Args:
            bid_from: Starting branch ID (inclusive)
            bid_to: Ending branch ID (inclusive)
            tran_count: Number of transactions to execute (includes preheat transactions)
            metrics_collector: Optional metrics collector for tracking performance
            table_folder: Folder name for tables (default: "pgbench")
            use_single_session: If True, use single session mode; if False, use pooled mode
            script_selector: Optional WeightedScriptSelector for multiple weighted scripts (if None, uses default script)
            preheat: Number of preheat transactions to run before counting metrics (default: 0)
        """
        super().__init__(
            bid_from,
            bid_to,
            0,
            metrics_collector,
            table_folder,
            use_single_session,
        )
        self._workload_start_time = workload_start_time
        self._duration = duration
        self._duration_unit = duration_unit

        # Create script selector if none provided
        if script_selector is None:
            # Create default selector with DEFAULT_SCRIPT
            default_script = WorkloadScript(
                filepath="<default>",
                content=DEFAULT_SCRIPT,
                weight=1.0,
                table_folder=table_folder,
            )
            self._script_selector = WeightedScriptSelector([default_script])
        else:
            self._script_selector = script_selector

    def _build_parameters(self, script: WorkloadScript, iteration: int) -> Dict[str, Any]:
        """
        Build parameters dictionary based on what the selected script uses.
        Generates random values for bid, tid, aid, and delta.

        Args:
            script: WorkloadScript with parameter usage flags
            iteration: Current iteration number

        Returns:
            Dictionary of parameters for the query
        """
        # Generate random values
        bid = randint(self._bid_from, self._bid_to)
        tid = (bid - 1) * TELLERS_PER_BRANCH + randint(1, TELLERS_PER_BRANCH)
        aid = (bid - 1) * ACCOUNTS_PER_BRANCH + randint(1, ACCOUNTS_PER_BRANCH)
        delta = randint(1, 1000)

        parameters = {}
        if script.uses_bid:
            parameters["$bid"] = ydb.TypedValue(bid, ydb.PrimitiveType.Int32)
        if script.uses_tid:
            parameters["$tid"] = ydb.TypedValue(tid, ydb.PrimitiveType.Int32)
        if script.uses_aid:
            parameters["$aid"] = ydb.TypedValue(aid, ydb.PrimitiveType.Int32)
        if script.uses_delta:
            parameters["$delta"] = ydb.TypedValue(delta, ydb.PrimitiveType.Int32)
        if script.uses_iteration:
            parameters["$iteration"] = ydb.TypedValue(iteration, ydb.PrimitiveType.Int32)
        return parameters

    async def _execute_operation(self, session: ydb.aio.QuerySession, iteration: int) -> None:
        """
        Execute a single pgbench-like transaction with randomly selected script.

        Args:
            session: YDB query session
            iteration: Current iteration number (0-based)
        """

        success = False
        error_message = ""
        total_duration_us = 0
        total_cpu_time_us = 0

        end_time = None

        try:
            # Select script for this transaction
            script_content, script = self._script_selector.get_script_with_params()

            # Build parameters dictionary based on what this specific script uses
            parameters = self._build_parameters(script, iteration)

            start_time = time.time()

            async with session.transaction() as tx:
                async with await tx.execute(
                    script_content,
                    parameters=parameters,
                    commit_tx=True,
                    stats_mode=ydb.QueryStatsMode.BASIC,
                ) as results:
                    async for result in results:
                        # All results should be obtained to get last_query_stats
                        pass
                    end_time = time.time()
                    total_duration_us = tx.last_query_stats.total_duration_us 
                    total_cpu_time_us = tx.last_query_stats.total_cpu_time_us 
            success = True
        except Exception as e:
            error_message = str(e)
            raise
        finally:
            # Only record metrics if not in preheat phase
            if end_time is None:
                end_time = time.time()
            if iteration >= 0 and self._metrics:
                self._metrics.record_transaction(
                    script.filepath,
                    start_time,
                    end_time,
                    success,
                    error_message,
                    total_duration_us,
                    total_cpu_time_us,
                )

    async def _execute_pooled(self, pool: ydb.aio.QuerySessionPool) -> None:
        """
        Execute operations using pool's retry mechanism.

        Args:
            pool: YDB query session pool
        """

        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] preheat started")
        while time.time() < self._workload_start_time:
            try:
                await pool.retry_operation_async(lambda session: self._execute_operation(session, -1))
            except Exception as e:
                logging.error("Retry limit exceeded")
                logging.error(e)
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] preheat completed")

        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] workload started")

        if self._duration_unit == DurationUnit.SECOND:
            i = 0
            while time.time() < self._workload_start_time + self._duration:
                try:
                    await pool.retry_operation_async(lambda session: self._execute_operation(session, i))
                    i = i + 1
                except Exception as e:
                    logging.error("Retry limit exceeded")
                    logging.error(e)
        else:
            for i in range(self._duration):
                try:
                    await pool.retry_operation_async(lambda session: self._execute_operation(session, i))
                except Exception as e:
                    logging.error("Retry limit exceeded")
                    logging.error(e)
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] workload completed")

    async def _execute_single_session(self, pool: ydb.aio.QuerySessionPool) -> None:
        """
        Execute operations using a single acquired session.

        Args:
            pool: YDB query session pool
        """

        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] preheat started")
        while time.time() < self._workload_start_time:
            try:
                await pool.retry_operation_async(lambda session: self._execute_operation(session, 0))
            except Exception as e:
                logging.error("Retry limit exceeded")
                logging.error(e)
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] preheat completed")

        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] workload started")
        session = await pool.acquire()

        if self._duration_unit == DurationUnit.SECOND:
            i = 0
            try:
                while time.time() < self._workload_start_time + self._duration:
                    await self._execute_operation(session, i)
                    i = i + 1
            finally:
                await pool.release(session)
        else:
            try:
                for i in range(self._duration):
                    await self._execute_operation(session, i)
            finally:
                await pool.release(session)
        logger.info(f"{self.__class__.__name__} [{self._bid_from}, {self._bid_to}] workload completed")
