import ydb
import asyncio
from contextlib import asynccontextmanager
from random import randint
from typing import Optional
from functools import partial
import math
from multiprocessing import Pool


class Runner:
    def __init__(
        self,
        endpoint: str,
        database: str,
        root_certificates_file: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 5
    ):
        """
        Initialize YdbExecutor with YDB connection parameters.
        
        Args:
            endpoint: YDB endpoint (e.g., "grpcs://ydb-host:2135")
            database: Database path (e.g., "/Root/database")
            root_certificates_file: Optional path to root certificate file for TLS
            user: Optional username for authentication
            password: Optional password for authentication
            timeout: Connection timeout in seconds (default: 5)
        """
        self._endpoint = endpoint
        self._database = database
        self._timeout = timeout
        
        # Load root certificates from file if provided
        root_certificates = None
        if root_certificates_file:
            root_certificates = ydb.load_ydb_root_certificate(root_certificates_file)
        
        # Create driver configuration
        self._config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            root_certificates=root_certificates
        )
        
        # Create credentials from username and password if provided
        self._credentials = None
        if user and password:
            self._credentials = ydb.StaticCredentials(self._config, user=user, password=password)

    @asynccontextmanager
    async def _get_pool(self):
        """
        Async context manager that creates and yields a YDB QuerySessionPool.
        Handles driver initialization, connection waiting, and cleanup.
        """
        async with ydb.aio.Driver(driver_config=self._config, credentials=self._credentials) as driver:
            await driver.wait()
            print("Connect")
            await asyncio.sleep(3)  # Required to make node discovery work
            print("Start")
            async with ydb.aio.QuerySessionPool(driver) as pool:
                yield pool

    def init_tables(self, scale: int = 100):
        """Initialize database tables with the specified scale factor."""
        async def _init():
            async with self._get_pool() as pool:
                initer = Initializer(scale)
                await initer.create_tables(pool)
                
                # Fill tables in parallel using TaskGroup
                async with asyncio.TaskGroup() as tg:
                    for i in range(scale):
                        tg.create_task(initer.fill_branch(pool, i + 1))
        
        asyncio.run(_init())

    def run(self, worker_count: int = 7, tran_count: int = 100):
        asyncio.run(self._execute_parallel(worker_count, tran_count))

    async def _execute_parallel(self, worker_count: int, tran_count: int):
        async with self._get_pool() as pool:
            async with asyncio.TaskGroup() as tg:
                for i in range(worker_count):
                    bid_from = math.floor(100.0/worker_count*i)+1
                    bid_to = math.floor(100.0/worker_count*(i+1))
                    worker = Worker(bid_from, bid_to, tran_count)
                    tg.create_task(worker.execute_pooled(pool))
            print("Done")

class Initializer:
    def __init__(self, scale: int = 100):
        self._scale = 100;

    async def create_tables(self, pool: ydb.aio.QuerySessionPool):
        await pool.execute_with_retries(
            """
            DROP TABLE IF EXISTS `pgbench/accounts`;
            CREATE TABLE `pgbench/accounts`
            (
                aid Int32,
                bid Int32, 
                abalance Int32,
                filler Utf8,
                PRIMARY KEY(aid)
            );

            DROP TABLE IF EXISTS `pgbench/branches`;
            CREATE TABLE `pgbench/branches`
            (
                bid Int32, 
                bbalance Int32,
                filler Utf8,
                PRIMARY KEY(bid)
            );

            DROP TABLE IF EXISTS `pgbench/tellers`;
            CREATE TABLE `pgbench/tellers`
            (
                tid Int32,
                bid Int32, 
                tbalance Int32,
                filler Utf8,
                PRIMARY KEY(tid)
            );

            DROP TABLE IF EXISTS `pgbench/history`;
            CREATE TABLE `pgbench/history`
            (
                tid Int32,
                bid Int32, 
                aid Int32,
                delta Int32,
                mtime timestamp,
                filler Utf8,
                PRIMARY KEY(aid, mtime)
            );
            """
        )

    async def fill_branch(self, pool: ydb.aio.QuerySessionPool, bid: int):
        """Fill data for a single branch. Called in parallel by Runner."""
        await pool.execute_with_retries(
            """
            $d = SELECT d FROM (SELECT AsList(0,1,2,3,4,5,6,7,8,9) as d) FLATTEN LIST BY (d);

            REPLACE INTO `pgbench/branches`(bid, bbalance, filler)
            VALUES ($bid, 0 , null);

            REPLACE INTO `pgbench/tellers`(tid, bid, tbalance, filler)
            SELECT
                ($bid-1)*10+d1.d+1 as tid, $bid, 0 , null
            FROM
                $d as d1;

            REPLACE INTO `pgbench/accounts`(aid, bid, abalance, filler)
            SELECT
                ($bid-1)*100000 + rn + 1 as aid,
                $bid as bid,
                0 as abalance,
                null as filler
            FROM (
                SELECT
                    d1.d+d2.d*10+d3.d*100+d4.d*1000+d5.d*10000 as rn
                FROM
                    -- 100k rows
                    $d as d1
                    CROSS JOIN $d as d2
                    CROSS JOIN $d as d3
                    CROSS JOIN $d as d4
                    CROSS JOIN $d as d5
                ) t
            """,
            parameters={
                    "$bid": ydb.TypedValue(bid, ydb.PrimitiveType.Int32)
            }
        )

class Worker:
    def __init__(self, bid_from: int, bid_to: int, count: int):
        self._bid_from = bid_from
        self._bid_to = bid_to
        self._count = count
        self._bid = None

    async def execute_pooled(self, pool: ydb.aio.QuerySessionPool):
        print(f"Worker [{self._bid_from}, {self._bid_to}] started")
        for i in range(self._count):
            self._bid = randint(self._bid_from, self._bid_to)
            await pool.retry_operation_async(self._execute_workload)
        print(f"Worker [{self._bid_from}, {self._bid_to}] completed")

    async def execute_single_session(self, pool: ydb.aio.QuerySessionPool):
        print(f"Worker [{self._bid_from}, {self._bid_to}] started")
        session = await pool.acquire()
        try:
            for i in range(self._count):
                self._bid = randint(self._bid_from, self._bid_to)
                await self._execute_workload(session)
        finally:
            await pool.release(session)
        print(f"Worker [{self._bid_from}, {self._bid_to}] completed")

    async def _execute_workload(self, session: ydb.aio.QuerySession):
        bid = self._bid
        tid = (bid - 1) * 10 + randint(1, 10)  
        aid = (bid - 1) * 100000 + randint(1, 100000)
        delta = randint(1, 1000)

        try:
            async with session.transaction() as tx:
                await tx.execute(
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
                    commit_tx=True
                )
        except Exception as e:
            print(e)


def main(pn: int = None):
    if pn is not None:
        print(f"Subprocess {pn}")

    runner = Runner(     
            endpoint = "grpcs://ydb-static-node-2.ydb-cluster.com:2135",
            database = "/Root/database",
            root_certificates_file="./ca.crt",
            user="root"
        )
    # runner.init_tables(100)
    runner.run(100, 1000)

if __name__ == '__main__':
    main()
    # process_count = 4
    # with Pool(process_count) as pool:
    #     pool.map(
    #         main,
    #         [i for i in range(process_count)]
    #     )
    


