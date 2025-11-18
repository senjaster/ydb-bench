import ydb
import asyncio
from contextlib import asynccontextmanager
from typing import Optional
import math


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
        from initializer import Initializer
        
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
        """Run workload with specified number of workers and transactions."""
        asyncio.run(self._execute_parallel(worker_count, tran_count))

    async def _execute_parallel(self, worker_count: int, tran_count: int):
        """Execute workload in parallel with multiple workers."""
        from worker import Worker
        
        async with self._get_pool() as pool:
            async with asyncio.TaskGroup() as tg:
                for i in range(worker_count):
                    bid_from = math.floor(100.0/worker_count*i)+1
                    bid_to = math.floor(100.0/worker_count*(i+1))
                    worker = Worker(bid_from, bid_to, tran_count)
                    tg.create_task(worker.execute_pooled(pool))
            print("Done")