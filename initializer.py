import ydb
from constants import TELLERS_PER_BRANCH, ACCOUNTS_PER_BRANCH


class Initializer:
    def __init__(self, scale: int = 100):
        """
        Initialize the Initializer with a scale factor.
        
        Args:
            scale: Number of branches to create (default: 100)
        """
        self._scale = scale

    async def create_tables(self, pool: ydb.aio.QuerySessionPool):
        """Create the pgbench tables in the database."""
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
        """
        Fill data for a single branch. Called in parallel by Runner.
        
        Args:
            pool: YDB query session pool
            bid: Branch ID to fill
            
        Raises:
            ValueError: If bid is out of valid range [1, scale]
        """
        if bid < 1 or bid > self._scale:
            raise ValueError(f"Branch ID {bid} is out of valid range [1, {self._scale}]")
        
        await pool.execute_with_retries(
            """
            $d = SELECT d FROM (SELECT AsList(0,1,2,3,4,5,6,7,8,9) as d) FLATTEN LIST BY (d);

            REPLACE INTO `pgbench/branches`(bid, bbalance, filler)
            VALUES ($bid, 0 , null);

            REPLACE INTO `pgbench/tellers`(tid, bid, tbalance, filler)
            SELECT
                ($bid-1)*$tellers_per_branch+d1.d+1 as tid, $bid, 0 , null
            FROM
                $d as d1;

            REPLACE INTO `pgbench/accounts`(aid, bid, abalance, filler)
            SELECT
                ($bid-1)*$accounts_per_branch + rn + 1 as aid,
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
                    "$bid": ydb.TypedValue(bid, ydb.PrimitiveType.Int32),
                    "$tellers_per_branch": ydb.TypedValue(TELLERS_PER_BRANCH, ydb.PrimitiveType.Int32),
                    "$accounts_per_branch": ydb.TypedValue(ACCOUNTS_PER_BRANCH, ydb.PrimitiveType.Int32)
            }
        )