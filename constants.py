"""
Constants for YDB pgbench implementation.

These values match the original pgbench specification and must remain
consistent between initialization and workload execution.
"""

# Number of tellers per branch (hardcoded in original pgbench)
TELLERS_PER_BRANCH = 10

# Number of accounts per branch (hardcoded in original pgbench)
ACCOUNTS_PER_BRANCH = 100000