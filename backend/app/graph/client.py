"""
Async Neo4j driver wrapper with connection pooling and structured logging.
"""

from __future__ import annotations

import structlog
from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession

from app.config import get_settings

logger = structlog.get_logger(__name__)


class Neo4jClient:
    """
    Thin async wrapper around the official Neo4j Python driver.
    Manages a single connection pool shared across the application.
    """

    def __init__(self) -> None:
        self._driver: AsyncDriver | None = None
        self._settings = get_settings()

    async def connect(self) -> None:
        self._driver = AsyncGraphDatabase.driver(
            self._settings.neo4j_uri,
            auth=(self._settings.neo4j_user, self._settings.neo4j_password),
            max_connection_pool_size=self._settings.neo4j_max_connection_pool_size,
        )
        await self._driver.verify_connectivity()
        logger.info("neo4j_connected", uri=self._settings.neo4j_uri)

    async def close(self) -> None:
        if self._driver:
            await self._driver.close()
            logger.info("neo4j_disconnected")

    def session(self, **kwargs) -> AsyncSession:
        if not self._driver:
            raise RuntimeError("Neo4j driver not initialized. Call connect() first.")
        return self._driver.session(**kwargs)

    async def run(
        self,
        query: str,
        parameters: dict | None = None,
        **kwargs,
    ) -> list[dict]:
        """
        Execute a Cypher query and return results as a list of dicts.
        Suitable for read queries and small write operations.
        """
        async with self.session() as session:
            result = await session.run(query, parameters or {}, **kwargs)
            records = await result.data()
            return records

    async def run_write(
        self,
        query: str,
        parameters: dict | None = None,
    ) -> list[dict]:
        """
        Execute a write transaction with auto-retry on transient errors.
        """

        async def _tx(tx):
            result = await tx.run(query, parameters or {})
            return await result.data()

        async with self.session() as session:
            return await session.execute_write(_tx)

    async def run_batch(self, queries: list[tuple[str, dict]]) -> None:
        """
        Execute a batch of write operations in a single transaction.
        Used by the graph writer for atomic lineage event commits.
        """

        async def _tx(tx):
            for query, params in queries:
                await tx.run(query, params)

        async with self.session() as session:
            await session.execute_write(_tx)


# ── Singleton ─────────────────────────────────────────────────────────────────

_client: Neo4jClient | None = None


def get_client() -> Neo4jClient:
    global _client
    if _client is None:
        _client = Neo4jClient()
    return _client
