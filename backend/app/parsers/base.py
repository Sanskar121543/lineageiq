"""
Abstract base class for all LineageIQ source parsers.

Each parser is responsible for one source system (dbt, Spark, SQL, Kafka).
Parsers emit normalized LineageEvents consumed by the graph writer.

To add a new source:
1. Subclass BaseParser
2. Implement parse(config) -> list[LineageEvent]
3. Register in workers/tasks.py
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import structlog

from app.models.lineage import LineageEvent, SourceType

logger = structlog.get_logger(__name__)


class BaseParser(ABC):
    """
    Abstract parser. Subclasses implement the parse() method for their
    specific source format and emit normalized LineageEvents.
    """

    source_type: SourceType  # must be set on every subclass

    @abstractmethod
    def parse(self, config: dict[str, Any]) -> list[LineageEvent]:
        """
        Parse a source artifact and return a list of LineageEvents.

        Args:
            config: Source-specific configuration. Each subclass documents
                    the required keys in its class docstring.

        Returns:
            List of LineageEvent objects, one per logical transform found.
        """
        ...

    def _log_parse_start(self, **kwargs) -> None:
        logger.info(
            "parser_start",
            source_type=self.source_type.value,
            **kwargs,
        )

    def _log_parse_complete(self, event_count: int, **kwargs) -> None:
        logger.info(
            "parser_complete",
            source_type=self.source_type.value,
            events_emitted=event_count,
            **kwargs,
        )
