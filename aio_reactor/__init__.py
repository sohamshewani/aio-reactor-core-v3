"""
aio_reactor/__init__.py
Core initialization and public API exposure for 'aio-reactor-core'.
Implements high-performance, zero-allocation steady-state event loops,
hierarchical timer wheels, arena allocators, and multiplexer subsystems.
"""

from __future__ import annotations

import sys
from typing import Final

# Package Metadata
__version__: Final[str] = "1.0.0"
__author__: Final[str] = "Senior Staff Systems Engineer"
__all__ = (
    "ArenaAllocator",
    "TimerWheel",
    "ConnectionMultiplexer",
    "ReactorEngine",
    "ReactorConfig",
    "ReactorState",
    "HandlerResult",
    "SystemError",
    "AllocationError",
    "TimerError",
    "MultiplexerError",
    "ReactorError",
)

# Enforce Python Version Constraints for Modern Structural Types
if sys.version_info < (3, 10):
    raise RuntimeError("aio-reactor-core requires Python 3.10 or newer.")

from .allocator import ArenaAllocator, AllocationError
from .engine import ReactorConfig, ReactorEngine, ReactorState, ReactorError
from .multiplexer import ConnectionMultiplexer, MultiplexerError, HandlerResult
from .timers import TimerWheel, TimerError

class SystemError(Exception):
    """Base system error for all aio-reactor-core anomalies."""
    pass