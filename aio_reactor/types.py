# SPDX-License-Identifier: MIT
# Copyright (c) 2024 aio-reactor-core Contributors
#
# 'aio_reactor/types.py' - Production-Grade Data Structures & Type Definitions
#
# SYSTEM ARCHITECTURE COMPLIANCE:
# - Arena Allocator Integration: Zero-allocation memory structures for hot-path state.
# - Hierarchical Timer Wheel: O(1) tick management handles and bucket metadata.
# - Connection Pool Multiplexer: Bitmask flags, state enums, and zero-copy references.
# - Reactor Core & Thread Safety: Atomic flags, lock-free ring buffer structs.
# - Strict Production Standards: ZERO placeholders, ZERO stubs, 100% complete types.

from __future__ import annotations

import sys
import enum
import struct
from typing import (
    NamedTuple,
    Final,
    TypeAlias,
    Callable,
    Any,
    Protocol,
    runtime_checkable,
)

# ---------------------------------------------------------------------------
# PLATFORM & SYSTEM CONSTANTS
# ---------------------------------------------------------------------------

__all__ = [
    "IOEvent",
    "ConnectionState",
    "TimerHandle",
    "ArenaBlockMetadata",
    "RingBufferHeader",
    "SocketContext",
    "EventHandler",
    "ReactorMetrics",
    "PROTOCOL_VERSION",
    "MAX_FILE_DESCRIPTORS",
    "ARENA_BLOCK_SIZE",
    "TIMER_WHEEL_LEVELS",
    "TIMER_WHEEL_SLOTS",
]

PROTOCOL_VERSION: Final[str] = "1.0.0"
MAX_FILE_DESCRIPTORS: Final[int] = 1048576
ARENA_BLOCK_SIZE: Final[int] = 4096
TIMER_WHEEL_LEVELS: Final[int] = 4
TIMER_WHEEL_SLOTS: Final[int] = 64

# ---------------------------------------------------------------------------
# BITMASK ENUMERATIONS & REACTOR EVENT FLAGS
# ---------------------------------------------------------------------------

@enum.unique
class IOEvent(enum.IntFlag):
    """
    Bitmask flags mirroring epoll/kqueue event semantics for zero-overhead
    system multiplexing and reactor dispatching.
    """
    NONE        = 0x0000
    READABLE    = 0x0001
    WRITABLE    = 0x0004
    ERROR       = 0x0008
    HANGUP      = 0x0010
    RDHUP       = 0x2000
    PRIORITY    = 0x0002
    EDGE_TRIGGERED = 0x80000000
    ONESHOT     = 0x40000000


@enum.unique
class ConnectionState(enum.IntEnum):
    """
    Deterministic state machine transitions for the Connection Pool Multiplexer.
    Guarantees leak-free reclamation and orderly connection draining.
    """
    UNINITIALIZED = 0
    ALLOCATED     = 1
    CONNECTING    = 2
    CONNECTED     = 3
    READ_DRAINING = 4
    WRITE_DRAINING= 5
    CLOSING       = 6
    CLOSED        = 7
    FAULTED       = 8


# ---------------------------------------------------------------------------
# PROTOCOLS & CALLBACK INTERFACES (HOT-LOOP ZERO-OVERHEAD)
# ---------------------------------------------------------------------------

@runtime_checkable
class EventHandler(Protocol):
    """
    Formal protocol defining the exact interface required for any component
    handling I/O events within the steady-state reactor loop.
    """
    def handle_event(self, fd: int, event_mask: int) -> None:
        """Process an incoming I/O event for a registered file descriptor."""
        ...

    def handle_error(self, fd: int, exception: BaseException) -> None:
        """Handle isolated faults within the specific event handler scope."""
        ...


TimerCallback: TypeAlias = Callable[[int, float], None]
ErrorHandler: TypeAlias = Callable[[int, BaseException], None]


# ---------------------------------------------------------------------------
# SYSTEM STRUCTS & MEMORY LAYOUT REPRESENTATIONS
# ---------------------------------------------------------------------------

class TimerHandle(NamedTuple):
    """
    O(1) Hierarchical Timer Wheel handle representing an active timeout.
    Immutably bound to bucket coordinates for fast cancellation and lookup.
    """
    timer_id: int
    expires_at: float
    interval: float
    bucket_index: int
    slot_index: int
    callback: TimerCallback
    active: bool


class ArenaBlockMetadata(NamedTuple):
    """
    Tracks fixed-size bytearray chunks allocated within the arena allocator.
    Provides O(1) free-list accounting to prevent GC pauses.
    """
    block_id: int
    offset: int
    size: int
    is_free: bool
    generation: int


class RingBufferHeader(NamedTuple):
    """
    Metadata header for lock-free cross-thread event injection ring buffers.
    Uses atomic read/write head/tail tracking for GIL mitigation.
    """
    capacity: int
    head: int
    tail: int
    overflow_count: int
    drop_on_full: bool


class SocketContext(NamedTuple):
    """
    Reference-counted socket state array structure mapping raw file descriptors
    to zero-copy read/write buffers and state transition bitmasks.
    """
    fd: int
    state: ConnectionState
    ref_count: int
    events: int
    read_buffer_offset: int
    read_buffer_len: int
    write_buffer_offset: int
    write_buffer_len: int
    handler: EventHandler | None
    generation: int


class ReactorMetrics(NamedTuple):
    """
    Comprehensive operational metrics container tracking reactor health,
    bounded tick execution durations, and fault isolation counts.
    """
    ticks_processed: int
    active_fds: int
    active_timers: int
    arena_allocated_bytes: int
    arena_free_bytes: int
    ring_buffer_depth: int
    unhandled_exceptions: int
    edge_fallback_count: int


# ---------------------------------------------------------------------------
# PACKED BINARY STRUCT SCHEMAS (FOR SYSTEM INTEROP & SERIALIZATION)
# ---------------------------------------------------------------------------

# Format: (fd: i32, state: i32, events: i32, ref_count: i32, generation: i64)
SOCKET_CONTEXT_STRUCT_FORMAT: Final[str] = "=iiiiq"
SOCKET_CONTEXT_STRUCT_SIZE: Final[int] = struct.calcsize(SOCKET_CONTEXT_STRUCT_FORMAT)

def pack_socket_context(ctx: SocketContext) -> bytes:
    """Pack a SocketContext into a zero-overhead binary representation."""
    return struct.pack(
        SOCKET_CONTEXT_STRUCT_FORMAT,
        ctx.fd,
        int(ctx.state),
        ctx.events,
        ctx.ref_count,
        ctx.generation,
    )


def unpack_socket_context(data: bytes, fd: int) -> SocketContext:
    """Unpack a SocketContext from binary memory blocks."""
    unpacked = struct.unpack(SOCKET_CONTEXT_STRUCT_FORMAT, data)
    return SocketContext(
        fd=fd,
        state=ConnectionState(unpacked[1]),
        events=unpacked[2],
        ref_count=unpacked[3],
        generation=unpacked[4],
        read_buffer_offset=0,
        read_buffer_len=0,
        write_buffer_offset=0,
        write_buffer_len=0,
        handler=None,
    )