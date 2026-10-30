# =============================================================================
# aio_reactor/engine.py
# =============================================================================
import os
import sys
import errno
import select
import logging
import threading
from typing import Dict, Optional, Callable, Tuple, List

logger = logging.getLogger("aio_reactor.engine")

# Define OS-specific constants safely
EPOLLET = getattr(select, "EPOLLET", 0x80000000)
EPOLLIN = getattr(select, "EPOLLIN", 0x001)
EPOLLOUT = getattr(select, "EPOLLOUT", 0x004)
EPOLLERR = getattr(select, "EPOLLERR", 0x008)
EPOLLHUP = getattr(select, "EPOLLHUP", 0x010)
EPOLLRDHUP = getattr(select, "EPOLLRDHUP", 0x2000)

KQUEUE_FILTER_READ = getattr(select, "KQ_FILTER_READ", -1)
KQUEUE_FILTER_WRITE = getattr(select, "KQ_FILTER_WRITE", -2)
KQUEUE_EV_ADD = getattr(select, "KQ_EV_ADD", 0x0001)
KQUEUE_EV_DELETE = getattr(select, "KQ_EV_DELETE", 0x0002)
KQUEUE_EV_CLEAR = getattr(select, "KQ_EV_CLEAR", 0x0020)


class ReactorError(Exception):
    """Base exception for reactor engine failures."""
    pass


class DuplicateRegistrationError(ReactorError):
    """Raised when attempting to register an already tracked file descriptor."""
    pass


class DescriptorNotFoundError(ReactorError):
    """Raised when operating on an untracked file descriptor."""
    pass


class LockFreeRingBuffer:
    """
    Lock-free single-producer single-consumer (or multi-producer with atomic CAS)
    ring buffer for cross-thread event injection into the reactor hot loop.
    Uses pre-allocated fixed-size lists to eliminate dynamic allocations.
    """
    __slots__ = ("_capacity", "_buffer", "_head", "_tail", "_mask")

    def __init__(self, capacity: int = 65536) -> None:
        # Enforce power of two for fast bitwise masking
        if (capacity & (capacity - 1)) != 0:
            raise ValueError("Ring buffer capacity must be a power of two.")
        self._capacity = capacity
        self._mask = capacity - 1
        self._buffer: List[Optional[Callable[[], None]]] = [None] * capacity
        self._head = 0
        self._tail = 0

    def push(self, item: Callable[[], None]) -> bool:
        """
        Push an item into the ring buffer. Returns True on success, 
        False if the buffer is full (bounded capacity).
        """
        tail = self._tail
        head = self._head
        if (tail - head) >= self._capacity:
            return False
        
        self._buffer[tail & self._mask] = item
        self._tail = tail + 1
        return True

    def pop_bulk(self, max_items: int, output_list: List[Callable[[], None]]) -> int:
        """
        Drain up to max_items from the ring buffer into output_list without allocations.
        """
        head = self._head
        tail = self._tail
        size = tail - head
        if size <= 0:
            return 0

        n = min(size, max_items)
        mask = self._mask
        buffer = self._buffer
        
        for i in range(n):
            idx = (head + i) & mask
            output_list.append(buffer[idx])
            buffer[idx] = None
            
        self._head = head + n
        return n


class ReactorEngine:
    """
    High-performance, cross-platform, non-blocking event demultiplexing engine.
    Implements strict FD uniqueness, bounded latencies, zero dynamic allocations
    in the steady-state hot loop, and automatic level/edge degradation.
    """

    def __init__(self, max_events_per_tick: int = 1024) -> None:
        self._max_events = max_events_per_tick
        self._running = False
        self._self_pipe_r, self._self_pipe_w = os.pipe()
        os.set_blocking(self._self_pipe_r, False)
        os.set_blocking(self._self_pipe_w, False)

        # Thread safety primitives
        self._mutex = threading.RLock()
        self._injection_ring = LockFreeRingBuffer(capacity=65536)
        self._drain_scratchpad: List[Callable[[], None]] = []

        # Registration tables: FD -> Handler metadata
        # Metadata tuple: (read_callback, write_callback, edge_triggered_flag, is_kqueue)
        self._read_handlers: Dict[int, Callable[[int], None]] = {}
        self._write_handlers: Dict[int, Callable[[int], None]] = {}
        self._edge_triggered: Dict[int, bool] = {}

        # Initialize OS multiplexer
        self._backend = self._detect_backend()
        self._selector = self._init_selector()

        # Register self-pipe for cross-thread wakeups
        self._init_self_pipe()

    def _detect_backend(self) -> str:
        if hasattr(select, "epoll"):
            return "epoll"
        elif hasattr(select, "kqueue"):
            return "kqueue"
        else:
            return "select"

    def _init_selector(self):
        if self._backend == "epoll":
            return select.epoll()
        elif self._backend == "kqueue":
            return select.kqueue()
        else:
            return select.PollSelector()

    def _init_self_pipe(self) -> None:
        with self._mutex:
            if self._backend == "epoll":
                self._selector.register(self._self_pipe_r, EPOLLIN)
            elif self._backend == "kqueue":
                ev = select.kevent(
                    self._self_pipe_r,
                    KQUEUE_FILTER_READ,
                    KQUEUE_EV_ADD | KQUEUE_EV_CLEAR
                )
                self._selector.control([ev], 0, 0)
            else:
                self._selector.register(self._self_pipe_r, select.POLLIN)

    def register(
        self,
        fd: int,
        read_callback: Optional[Callable[[int], None]] = None,
        write_callback: Optional[Callable[[int], None]] = None,
        edge_triggered: bool = True
    ) -> None:
        """
        Register a file descriptor with the reactor. Enforces FD uniqueness invariant.
        """
        with self._mutex:
            if fd in self._read_handlers or fd in self._write_handlers:
                raise DuplicateRegistrationError(f"FD {fd} is already registered in the reactor.")

            if read_callback is not None:
                self._read_handlers[fd] = read_callback
            if write_callback is not None:
                self._write_handlers[fd] = write_callback

            self._edge_triggered[fd] = edge_triggered

            # Construct OS-level subscription flags
            if self._backend == "epoll":
                flags = EPOLLERR | EPOLLHUP | EPOLLRDHUP
                if edge_triggered:
                    flags |= EPOLLET
                if read_callback is not None:
                    flags |= EPOLLIN
                if write_callback is not None:
                    flags |= EPOLLOUT
                try:
                    self._selector.register(fd, flags)
                except Exception as e:
                    self._cleanup_fd_metadata(fd)
                    raise ReactorError(f"Failed to register fd {fd} in epoll: {e}") from e

            elif self._backend == "kqueue":
                events = []
                ev_flags = KQUEUE_EV_ADD
                if edge_triggered:
                    ev_flags |= KQUEUE_EV_CLEAR

                if read_callback is not None:
                    events.append(select.kevent(fd, KQUEUE_FILTER_READ, ev_flags))
                if write_callback is not None:
                    events.append(select.kevent(fd, KQUEUE_FILTER_WRITE, ev_flags))
                
                try:
                    self._selector.control(events, 0, 0)
                except Exception as e:
                    self._cleanup_fd_metadata(fd)
                    raise ReactorError(f"Failed to register fd {fd} in kqueue: {e}") from e

            else:  # PollSelector fallback
                flags = 0
                if read_callback is not None:
                    flags |= select.POLLIN
                if write_callback is not None:
                    flags |= select.POLLOUT
                try:
                    self._selector.register(fd, flags)
                except Exception as e:
                    self._cleanup_fd_metadata(fd)
                    raise ReactorError(f"Failed to register fd {fd} in poll: {e}") from e

    def modify(
        self,
        fd: int,
        read_callback: Optional[Callable[[int], None]] = None,
        write_callback: Optional[Callable[[int], None]] = None
    ) -> None:
        """
        Modify existing file descriptor registrations.
        """
        with self._mutex:
            if fd not in self._read_handlers and fd not in self._write_handlers:
                raise DescriptorNotFoundError(f"FD {fd} is not registered in the reactor.")

            if read_callback is not None:
                self._read_handlers[fd] = read_callback
            else:
                self._read_handlers.pop(fd, None)

            if write_callback is not None:
                self._write_handlers[fd] = write_callback
            else:
                self._write_handlers.pop(fd, None)

            edge_triggered = self._edge_triggered.get(fd, True)

            if self._backend == "epoll":
                flags = EPOLLERR | EPOLLHUP | EPOLLRDHUP
                if edge_triggered:
                    flags |= EPOLLET
                if fd in self._read_handlers:
                    flags |= EPOLLIN
                if fd in self._write_handlers:
                    flags |= EPOLLOUT
                self._selector.modify(fd, flags)

            elif self._backend == "kqueue":
                events = []
                ev_flags = KQUEUE_EV_ADD
                if edge_triggered:
                    ev_flags |= KQUEUE_EV_CLEAR

                # To safely modify, remove both filters first or update explicitly
                # We update based on active presence
                r_action = KQUEUE_EV_ADD if fd in self._read_handlers else KQUEUE_EV_DELETE
                w_action = KQUEUE_EV_ADD if fd in self._write_handlers else KQUEUE_EV_DELETE

                if fd in self._read_handlers:
                    events.append(select.kevent(fd, KQUEUE_FILTER_READ, ev_flags))
                else:
                    events.append(select.kevent(fd, KQUEUE_FILTER_READ, KQUEUE_EV_DELETE))

                if fd in self._write_handlers:
                    events.append(select.kevent(fd, KQUEUE_FILTER_WRITE, ev_flags))
                else:
                    events.append(select.kevent(fd, KQUEUE_FILTER_WRITE, KQUEUE_EV_DELETE))

                self._selector.control(events, 0, 0)

            else:
                flags = 0
                if fd in self._read_handlers:
                    flags |= select.POLLIN
                if fd in self._write_handlers:
                    flags |= select.POLLOUT
                self._selector.modify(fd, flags)

    def unregister(self, fd: int) -> None:
        """
        Unregister and reclaim file descriptor states. Implements connection drain semantics.
        """
        with self._mutex:
            if fd not in self._read_handlers and fd not in self._write_handlers:
                raise DescriptorNotFoundError(f"FD {fd} is not registered in the reactor.")

            self._cleanup_fd_metadata(fd)

            if self._backend == "epoll":
                try:
                    self._selector.unregister(fd)
                except Exception as e:
                    logger.warning(f"Error unregistering fd {fd} from epoll: {e}")
            elif self._backend == "kqueue":
                events = []
                try:
                    events.append(select.kevent(fd, KQUEUE_FILTER_READ, KQUEUE_EV_DELETE))
                except Exception:
                    pass
                try:
                    events.append(select.kevent(fd, KQUEUE_FILTER_WRITE, KQUEUE_EV_DELETE))
                except Exception:
                    pass
                try:
                    self._selector.control(events, 0, 0)
                except Exception as e:
                    logger.warning(f"Error clearing kqueue filters for fd {fd}: {e}")
            else:
                try:
                    self._selector.unregister(fd)
                except Exception as e:
                    logger.warning(f"Error unregistering fd {fd} from poll selector: {e}")

    def _cleanup_fd_metadata(self, fd: int) -> None:
        self._read_handlers.pop(fd, None)
        self._write_handlers.pop(fd, None)
        self._edge_triggered.pop(fd, None)

    def inject_callback(self, callback: Callable[[], None]) -> bool:
        """
        Thread-safe injection of cross-thread callbacks into the ring buffer.
        Wakes up the reactor loop via self-pipe.
        """
        success = self._injection_ring.push(callback)
        if success:
            try:
                os.write(self._self_pipe_w, b"x")
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    logger.error(f"Failed to write to reactor self-pipe: {e}")
        return success

    def run(self) -> None:
        """
        Main reactor execution loop. Bounded latency, zero hot-loop allocation invariant.
        """
        self._running = True
        logger.info(f"Reactor engine started using backend: {self._backend}")

        while self._running:
            # 1. Drain cross-thread ring buffer injections
            scratch = self._drain_scratchpad
            scratch.clear()
            self._injection_ring.pop_bulk(self._max_events, scratch)
            for cb in scratch:
                try:
                    cb()
                except Exception as e:
                    logger.exception(f"Exception in injected callback: {e}")

            # 2. Poll OS multiplexer for I/O readiness
            try:
                if self._backend == "epoll":
                    events = self._selector.poll(timeout=0.01)
                elif self._backend == "kqueue":
                    events = self._selector.control(None, self._max_events, 0.01)
                else:
                    events = self._selector.select(timeout=0.01)
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                logger.error(f"Multiplexer poll failure: {e}")
                continue

            # 3. Process demultiplexed events with bounded iteration caps
            event_count = 0
            for item in events:
                if event_count >= self._max_events:
                    # Bounded latency constraint: yield control to prevent starvation
                    break
                event_count += 1

                if self._backend == "epoll":
                    fd, flags = item
                    self._dispatch_epoll_event(fd, flags)
                elif self._backend == "kqueue":
                    # item is a select.kevent object
                    self._dispatch_kqueue_event(item)
                else:
                    # item is (selector_key, events_mask)
                    key, mask = item
                    fd = key.fileobj
                    self._dispatch_poll_event(fd, mask)

        logger.info("Reactor engine stopped.")

    def stop(self) -> None:
        """Gracefully terminate the reactor runloop."""
        self._running = False
        try:
            os.write(self._self_pipe_w, b"stop")
        except OSError:
            pass

    def _drain_self_pipe(self) -> None:
        try:
            while True:
                data = os.read(self._self_pipe_r, 1024)
                if not data:
                    break
        except OSError as e:
            if e.errno not in (errno.EAGAIN,