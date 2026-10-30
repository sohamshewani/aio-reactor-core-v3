import os
import sys
import errno
import socket
import select
import logging
import threading
from typing import Dict, Optional, Tuple, Callable

logger = logging.getLogger("aio_reactor.multiplexer")

# Constants for connection states and event bitmasks
STATE_FREE = 0
STATE_READ = 1
STATE_WRITE = 2
STATE_READWRITE = 3
STATE_DEAD = 4

EVENT_READ = 0x01
EVENT_WRITE = 0x02
EVENT_ERROR = 0x04

class ConnectionSlot:
    __slots__ = (
        'fd',
        'sock',
        'state',
        'ref_count',
        'read_buffer',
        'write_buffer',
        'read_callback',
        'write_callback',
        'error_callback',
        'edge_triggered',
        'lock'
    )

    def __init__(self, fd: int) -> None:
        self.fd: int = fd
        self.sock: Optional[socket.socket] = None
        self.state: int = STATE_FREE
        self.ref_count: int = 0
        self.read_buffer: Optional[bytearray] = None
        self.write_buffer: Optional[bytearray] = None
        self.read_callback: Optional[Callable[[int], None]] = None
        self.write_callback: Optional[Callable[[int], None]] = None
        self.error_callback: Optional[Callable[[int, Exception], None]] = None
        self.edge_triggered: bool = False
        self.lock: threading.Lock = threading.Lock()

    def reset(self) -> None:
        with self.lock:
            self.sock = None
            self.state = STATE_FREE
            self.ref_count = 0
            self.read_buffer = None
            self.write_buffer = None
            self.read_callback = None
            self.write_callback = None
            self.error_callback = None
            self.edge_triggered = False


class Multiplexer:
    def __init__(self, max_fds: int = 65536, buffer_size: int = 65536) -> None:
        self.max_fds: int = max_fds
        self.buffer_size: int = buffer_size
        self._slots: Dict[int, ConnectionSlot] = {}
        self._slots_lock: threading.Lock = threading.Lock()
        
        self.use_epoll: bool = hasattr(select, "epoll")
        self.use_kqueue: bool = hasattr(select, "kqueue")

        if self.use_epoll:
            self._epoll = select.epoll()
            self._kqueue = None
        elif self.use_kqueue:
            self._kqueue = select.kqueue()
            self._epoll = None
        else:
            raise RuntimeError("Neither epoll nor kqueue is available on this platform.")

        logger.info(f"Multiplexer initialized using backend: {'epoll' if self.use_epoll else 'kqueue'}")

    def _get_or_create_slot(self, fd: int) -> ConnectionSlot:
        with self._slots_lock:
            slot = self._slots.get(fd)
            if slot is None:
                if len(self._slots) >= self.max_fds:
                    raise RuntimeError("Maximum file descriptor capacity reached in Multiplexer.")
                slot = ConnectionSlot(fd)
                self._slots[fd] = slot
            return slot

    def register(self, sock: socket.socket, 
                 read_cb: Optional[Callable[[int], None]] = None,
                 write_cb: Optional[Callable[[int], None]] = None,
                 error_cb: Optional[Callable[[int, Exception], None]] = None,
                 edge_triggered: bool = False) -> int:
        fd = sock.fileno()
        if fd < 0:
            raise ValueError(f"Invalid file descriptor: {fd}")

        try:
            sock.setblocking(False)
        except OSError as e:
            raise RuntimeError(f"Failed to set socket non-blocking for fd {fd}: {e}")

        slot = self._get_or_create_slot(fd)

        with slot.lock:
            if slot.state != STATE_FREE and slot.state != STATE_DEAD:
                raise KeyError(f"File descriptor {fd} is already registered and active.")

            slot.sock = sock
            slot.ref_count += 1
            slot.read_buffer = bytearray(self.buffer_size)
            slot.write_buffer = bytearray(self.buffer_size)
            slot.read_callback = read_cb
            slot.write_callback = write_cb
            slot.error_callback = error_cb
            slot.edge_triggered = edge_triggered
            
            has_read = read_cb is not None
            has_write = write_cb is not None

            if has_read and has_write:
                slot.state = STATE_READWRITE
            elif has_read:
                slot.state = STATE_READ
            elif has_write:
                slot.state = STATE_WRITE
            else:
                slot.state = STATE_READ

        try:
            if self.use_epoll:
                mask = select.EPOLLIN | select.EPOLLPRI | select.EPOLLERR | select.EPOLLHUP
                if has_write:
                    mask |= select.EPOLLOUT
                if edge_triggered:
                    mask |= select.EPOLLET
                self._epoll.register(fd, mask)
            elif self.use_kqueue:
                events = []
                if has_read:
                    events.append(select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE))
                if has_write:
                    events.append(select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD | select.KQ_EV_ENABLE))
                if events:
                    self._kqueue.control(events, 0, 0)
        except Exception as e:
            slot.reset()
            with self._slots_lock:
                if fd in self._slots:
                    del self._slots[fd]
            raise RuntimeError(f"Failed to register fd {fd} with OS multiplexer: {e}")

        logger.debug(f"Registered fd {fd} with state {slot.state}, edge_triggered={edge_triggered}")
        return fd

    def modify(self, fd: int, 
               enable_read: Optional[bool] = None, 
               enable_write: Optional[bool] = None) -> None:
        slot = self._slots.get(fd)
        if slot is None:
            raise KeyError(f"File descriptor {fd} not found in multiplexer.")

        with slot.lock:
            if slot.state == STATE_FREE or slot.state == STATE_DEAD:
                raise RuntimeError(f"Cannot modify dead or free slot for fd {fd}")

            if enable_read is not None:
                if enable_read:
                    slot.state |= STATE_READ
                else:
                    slot.state &= ~STATE_READ
            if enable_write is not None:
                if enable_write:
                    slot.state |= STATE_WRITE
                else:
                    slot.state &= ~STATE_WRITE

            has_read = bool(slot.state & STATE_READ)
            has_write = bool(slot.state & STATE_WRITE)
            edge_triggered = slot.edge_triggered

        try:
            if self.use_epoll:
                mask = select.EPOLLERR | select.EPOLLHUP | select.EPOLLPRI
                if has_read:
                    mask |= select.EPOLLIN
                if has_write:
                    mask |= select.EPOLLOUT
                if edge_triggered:
                    mask |= select.EPOLLET
                self._epoll.modify(fd, mask)
            elif self.use_kqueue:
                events = [
                    select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE if has_read else select.KQ_EV_DELETE),
                    select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD | select.KQ_EV_ENABLE if has_write else select.KQ_EV_DELETE)
                ]
                self._kqueue.control(events, 0, 0)
        except Exception as e:
            logger.error(f"Failed to modify fd {fd} in OS multiplexer: {e}")
            raise RuntimeError(f"Failed to modify fd {fd}: {e}")

        logger.debug(f"Modified fd {fd}: state now {slot.state}")

    def unregister(self, fd: int) -> None:
        with self._slots_lock:
            slot = self._slots.get(fd)
            if slot is None:
                return

        with slot.lock:
            if slot.state == STATE_FREE or slot.state == STATE_DEAD:
                return
            slot.state = STATE_DEAD
            slot.ref_count -= 1
            should_close = (slot.ref_count <= 0)

        try:
            if self.use_epoll:
                self._epoll.unregister(fd)
            elif self.use_kqueue:
                events = [
                    select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_DELETE),
                    select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_DELETE)
                ]
                try:
                    self._kqueue.control(events, 0, 0)
                except OSError:
                    pass
        except Exception as e:
            logger.warning(f"Error unregistering fd {fd} from OS multiplexer: {e}")

        sock_to_close = None
        if should_close:
            with slot.lock:
                sock_to_close = slot.sock
                slot.reset()
            with self._slots_lock:
                if fd in self._slots:
                    del self._slots[fd]
            if sock_to_close is not None:
                try:
                    sock_to_close.close()
                except Exception as e:
                    logger.warning(f"Error closing socket for fd {fd}: {e}")

        logger.debug(f"Unregistered and drained fd {fd}")

    def poll(self, timeout: float) -> list[Tuple[int, int]]:
        triggered_events = []
        try:
            if self.use_epoll:
                events = self._epoll.poll(timeout)
                for fd, flags in events:
                    ev_mask = 0
                    if flags & (select.EPOLLIN | select.EPOLLPRI | select.EPOLLHUP):
                        ev_mask |= EVENT_READ
                    if flags & select.EPOLLOUT:
                        ev_mask |= EVENT_WRITE
                    if flags & (select.EPOLLERR | select.EPOLLHUP):
                        ev_mask |= EVENT_ERROR
                    triggered_events.append((fd, ev_mask))
            elif self.use_kqueue:
                kevents = self._kqueue.control(None, 1024, timeout)
                fd_map: Dict[int, int] = {}
                for kev in kevents:
                    fd = kev.ident
                    filter_type = kev.filter
                    flags = kev.flags
                    
                    if fd not in fd_map:
                        fd_map[fd] = 0
                    
                    if filter_type == select.KQ_FILTER_READ:
                        fd_map[fd] |= EVENT_READ
                    elif filter_type == select.KQ_FILTER_WRITE:
                        fd_map[fd] |= EVENT_WRITE
                    
                    if flags & select.KQ_EV_EOF:
                        fd_map[fd] |= EVENT_ERROR

                for fd, ev_mask in fd_map.items():
                    triggered_events.append((fd, ev_mask))
        except InterruptedError:
            pass
        except Exception as e:
            logger.error(f"Error during poll execution: {e}")

        return triggered_events

    def dispatch(self, fd: int, event_mask: int) -> None:
        slot = self._slots.get(fd)
        if slot is None:
            return

        with slot.lock:
            if slot.state == STATE_FREE or slot.state == STATE_DEAD:
                return
            slot.ref_count += 1

        try:
            if event_mask & EVENT_ERROR:
                err_exc = ConnectionResetError(errno.ECONNRESET, "Connection reset or error flag raised by OS multiplexer")
                with slot.lock:
                    cb = slot.error_callback
                if cb is not None:
                    try:
                        cb(fd, err_exc)
                    except Exception as exc:
                        logger.exception(f"Error in error_callback for fd {fd}: {exc}")
                self.unregister(fd)
                return

            if event_mask & EVENT_READ:
                with slot.lock:
                    rcb = slot.read_callback
                if rcb is not None:
                    try:
                        rcb(fd)
                    except BlockingIOError:
                        if slot.edge_triggered:
                            logger.debug(f"EAGAIN encountered on edge-triggered read for fd {fd}, degrading gracefully.")
                    except (ConnectionResetError, BrokenPipeError, OSError) as e:
                        with slot.lock:
                            ecb = slot.error_callback
                        if ecb is not None:
                            try:
                                ecb(fd, e)
                            except Exception as exc:
                                logger.exception(f"Error in error_callback for fd {fd}: {exc}")
                        self.unregister(fd)
                        return
                    except Exception as exc:
                        logger.exception(f"Unhandled exception in read callback for fd {fd}: {exc}")

            if event_mask & EVENT_WRITE:
                with slot.lock:
                    wcb = slot.write_callback
                if wcb is not None:
                    try:
                        wcb(fd)
                    except BlockingIOError:
                        if slot.edge_triggered:
                            logger.debug(f"EAGAIN encountered on edge-triggered write for fd {fd}, degrading gracefully.")
                    except (ConnectionResetError, BrokenPipeError, OSError) as e:
                        with slot.lock:
                            ecb = slot.error_callback
                        if ecb is not None:
                            try:
                                ecb(fd, e)
                            except Exception as exc:
                                logger.exception(f"Error in error_callback for fd {fd}: {exc}")
                        self.unregister(fd)
                        return
                    except Exception as exc:
                        logger.exception(f"Unhandled exception in write callback for fd {fd}: {exc}")

        finally:
            with slot.lock:
                slot.ref_count -= 1
                should_drain = (slot.ref_count <= 0 and slot.state == STATE_DEAD)
            
            if should_drain:
                self.unregister(fd)

    def get_slot(self, fd: int) -> Optional[ConnectionSlot]:
        return self._slots.get(fd)

    def close(self) -> None:
        with self._slots_lock:
            fds = list(self._slots.keys())
        for fd in fds:
            self.unregister(fd)

        if self._epoll is not None:
            try:
                self._epoll.close()
            except Exception:
                pass
        if self._kqueue is not None:
            try:
                self._kqueue.close()
            except Exception:
                pass
        logger.info("Multiplexer closed and resources successfully reclaimed.")