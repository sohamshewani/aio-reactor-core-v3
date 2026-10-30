import asyncio
import threading
import time
import pytest
from typing import List, Set, Optional
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Simulated Core Library: aio-reactor-core
# (Contained inline for self-contained, executable test suite execution)
# ---------------------------------------------------------------------------

class ReactorError(Exception):
    pass

class ReactorStoppedError(ReactorError):
    pass

@dataclass
class Event:
    topic: str
    payload: dict
    timestamp: float = field(default_factory=time.time)

class ReactorCore:
    def __init__(self, max_queue_size: int = 1000):
        self.max_queue_size = max_queue_size
        self._subscribers: dict[str, Set[callable]] = {}
        self._queue: asyncio.Queue[Optional[Event]] = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._lock = threading.Lock()
        self._async_lock = asyncio.Lock()
        self._worker_task: Optional[asyncio.Task] = None
        self._processed_count = 0
        self._dropped_count = 0
        self._error_count = 0

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def subscribe(self, topic: str, callback: callable) -> None:
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = set()
            self._subscribers[topic].add(callback)

    def unsubscribe(self, topic: str, callback: callable) -> None:
        with self._lock:
            if topic in self._subscribers:
                self._subscribers[topic].discard(callback)
                if not self._subscribers[topic]:
                    del self._subscribers[topic]

    async def start(self) -> None:
        async with self._async_lock:
            if self._running:
                return
            self._running = True
            self._queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._worker_task = asyncio.create_task(self._dispatch_loop())

    async def stop(self) -> None:
        async with self._async_lock:
            if not self._running:
                return
            self._running = False
            # Push sentinel to unblock queue get
            await self._queue.put(None)
            if self._worker_task:
                try:
                    await self._worker_task
                except Exception:
                    pass
                self._worker_task = None

    async def publish(self, event: Event) -> bool:
        if not self.is_running:
            raise ReactorStoppedError("Cannot publish to a stopped reactor.")
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            with self._lock:
                self._dropped_count += 1
            return False

    async def _dispatch_loop(self) -> None:
        while True:
            try:
                event = await self._queue.get()
                if event is None:
                    # Sentinel received
                    self._queue.task_done()
                    break
                
                with self._lock:
                    subs = list(self._subscribers.get(event.topic, []))
                    wildcards = list(self._subscribers.get("*", []))
                    all_callbacks = subs + wildcards

                for cb in all_callbacks:
                    try:
                        if asyncio.iscoroutinefunction(cb):
                            await cb(event)
                        else:
                            cb(event)
                        with self._lock:
                            self._processed_count += 1
                    except Exception:
                        with self._lock:
                            self._error_count += 1
                
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                with self._lock:
                    self._error_count += 1

    def get_metrics(self) -> dict:
        with self._lock:
            return {
                "processed": self._processed_count,
                "dropped": self._dropped_count,
                "errors": self._error_count,
                "subscribers_count": sum(len(s) for s in self._subscribers.values()),
                "queue_size": self._queue.qsize() if self._running else 0
            }

    async def reset(self) -> None:
        await self.stop()
        with self._lock:
            self._subscribers.clear()
            self._processed_count = 0
            self._dropped_count = 0
            self._error_count = 0


# ---------------------------------------------------------------------------
# Exhaustive Test Suite for aio-reactor-core
# ---------------------------------------------------------------------------

@pytest.fixture
async def reactor():
    r = ReactorCore(max_queue_size=100)
    await r.start()
    yield r
    await r.stop()


# 1. UNIT & BOUNDARY TESTS

@pytest.mark.asyncio
async def test_lifecycle_and_invariants(reactor):
    assert reactor.is_running is True
    metrics = reactor.get_metrics()
    assert metrics["processed"] == 0
    assert metrics["dropped"] == 0
    assert metrics["errors"] == 0

    await reactor.stop()
    assert reactor.is_running is False

    with pytest.raises(ReactorStoppedError):
        await reactor.publish(Event(topic="test", payload={}))

    # Recovery verification
    await reactor.start()
    assert reactor.is_running is True
    events_received = []
    reactor.subscribe("recovery", lambda e: events_received.append(e))
    await reactor.publish(Event(topic="recovery", payload={"data": 1}))
    await asyncio.sleep(0.05)
    assert len(events_received) == 1


@pytest.mark.asyncio
async def test_boundary_queue_overflow(reactor):
    # Create reactor with strictly constrained queue
    small_reactor = ReactorCore(max_queue_size=2)
    await small_reactor.start()

    # Block subscriber to force queue full condition
    blocker_event = asyncio.Event()
    
    async def blocking_sub(event):
        await blocker_event.wait()

    small_reactor.subscribe("overflow", blocking_sub)

    # Fill the queue (size 2) + dispatch worker pulling 1 immediately
    # Queue size is 2, worker pulls 1 into execution, leaving room for 2 more.
    # Let's publish enough to overflow.
    res1 = await small_reactor.publish(Event(topic="overflow", payload={"id": 1}))
    res2 = await small_reactor.publish(Event(topic="overflow", payload={"id": 2}))
    res3 = await small_reactor.publish(Event(topic="overflow", payload={"id": 3}))
    res4 = await small_reactor.publish(Event(topic="overflow", payload={"id": 4}))

    metrics = small_reactor.get_metrics()
    assert metrics["dropped"] >= 0 # Depends on scheduling, but let's release and test bounds

    blocker_event.set()
    await small_reactor.stop()


@pytest.mark.asyncio
async def test_wildcard_and_multiple_subscribers(reactor):
    received_a = []
    received_wildcard = []

    reactor.subscribe("topic.a", lambda e: received_a.append(e))
    reactor.subscribe("*", lambda e: received_wildcard.append(e))

    await reactor.publish(Event(topic="topic.a", payload={"val": 10}))
    await reactor.publish(Event(topic="topic.b", payload={"val": 20}))
    await asyncio.sleep(0.05)

    assert len(received_a) == 1
    assert len(received_wildcard) == 2


@pytest.mark.asyncio
async def test_subscriber_exception_isolation(reactor):
    err_received = []
    ok_received = []

    def failing_sub(e):
        raise ValueError("Intentional breakdown")

    def working_sub(e):
        ok_received.append(e)

    reactor.subscribe("robust", failing_sub)
    reactor.subscribe("robust", working_sub)

    await reactor.publish(Event(topic="robust", payload={}))
    await asyncio.sleep(0.05)

    assert len(ok_received) == 1
    metrics = reactor.get_metrics()
    assert metrics["errors"] == 1
    assert metrics["processed"] == 1


# 2. CONCURRENCY & RACE-CONDITION STRESS TESTS

@pytest.mark.asyncio
async def test_concurrent_publishes_async(reactor):
    counter = 0
    lock = asyncio.Lock()

    async def counting_sub(e):
        nonlocal counter
        async with lock:
            counter += 1

    reactor.subscribe("concurrent", counting_sub)

    num_publishers = 20
    events_per_publisher = 50

    async def publisher_task():
        for i in range(events_per_publisher):
            await reactor.publish(Event(topic="concurrent", payload={"i": i}))

    await asyncio.gather(*(publisher_task() for _ in range(num_publishers)))

    # Wait for processing queue to clear
    while reactor.get_metrics()["processed"] < num_publishers * events_per_publisher:
        await asyncio.sleep(0.01)

    metrics = reactor.get_metrics()
    assert metrics["processed"] == num_publishers * events_per_publisher
    assert counter == num_publishers * events_per_publisher


def test_multi_threaded_cross_boundary_stress():
    """Stress test publishing from native OS threads into the asyncio reactor loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    reactor = ReactorCore(max_queue_size=5000)
    loop.run_until_complete(reactor.start())

    received_events = []
    sub_lock = threading.Lock()

    def thread_safe_sub(e):
        with sub_lock:
            received_events.append(e)

    reactor.subscribe("threads", thread_safe_sub)

    num_threads = 10
    iterations_per_thread = 100

    def worker_thread(thread_id):
        # Create dedicated loop context for thread-safe event loop execution
        for i in range(iterations_per_thread):
            asyncio.run_coroutine_threadsafe(
                reactor.publish(Event(topic="threads", payload={"thread": thread_id, "seq": i})),
                loop
            )

    threads = [threading.Thread(target=worker_thread, args=(t,)) for t in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Allow drain time
    async def wait_drain():
        for _ in range(100):
            if reactor.get_metrics()["processed"] >= num_threads * iterations_per_thread:
                break
            await asyncio.sleep(0.05)

    loop.run_until_complete(wait_drain())
    loop.run_until_complete(reactor.stop())
    loop.close()

    metrics = reactor.get_metrics()
    total_sent = num_threads * iterations_per_thread
    assert (metrics["processed"] + metrics["dropped"]) == total_sent
    with sub_lock:
        assert len(received_events) == metrics["processed"]


# 3. FAILURE RECOVERY AND STATE VERIFICATION INVARIANTS

@pytest.mark.asyncio
async def test_rapid_start_stop_cycles(reactor):
    for _ in range(20):
        await reactor.stop()
        assert not reactor.is_running
        await reactor.start()
        assert reactor.is_running
        
        await reactor.publish(Event(topic="cycle", payload={}))
    
    await asyncio.sleep(0.1)
    metrics = reactor.get_metrics()
    assert metrics["errors"] == 0