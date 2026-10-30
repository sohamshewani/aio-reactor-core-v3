# aio_reactor/timers.py

import time
from typing import Callable, Optional, Any

class TimerHandle:
    """
    Represents an active timer instance within the Hierarchical Timer Wheel.
    Maintains internal pointers for $O(1)$ unlinking and cancellation.
    """
    __slots__ = (
        '_callback',
        '_args',
        '_kwargs',
        '_expiration',
        '_period',
        '_wheel_index',
        '_bucket_index',
        '_cancelled',
        '_next',
        '_prev',
    )

    def __init__(
        self,
        callback: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        expiration: float,
        period: float
    ) -> None:
        self._callback: Callable[..., Any] = callback
        self._args: tuple = args
        self._kwargs: dict = kwargs
        self._expiration: float = expiration
        self._period: float = period
        self._wheel_index: int = -1
        self._bucket_index: int = -1
        self._cancelled: bool = False
        self._next: Optional['TimerHandle'] = None
        self._prev: Optional['TimerHandle'] = None

    def cancel(self) -> None:
        """
        Cancels the timer handle, marking it for lazy or immediate eviction.
        """
        self._cancelled = True

    def is_cancelled(self) -> bool:
        """
        Returns whether the timer handle has been cancelled.
        """
        return self._cancelled

    def _reset_links(self) -> None:
        """
        Resets internal doubly-linked list pointers.
        """
        self._next = None
        self._prev = None
        self._wheel_index = -1
        self._bucket_index = -1


class TimerWheel:
    """
    Hierarchical Timer Wheel: Multi-level power-of-two slotted timing buckets
    providing O(1) amortized insertion, deletion, and tick execution for
    hundreds of thousands of concurrent timeout handles.
    """

    # Wheel configuration parameters
    # 5 wheels with resolutions/capacities matching power-of-two slot sizing
    RESOLUTIONS = (0.001, 0.064, 4.096, 262.144, 16777.216)
    SLOTS_PER_WHEEL = 64
    NUM_WHEELS = 5

    def __init__(self) -> None:
        # Initialize buckets for each wheel level: list of lists of TimerHandle (head pointers)
        self._wheels: list[list[Optional[TimerHandle]]] = [
            [None for _ in range(self.SLOTS_PER_WHEEL)]
            for _ in range(self.NUM_WHEELS)
        ]
        
        # Track intervals and shift factors for bitwise/arithmetic wheel cascading
        self._intervals = [self.RESOLUTIONS[i] for i in range(self.NUM_WHEELS)]
        
        # Current monotonic time reference point for the reactor tick
        self._current_time: float = time.monotonic()
        
        # Wheel tick counters tracking cumulative ticks per level
        self._wheel_ticks = [0] * self.NUM_WHEELS

    def schedule(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        period: float = 0.0,
        **kwargs: Any
    ) -> TimerHandle:
        """
        Schedules a callback to be executed after a specified delay.
        Supports periodic execution if `period` is greater than zero.
        Provides O(1) amortized insertion.
        """
        if delay < 0.0:
            raise ValueError("Timer delay must be non-negative.")
        if period < 0.0:
            raise ValueError("Timer period must be non-negative.")

        expiration = self._current_time + delay
        handle = TimerHandle(callback, args, kwargs, expiration, period)
        self._insert(handle)
        return handle

    def cancel(self, handle: TimerHandle) -> None:
        """
        Removes an active timer handle from the wheel in O(1) time.
        """
        if handle._cancelled:
            return
        
        handle.cancel()
        
        # If the handle is currently linked in a bucket, remove it
        if handle._wheel_index != -1 and handle._bucket_index != -1:
            self._remove_from_bucket(handle)

    def tick(self, current_time: Optional[float] = current_time) -> list[TimerHandle]:
        """
        Advances the timer wheel relative to the current monotonic time.
        Executes and cascades expired timers, returning a list of ready handles
        to be dispatched by the reactor core.
        """
        if current_time is None:
            now = time.monotonic()
        else:
            now = current_time

        ready_handles: list[TimerHandle] = []

        # Calculate time delta since last tick execution
        elapsed = now - self._current_time
        if elapsed <= 0.0:
            return ready_handles

        self._current_time = now

        # Determine how many ticks occurred on the base wheel
        base_resolution = self.RESOLUTIONS[0]
        ticks_to_advance = int(elapsed / base_resolution)
        if ticks_to_advance <= 0:
            # Even if full tick hasn't passed, check exact bucket for current time
            self._harvest_bucket(0, self._wheel_ticks[0] % self.SLOTS_PER_WHEEL, ready_handles)
            return ready_handles

        # Advance wheels progressively with cascading
        for i in range(self.NUM_WHEELS):
            if ticks_to_advance <= 0:
                break

            steps = ticks_to_advance % self.SLOTS_PER_WHEEL
            ticks_to_advance = ticks_to_advance // self.SLOTS_PER_WHEEL

            for _ in range(steps):
                current_slot = self._wheel_ticks[i] % self.SLOTS_PER_WHEEL
                self._wheel_ticks[i] += 1

                # Harvest expired timers in this slot
                self._harvest_bucket(i, current_slot, ready_handles)

                # If we rolled over a wheel, cascade down from the next higher wheel
                if current_slot == self.SLOTS_PER_WHEEL - 1 and i + 1 < self.NUM_WHEELS:
                    self._cascade(i + 1)

        return ready_handles

    def _insert(self, handle: TimerHandle) -> None:
        """
        Internal method to place a timer handle into the appropriate hierarchical bucket.
        """
        delta = handle._expiration - self._current_time
        if delta < 0.0:
            delta = 0.0

        # Select appropriate wheel level based on time delta range
        wheel_idx = 0
        for i in range(self.NUM_WHEELS - 1):
            max_range = self.RESOLUTIONS[i] * self.SLOTS_PER_WHEEL
            if delta >= max_range and i + 1 < self.NUM_WHEELS:
                # Check if next wheel is a better fit
                # If delta exceeds current tier's capacity, evaluate higher tiers
                pass
            
            # Find the highest wheel level where delta fits within the wheel span
            span = self.RESOLUTIONS[i] * self.SLOTS_PER_WHEEL
            if delta < span or i == self.NUM_WHEELS - 1:
                wheel_idx = i
                break
            else:
                wheel_idx = i + 1

        # Calculate slot index within the selected wheel
        resolution = self.RESOLUTIONS[wheel_idx]
        ticks_from_now = int(delta / resolution)
        slot_idx = (self._wheel_ticks[wheel_idx] + ticks_from_now) % self.SLOTS_PER_WHEEL

        # Assign locations and link into bucket
        handle._wheel_index = wheel_idx
        handle._bucket_index = slot_idx
        self._add_to_bucket(handle, wheel_idx, slot_idx)

    def _add_to_bucket(self, handle: TimerHandle, wheel_idx: int, slot_idx: int) -> None:
        """
        Prepends a timer handle to the doubly-linked list of a specific bucket.
        """
        head = self._wheels[wheel_idx][slot_idx]
        handle._next = head
        handle._prev = None
        if head is not None:
            head._prev = handle
        self._wheels[wheel_idx][slot_idx] = handle

    def _remove_from_bucket(self, handle: TimerHandle) -> None:
        """
        Removes a timer handle from its current bucket in O(1) time.
        """
        wheel_idx = handle._wheel_index
        slot_idx = handle._bucket_index

        if wheel_idx == -1 or slot_idx == -1:
            return

        if handle._prev is not None:
            handle._prev._next = handle._next
        else:
            # Handle was the head of the bucket list
            self._wheels[wheel_idx][slot_idx] = handle._next

        if handle._next is not None:
            handle._next._prev = handle._prev

        handle._reset_links()

    def _harvest_bucket(self, wheel_idx: int, slot_idx: int, ready_handles: list[TimerHandle]) -> None:
        """
        Empties a wheel slot bucket, processing valid handles and re-inserting
        or rescheduling periodic timers as required.
        """
        current = self._wheels[wheel_idx][slot_idx]
        self._wheels[wheel_idx][slot_idx] = None

        while current is not None:
            nxt = current._next
            current._reset_links()

            if current._cancelled:
                current = nxt
                continue

            if current._expiration <= self._current_time:
                ready_handles.append(current)
                # Handle periodic rescheduling
                if current._period > 0.0:
                    current._expiration = self._current_time + current._period
                    current._cancelled = False
                    self._insert(current)
            else:
                # Timer expired early due to wheel granularity precision shifts; re-insert
                self._insert(current)

            current = nxt

    def _cascade(self, wheel_idx: int) -> None:
        """
        Cascades timers from a higher-resolution wheel down to lower-resolution wheels
        when a higher wheel completes a full rotation.
        """
        current_slot = self._wheel_ticks[wheel_idx] % self.SLOTS_PER_WHEEL
        self._wheel_ticks[wheel_idx] += 1

        bucket = self._wheels[wheel_idx][current_slot]
        self._wheels[wheel_idx][current_slot] = None

        current = bucket
        while current is not None:
            nxt = current._next
            current._reset_links()

            if not current._cancelled:
                # Re-insert handle into the appropriate lower-level wheel/bucket
                self._insert(current)

            current = nxt

        # If higher wheel also wrapped around, cascade recursively to the next level
        if current_slot == self.SLOTS_PER_WHEEL - 1 and wheel_idx + 1 < self.NUM_WHEELS:
            self._cascade(wheel_idx + 1)