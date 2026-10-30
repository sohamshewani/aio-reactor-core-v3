"""
aio_reactor/allocator.py

Arena Allocator implementation providing fixed-size pre-allocated byte blocks
utilizing free-lists with O(1) allocation and deallocation to eliminate Python
GC pauses under high-throughput event looping.
"""

from typing import Optional, List, Tuple
import sys


class AllocatorError(Exception):
    """Base exception for allocator failures."""
    pass


class OutOfMemoryError(AllocatorError):
    """Raised when the arena is exhausted and cannot allocate more blocks."""
    pass


class InvalidPointerError(AllocatorError):
    """Raised when an invalid memory address or block is passed for deallocation."""
    pass


class ArenaAllocator:
    """
    Fixed-size arena allocator managing pre-allocated memory blocks via a 
    free-list data structure. Ensures O(1) time complexity for both allocation 
    and deallocation operations without triggering Python garbage collection 
    pauses during steady-state reactor operations.
    """

    __slots__ = (
        '_block_size',
        '_block_count',
        '_alignment',
        '_buffer',
        '_free_head',
        '_next_free',
        '_allocated_count',
        '_peak_allocated',
        '_block_indices'
    )

    def __init__(self, block_size: int = 4096, block_count: int = 1024, alignment: int = 8) -> None:
        """
        Initializes the arena allocator with a pre-allocated bytearray buffer.

        :param block_size: Size of each individual memory block in bytes.
        :param block_count: Total number of blocks pre-allocated in the arena.
        :param alignment: Memory alignment boundary (must be a power of 2).
        """
        if block_size <= 0:
            raise ValueError("block_size must be greater than zero.")
        if block_count <= 0:
            raise ValueError("block_count must be greater than zero.")
        if alignment <= 0 or (alignment & (alignment - 1)) != 0:
            raise ValueError("alignment must be a positive power of 2.")

        # Ensure block size is aligned to the requested alignment boundary
        remainder = block_size % alignment
        if remainder != 0:
            block_size += (alignment - remainder)

        self._block_size: int = block_size
        self._block_count: int = block_count
        self._alignment: int = alignment

        # Total memory pool size
        total_size = self._block_size * self._block_count
        
        # Pre-allocate contiguous memory via bytearray to eliminate dynamic allocations
        self._buffer: bytearray = bytearray(total_size)

        # Free list head index (points to the first available block index)
        self._free_head: int = 0

        # Array tracking the next free block index for each block position.
        # Acts as an embedded singly linked list within the metadata array.
        # Index -1 denotes the end of the free list.
        self._next_free: List[int] = [i + 1 for i in range(block_count)]
        if self._next_free:
            self._next_free[-1] = -1

        # Fast lookup mapping for memory view safety and inverse index validation
        # Stores the byte offset for each block index
        self._block_indices: List[int] = [i * self._block_size for i in range(block_count)]

        # Metrics and safety monitors
        self._allocated_count: int = 0
        self._peak_allocated: int = 0

    @property
    def block_size(self) -> int:
        """Returns the size of an individual allocation block."""
        return self._block_size

    @property
    def block_count(self) -> int:
        """Returns the total capacity of blocks in the arena."""
        return self._block_count

    @property
    def allocated_count(self) -> int:
        """Returns the current number of actively allocated blocks."""
        return self._allocated_count

    @property
    def peak_allocated(self) -> int:
        """Returns the peak water mark of allocated blocks."""
        return self._peak_allocated

    @property
    def capacity_bytes(self) -> int:
        """Returns the total byte size of the pre-allocated arena buffer."""
        return len(self._buffer)

    def allocate(self) -> memoryview:
        """
        Allocates a fixed-size block from the arena in O(1) time.

        :returns: A zero-copy memoryview representing the allocated block.
        :raises OutOfMemoryError: If the arena has exhausted all free blocks.
        """
        if self._free_head == -1:
            raise OutOfMemoryError(
                f"Arena allocator exhausted: block_size={self._block_size}, "
                f"block_count={self._block_count}, allocated={self._allocated_count}"
            )

        # Pop the head of the free list
        block_index = self._free_head
        self._free_head = self._next_free[block_index]

        # Mark block as allocated by setting its next pointer to an invalid sentinel (-2)
        self._next_free[block_index] = -2

        # Update metrics
        self._allocated_count += 1
        if self._allocated_count > self._peak_allocated:
            self._peak_allocated = self._allocated_count

        # Compute byte slice boundaries
        offset = self._block_indices[block_index]
        
        # Return a zero-copy slicing of the pre-allocated bytearray buffer
        return memoryview(self._buffer)[offset:offset + self._block_size]

    def deallocate(self, block: memoryview) -> None:
        """
        Deallocates a previously allocated memoryview block in O(1) time,
        returning it to the head of the free-list.

        :param block: The memoryview returned by a prior allocate() call.
        :raises InvalidPointerError: If the block does not belong to this arena
                                     or is already freed.
        """
        if not isinstance(block, memoryview):
            raise InvalidPointerError("Deallocation target must be a valid memoryview instance.")

        # Extract address info and validate ownership via buffer protocol inspection
        try:
            # c_contiguous or general memoryview offset extraction
            obj = block.obj
            if obj is not self._buffer:
                raise InvalidPointerError("Memoryview does not reference this arena's underlying buffer.")
            
            # Retrieve the start offset of the memoryview
            # memoryview.c_contiguous and format safety check
            if block.format != 'B':
                # Re-interpret format safely or cast
                pass
            
            # Calculate byte offset within the buffer
            # Using memoryview pointer comparison via casting or slice introspection
            # In Python, we can check the release status and offset using cast or slice start
            # Safest reliable offset derivation for bytearray-backed memoryviews:
            offset = block.offset
        except (AttributeError, ValueError, TypeError) as e:
            raise InvalidPointerError(f"Failed to resolve memoryview parameters: {e}") from e

        if offset < 0 or offset >= len(self._buffer) or (offset % self._block_size != 0):
            raise InvalidPointerError(f"Memoryview offset {offset} is misaligned or out of bounds.")

        block_index = offset // self._block_size

        # Verify that the block is actually in an allocated state (next_free == -2)
        if self._next_free[block_index] != -2:
            raise InvalidPointerError(
                f"Double free or corruption detected: block index {block_index} "
                f"at offset {offset} is not in an allocated state."
            )

        # Push the block back onto the free-list head
        self._next_free[block_index] = self._free_head
        self._free_head = block_index

        # Decrement allocated count
        self._allocated_count -= 1

    def reset(self) -> None:
        """
        Resets the entire arena allocator state in O(N) time (where N is block_count),
        reclaiming all allocated blocks instantly without releasing the underlying memory buffer.
        """
        self._free_head = 0
        for i in range(self._block_count - 1):
            self._next_free[i] = i + 1
        if self._block_count > 0:
            self._next_free[self._block_count - 1] = -1

        self._allocated_count = 0

    def contains(self, block: memoryview) -> bool:
        """
        Validates whether a given memoryview resides within this arena's address space.

        :param block: The memoryview to check.
        :returns: True if the memoryview belongs to this arena, False otherwise.
        """
        try:
            if not isinstance(block, memoryview):
                return False
            if block.obj is not self._buffer:
                return False
            offset = block.offset
            if offset < 0 or offset >= len(self._buffer):
                return False
            if offset % self._block_size != 0:
                return False
            block_index = offset // self._block_size
            # If next_free is -2, it's currently allocated; if >= -1, it's validly tracked
            return 0 <= block_index < self._block_count
        except Exception:
            return False

    def __repr__(self) -> str:
        return (
            f"<ArenaAllocator block_size={self._block_size}, "
            f"block_count={self._block_count}, "
            f"allocated={self._allocated_count}, "
            f"peak_allocated={self._peak_allocated}, "
            f"capacity_bytes={self.capacity_bytes}>"
        )