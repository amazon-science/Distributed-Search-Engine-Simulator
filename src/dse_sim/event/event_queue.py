# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import functools
from heapq import heappop, heappush
from typing import Any, Generic, List, Protocol, TypeVar


# Define a Protocol for objects that have a .time field
class HasTime(Protocol):
    time: Any  # Could be more specific like float or datetime


# Create a bounded TypeVar that only accepts types conforming to HasTime
T = TypeVar('T', bound=HasTime)


@functools.total_ordering
class TimeWrapper(Generic[T]):
    """Wrapper class that compares objects based on their .time attribute."""

    def __init__(self, item: T):
        self.item = item

    def __lt__(self, other):
        if not isinstance(other, TimeWrapper):
            return NotImplemented
        return self.item.time < other.item.time

    def __eq__(self, other):
        if not isinstance(other, TimeWrapper):
            return NotImplemented
        return self.item.time == other.item.time


class TimedQueue(Generic[T]):
    """A priority queue that orders objects by their .time field."""

    def __init__(self):
        self._queue: List[TimeWrapper[T]] = []

    def push(self, item: T) -> None:
        """Add an item to the queue, prioritized by its .time field."""
        heappush(self._queue, TimeWrapper(item))

    def pop(self) -> T:
        """Remove and return the item with the lowest .time value."""
        if not self._queue:
            raise IndexError("pop from an empty TimedQueue")
        return heappop(self._queue).item

    def peek(self) -> T:
        """Return the item with the lowest .time value without removing it."""
        if not self._queue:
            raise IndexError("peek from an empty TimedQueue")
        return self._queue[0].item

    def __len__(self) -> int:
        return len(self._queue)

    def __bool__(self) -> bool:
        return bool(self._queue)

#
# class OldEventQueue(TimedQueue[Event]):
#     def __init__(self, start_time: float = 0.0):
#         super().__init__()
#         self._time : float = start_time
#
#     @property
#     def time(self):
#         return self._time
#
#     async def pop(self):
#         out = super().pop()
#         self._time = out.time
#         return out
#
#     @property
#     def time(self):
#         return self._time
