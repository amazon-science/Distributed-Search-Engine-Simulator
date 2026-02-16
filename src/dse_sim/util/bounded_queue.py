# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from typing import Generic, List, TypeVar

from dse_sim.collection import DSEError

T = TypeVar('T')


class BoundedQueue(Generic[T]):
    def __init__(self, maxsize: int):
        self.capacity = maxsize
        self.head = 0
        self.tail = 0

        self.final_item: T = None

        self.arr: List[T] = [None for _ in range(maxsize)]

    def __len__(self):
        return self.tail - self.head

    def peek(self) -> T:
        if self.tail > self.head:
            return self.arr[self.head % self.capacity]
        elif self.final_item is not None:
            return self.final_item
        else:
            raise EmptyQueueError(f"The {self} object is empty.")

    def pop(self):
        out = self.peek()
        self.head += 1
        return out

    @property
    def enabled(self):
        return self.final_item is None

    def push(self, obj: T):
        if not self.enabled:
            raise DisabledQueueError("Queue is disabled.")
        elif not self.full:
            self.arr[self.tail % self.capacity] = obj
            self.tail += 1
        else:
            raise FullQueueError(f"Trying to insert an item into a {self} object with {len(self)} items.")

    def push_final(self, obj: T):
        self.final_item = obj

    def __repr__(self):
        return f'BoundedQueue(capacity={self.capacity})'

    @property
    def full(self) -> bool:
        return len(self) >= self.capacity

    def __bool__(self):
        return self.tail > self.head

    def __iter__(self):
        while self:
            yield self.pop()
        if self.final_item is not None:
            yield self.final_item


class DisabledQueueError(DSEError):
    pass


class EmptyQueueError(DSEError):
    pass


class FullQueueError(DSEError):
    pass
