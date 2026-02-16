# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from collections import defaultdict
from typing import Any, Callable, Iterable, Tuple, Union


class Counted(object):
    _counter = defaultdict(int)
    id: int
    repr: Iterable[Union[str, Tuple[str, str], Tuple[str, Callable[[Any], str]]]]

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        Counted._counter[cls] += 1
        instance.id = Counted._counter[cls]
        return instance

    # noinspection PyCallingNonCallable
    def __repr__(self):
        return f'{self.__class__.__name__}({self.id})'
        # repr0 = self.repr if hasattr(self, 'repr') else [(None, 'id')]
        # parts = []
        # for k in repr0:
        #     if isinstance(k, str):
        #         v = getattr(self, k)
        #     elif isinstance(k[1], str):
        #         k, v = k[0], getattr(self, k[1])
        #     else:
        #         k, v = k[0], k[1](self)
        #
        #     if isinstance(v, float):
        #             v = f'{v:.4f}'
        #     parts.append(f'{k}={v}' if k else f'{v}')
        #
        # return f'{self.__class__.__name__}({",".join(parts)})'
