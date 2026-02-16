# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from typing import Dict, Union


class Profilable(metaclass=ABCMeta):
    _last_profiled = defaultdict(float)

    @abstractmethod
    def run_profiler(self) -> Union[float, Dict[str, float]]:
        pass

    def profile(self, time: float) -> Dict[str, float]:
        out = self.run_profiler()

        if isinstance(out, float):
            out = {'value': out}

        Profilable._last_profiled[self] = time
        return out

    @property
    def last_profiled(self):
        return Profilable._last_profiled[self]
