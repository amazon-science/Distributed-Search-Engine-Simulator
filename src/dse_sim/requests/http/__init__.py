# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import json
import os.path
import sys
import traceback
from abc import ABC, abstractmethod
from typing import Type

from dse_sim.collection import DSEError
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.collection.index import Index
from dse_sim.event.event import Event


class HTTPRequest(Event, ABC):
    repr = [('t', 'time'), 'index']

    def __init__(self, time: float, index: Index):
        super().__init__(time)
        self.collection = index.collection
        self.index = index
        self.completion_time = None
        self.start_time = None
        self.workers = []

    @abstractmethod
    async def process(self):
        pass

    async def handle(self):
        try:
            self.index.log_request(self)
            result = await self.process()
            self.index.log_result(self, result, (self.completion_time or self.collection.time) - (self.start_time or self.time))
        except DSEError as e:
            result = '5xx'
            print(f'{self.time},{e.__class__.__name__},{self.__class__.__name__},"{e}"', file=sys.stderr)
        except Exception as e:
            result = '5xx'

            tb = traceback.extract_tb(e.__traceback__)
            file_name, line_number, func_name, text = tb[-1]

            print(f'{self.time},{e.__class__.__name__}(Unexpected at {os.path.basename(file_name)}:{line_number}),{self.__class__.__name__},"{e}"', file=sys.stderr)
        return result

    @staticmethod
    @abstractmethod
    def from_json(collection: DSECollection, s: str):
        pass

    @staticmethod
    def from_json_(collection: DSECollection, s: str):
        from dse_sim.requests.http.get import GetRequest
        from dse_sim.requests.http.put import PutRequest
        from dse_sim.requests.http.search import SearchRequest

        cls: Type[HTTPRequest] = {'GET': GetRequest, 'PUT': PutRequest, 'SEARCH': SearchRequest}[json.loads(s)['type']]
        return cls.from_json(collection, s)

    @abstractmethod
    def to_json(self) -> str:
        pass
