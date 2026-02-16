# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from abc import ABCMeta

from dse_sim.collection.dse_collection import DSECollection
from dse_sim.config import DEBUG
from dse_sim.profiler.profilable import Profilable
from dse_sim.util.counted import Counted


class Actor(Counted, metaclass=ABCMeta):
    def __init__(self, collection: DSECollection):
        self.collection = collection
        self.collection.register(self)

    # def __repr__(self) -> str:
    #     return f'{self.__class__.__name__}({self.id})'

    def delete(self):
        self.tear_down()
        self.collection.unregsiter(self)

    def __del__(self):
        self.delete()

    def log(self, message: str, no_self: bool = False):
        if DEBUG:
            self.collection.log(message, None if no_self else self)

    def tear_down(self):
        self.collection.unregsiter(self)

    def __new__(cls, *args, **kwargs):
        if 'dse_sim.' not in cls.__module__ and cls.__module__ != '__main__':
            raise TypeError(cls.__module__)
        return super().__new__(cls, *args, **kwargs)


class ProfilableActor(Profilable, Actor, metaclass=ABCMeta):
    pass
