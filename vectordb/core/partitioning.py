from abc import ABC, abstractmethod


class Partitioner(ABC):

    @abstractmethod
    def partition(self, data):
        pass