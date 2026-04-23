from abc import ABC, abstractmethod


class IndexBuilder(ABC):

    @abstractmethod
    def build(self, ids, vectors):
        pass