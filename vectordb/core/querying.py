from abc import ABC, abstractmethod


class QueryStrategy(ABC):

    @abstractmethod
    def create_map_tasks(self, queries_key, config):
        pass