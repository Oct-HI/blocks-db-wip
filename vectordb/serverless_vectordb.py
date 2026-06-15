from lithops import FunctionExecutor

from vectordb.config import SvlessVectorDBParams

from .indexing.indexator import initialize_database
from .orchestration.orchestrator import Orchestrator

class ServerlessVectorDB():
    
    def __init__(self, **parameters):
        self.params: SvlessVectorDBParams = SvlessVectorDBParams(**parameters)
        self.indexing_executor = FunctionExecutor()
        self.orchestrator = Orchestrator(self.params)
        
    def indexing(self, filename, num_workers):
        if not self.params.skip_init:
            return initialize_database(filename, self.params, self.indexing_executor, num_workers)
        return {}
        
    def search(self, id, query_vector, filter_tags=None):
        return self.orchestrator.search(id, query_vector, self.params.num_centroids_search, self.params.k_search, self.params.k_result, filter_tags=filter_tags)