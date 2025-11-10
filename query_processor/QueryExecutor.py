from typing import Union

from query_processor.model.Rows import Rows

# from StorageManager import ...
from QueryOptimizer import OptimizationEngine
# from ConcurrencyControlManager import ...
# from FailureRecovery import ...

class QueryExecutor:
    def __init__(self) -> None:
        # self.storage_manager = 
        self.optimization_engine = OptimizationEngine()
        print(self.optimization_engine, "anjay")
        # self.concurrency_control_manager = 
        # self.failure_recovery = 


        
    def execute_select(self, query : str) -> Union[Rows, int]:
        return Rows.from_list(["ini execute_select", "ha", "ha"])

    def execute_update(self, query : str) -> Union[Rows, int]:
        return Rows.from_list(["ini execute_update", "ha", "ha"])

    def execute_begin_transaction(self, query : str) -> Union[Rows, int]:
        return Rows.from_list(["ini execute_begin_transaction", "ha", "ha"])

    def execute_commit(self, query : str) -> Union[Rows, int]:
        return Rows.from_list(["ini execute_commit", "ha", "ha"])

    def execute_abort(self, query : str) -> Union[Rows, int]:
        return Rows.from_list(["ini execute_abort", "ha", "ha"])
