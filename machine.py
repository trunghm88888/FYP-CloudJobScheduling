from enum import Enum
import pandas as pd
from typing import Optional, Callable
import bisect

class Machine(object):
    machine_id: int
    cpu: float
    memory: float

    def __init__(self, machine_id: int, cpu_capacity: float, memory_capacity: float) -> None:
        self.machine_id = machine_id
        self.cpu = cpu_capacity
        self.memory = memory_capacity

class VendorCpuCapacity(float, Enum):
    CPU_CAPACITY_1 = 0.25
    CPU_CAPACITY_2 = 0.5
    CPU_CAPACITY_3 = 1.0

class Vendor(object):
    _machines_dict: dict[VendorCpuCapacity, list[Machine]]
    _key_fn: Callable[[Machine], float]

    def insert_machine(self, machine: Machine) -> None:
        bisect.insort_left(self._machines_dict[machine.cpu], machine, key=self._key_fn)


    def __init__(self, machines_df: pd.DataFrame) -> None:
        self._machines_dict = {
            VendorCpuCapacity.CPU_CAPACITY_1: [],
            VendorCpuCapacity.CPU_CAPACITY_2: [],
            VendorCpuCapacity.CPU_CAPACITY_3: []
        }

        self._key_fn = lambda x: x.memory
        
        for row in machines_df.itertuples(index=False):
            self.insert_machine(Machine(row.machine_ID, row.CPUs, row.Memory)) 


    def getSingleMachine(self, cpu_capacity: VendorCpuCapacity, ram_capacity: float) -> Optional[Machine]:
        if len(self._machines_dict[cpu_capacity]) > 0:
            i = bisect.bisect_left(self._machines_dict[cpu_capacity], ram_capacity, key=self._key_fn)
            if i != len(self._machines_dict[cpu_capacity]):
                out_machine = self._machines_dict[cpu_capacity].pop(i)
                return out_machine
            
        return None
    
    
    def retriveSingleMachine(self, machine: Machine) -> None:
        self.insert_machine(machine)