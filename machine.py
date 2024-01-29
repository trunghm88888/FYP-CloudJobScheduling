from enum import Enum
import pandas as pd
from typing import Optional, Callable
import bisect
import simpy

class Machine(object):
    machine_id: int
    cpu: float
    memory: float

    def __init__(self, machine_id: int, cpu_capacity: float, memory_capacity: float) -> None:
        self.machine_id = machine_id
        self.cpu = cpu_capacity
        self.memory = memory_capacity

class CpuCapacity(float, Enum):
    CPU_CAPACITY_1 = 0.25
    CPU_CAPACITY_2 = 0.5
    CPU_CAPACITY_3 = 1.0

    @classmethod
    def is_max_cpu(cls, cpu: "CpuCapacity") -> bool:
        return cpu == cls.CPU_CAPACITY_3

class MachineHolder(object):
    env: simpy.RealtimeEnvironment
    _machines_dict: dict[CpuCapacity, list[Machine]]
    _key_fn: Callable[[Machine], float]
    total_cpu_resource: float = 0
    total_memory_resource: float = 0
    in_use_cpu_resource: float = 0
    in_use_memory_resource: float = 0
    total_machine_num: int = 0
    in_use_machine_num: int = 0


    def insert_machine(self, machine: Machine) -> None:
        bisect.insort_left(self._machines_dict[machine.cpu], machine, key=self._key_fn)

    def __init__(self, machines_df: pd.DataFrame, env: simpy.RealtimeEnvironment = None) -> None:
        self.env = env

        self._machines_dict = {
            CpuCapacity.CPU_CAPACITY_1: [],
            CpuCapacity.CPU_CAPACITY_2: [],
            CpuCapacity.CPU_CAPACITY_3: []
        }

        self._key_fn = lambda x: x.memory
        for row in machines_df.itertuples(index=False):
            self.insert_machine(Machine(row.machine_ID, row.CPUs, row.Memory))
            self.total_machine_num += 1
            self.total_cpu_resource += row.CPUs
            self.total_memory_resource += row.Memory 

    def getSingleMachine(self, cpu_capacity: CpuCapacity, ram_capacity: float) -> Optional[Machine]:
        if len(self._machines_dict[cpu_capacity]) > 0:
            i = bisect.bisect_left(self._machines_dict[cpu_capacity], ram_capacity, key=self._key_fn)
            if i != len(self._machines_dict[cpu_capacity]):
                out_machine = self._machines_dict[cpu_capacity].pop(i)
                self.in_use_cpu_resource += out_machine.cpu
                self.in_use_memory_resource += out_machine.memory
                self.in_use_machine_num += 1
                return out_machine 
        return None
    
    def retriveSingleMachine(self, machine: Machine) -> None:
        self.insert_machine(machine)
        self.in_use_cpu_resource -= machine.cpu
        self.in_use_memory_resource -= machine.memory
        self.in_use_machine_num -= 1

    def log(self, scheduler, cpu_usage_list: list = None, memory_usage_list: list = None) -> None:
        while any(scheduler.keep_logging):
            if cpu_usage_list is not None:
                cpu_usage_list.append((self.env.now, self.in_use_cpu_resource / self.total_cpu_resource * 100))
            if memory_usage_list is not None:
                memory_usage_list.append((self.env.now, self.in_use_memory_resource / self.total_memory_resource * 100))

            print(f"CPU Usage: {self.in_use_cpu_resource:.2f}")
            print(f"Memory Usage: {self.in_use_memory_resource:.2f}")
            print(f"Number of machines in use: {self.in_use_machine_num}")

            yield self.env.timeout(900) # every 15 minutes