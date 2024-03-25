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
    def min_cpu(cls) -> "CpuCapacity":
        return cls.CPU_CAPACITY_1
    
    @classmethod
    def max_cpu(cls) -> "CpuCapacity":
        return cls.CPU_CAPACITY_3

    @classmethod
    def is_max_cpu(cls, cpu: "CpuCapacity") -> bool:
        return cpu == cls.CPU_CAPACITY_3

class MachineHolder(object):
    env: simpy.RealtimeEnvironment
    _machines_dict: dict[CpuCapacity, list[Machine]]
    _key_fn: Callable[[Machine], float]
    min_cpu: CpuCapacity
    max_cpu: CpuCapacity
    min_memory: float
    max_memory: float
    machine_types: list[tuple[CpuCapacity, float]]
    total_cpu_resource: float = 0
    total_memory_resource: float = 0
    in_use_cpu_resource: float = 0
    in_use_memory_resource: float = 0
    total_machine_num: int = 0
    in_use_machine_num: int = 0


    def insert_machine(self, machine: Machine) -> None:
        bisect.insort(self._machines_dict[machine.cpu], machine, key=self._key_fn)

    def __init__(self, machines_df: pd.DataFrame, env: simpy.RealtimeEnvironment = None) -> None:
        self.env = env

        self._machines_dict = {
            CpuCapacity.CPU_CAPACITY_1: [],
            CpuCapacity.CPU_CAPACITY_2: [],
            CpuCapacity.CPU_CAPACITY_3: []
        }

        self.min_cpu = CpuCapacity.min_cpu()
        self.max_cpu = CpuCapacity.max_cpu()
        self.min_memory = float('inf')
        self.max_memory = 0

        self._key_fn = lambda x: x.memory
        for row in machines_df.itertuples(index=False):
            self.insert_machine(Machine(row.machine_ID, row.CPUs, row.Memory))
            if row.Memory < self.min_memory:
                self.min_memory = row.Memory
            elif row.Memory > self.max_memory:
                self.max_memory = row.Memory
            self.total_machine_num += 1
            self.total_cpu_resource += row.CPUs
            self.total_memory_resource += row.Memory 

    def getSingleMachine(self) -> Optional[Machine]:
        for cpu_capacity in [CpuCapacity.CPU_CAPACITY_3, CpuCapacity.CPU_CAPACITY_2, CpuCapacity.CPU_CAPACITY_1]:
            if self._machines_dict[cpu_capacity]:
                out_machine = self._machines_dict[cpu_capacity].pop()
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

    def log(self, scheduler, cpu_usage_list: list = None, memory_usage_list: list = None):
        while any(scheduler.keep_logging):
            if cpu_usage_list is not None:
                cpu_utilization = scheduler.current_cpu_requested / self.in_use_cpu_resource * 100 if self.in_use_cpu_resource > 0 else 0
                cpu_usage_list.append((self.env.now / 3600, self.in_use_cpu_resource, cpu_utilization))
            if memory_usage_list is not None:
                memory_utilization = scheduler.current_memory_requested / self.in_use_memory_resource * 100 if self.in_use_memory_resource > 0 else 0
                memory_usage_list.append((self.env.now / 3600, self.in_use_memory_resource, memory_utilization))
            # print(f"CPU Usage: {self.in_use_cpu_resource:.2f}")
            # print(f"Memory Usage: {self.in_use_memory_resource:.2f}")
            # print(f"Number of machines in use: {self.in_use_machine_num}")

            yield self.env.timeout(1800) # every 30 minutes