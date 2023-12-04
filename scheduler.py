import queue
import threading
from machine import Machine, Vendor, VendorCpuCapacity
from typing import Callable, Optional
from simpy import RealtimeEnvironment, Timeout, Event, Container
import pandas as pd
import math
import configparser

def use_lock(lock: threading.Lock, fn: Callable) -> Callable:
    def new(*args, **kwargs):
        with lock:
            return fn(*args, **kwargs)
    return new

class Task(object):
    cpu_resource: float
    memory_resource: float
    runtime: float
    start_timestamp: float

    def __init__(self, cpu, memory, runtime) -> None:
        self.cpu_resource = cpu
        self.memory_resource = memory
        self.runtime = runtime

    def start_running(self, env: RealtimeEnvironment, when_complete_fn: Callable):
        self.start_timestamp = env.now
        run_event = Timeout(env, delay=self.runtime)
        run_event.callbacks.append(when_complete_fn)


class Instance(object):
    _machine: Machine
    _resource_lock: threading.Lock
    _env: RealtimeEnvironment
    cpu_resource: Container
    memory_resource: Container
    tasks: queue.Queue[Task]

    def __init__(self, machine: Machine, env: RealtimeEnvironment) -> None:
        self._machine = machine
        self._resource_lock = threading.Lock()
        # self._acquire_resources = use_lock(self._resource_lock, self._acquire_resources)
        # self._release_resources = use_lock(self._resource_lock, self._release_resources)
        self._env = env
        self.cpu_resource = Container(env, capacity=machine.cpu_capacity, init=machine.cpu_capacity)
        self.memory_resource = Container(env, capacity=machine.memory_capacity, init=machine.memory_capacity)
        self.tasks = queue.Queue()

    # def _acquire_resources(self, cpu_need: float, memory_need: float) -> bool:
    #     if cpu_need <= self.cpu_resource and memory_need <= self.memory_resource:
    #         self.cpu_resource -= cpu_need
    #         self.memory_resource -= memory_need
    #         return True
    #     else:
    #         return False
        
    # def _release_resources(self, cpu: float, memory: float) -> None:
    #     self.cpu_resource += cpu
    #     self.memory_resource += memory

    def when_task_complete(self, task: Task, verbose=False) -> Callable:
        def complete(event: Event):
            if verbose:
                print(f'Task complete at machine {self._machine.machine_id}!')
            yield self.cpu_resource.put(task.cpu_resource) & self.memory_resource.put(task.memory_resource)
            self.tasks.get(task)
        return complete

    def can_receive_workload(self, cpu_need: float, memory_need: float):
        return self.cpu_resource.level >= cpu_need and self.memory_resource.level >= memory_need

    def receive_task(self, task: Task) -> Optional[Task]:
        yield self.cpu_resource.get(task.cpu_resource) & self.memory_resource.get(task.memory_resource)
        self.tasks.put(task)
        task.start_running(self._env, self.when_task_complete(task, verbose=True))

    def get_longest_remaining_runtime(self) -> float:
        return max([task.runtime - (self._env.now - task.start_timestamp) for task in self.tasks.queue])


class Scheduler(object):
    NO_OF_BINS = 10
    BIN_STEP = 120
    runtime_bins: list[queue.Queue[Instance]]
    tasks_queue: queue.Queue[Task]
    vendor: Vendor
    env: RealtimeEnvironment
    _tasks_accepting_thread: threading.Thread
    _task_broker_thread: threading.Thread
    _bin_updating_thread: threading.Thread
    _vender_communicating_thread: threading.Thread

    def create_bins(self):
        self.runtime_bins = [queue.Queue() for _ in range(self.NO_OF_BINS)]

    def get_bin_idx(self, runtime: float) -> int:
        if runtime < self.BIN_STEP:
            return 0
        return math.floor(math.log(runtime // self.BIN_STEP, base=2))

    def __init__(self, env: RealtimeEnvironment, tasks_csv_path: str, vendor: Vendor) -> None:
        self.env = env
        self.create_bins()
        self.tasks_queue = queue.Queue()
        self._tasks_accepting_thread = threading.Thread(target=self.add_task_to_queue, args=(tasks_csv_path,))

    def add_task_to_queue(self, tasks_csv_path: str):
        reader = pd.read_csv(tasks_csv_path, chunksize=1000)
        for chunk in reader:
            for row in chunk.itertuples():
                task = Task(row[0], row[1], row[2])
                self.tasks_queue.put(task)

    def dequeue_task_and_assign(self) -> None:
        while not self.tasks_queue.empty():
            done = False
            task = self.tasks_queue.get()
            bin_idx = self.get_bin_idx(task.runtime)
            if bin_idx >= self.NO_OF_BINS:
                bin_idx = self.NO_OF_BINS - 1
            for inst in self.runtime_bins[bin_idx]:
                if inst.can_receive_workload(task.cpu_resource, task.memory_resource):
                    inst.receive_task(task)
                    done = True
                    break
            if not done:
                for cpu_capacity in VendorCpuCapacity:
                    if cpu_capacity >= task.cpu_resource:
                        machine = self.vendor.getSingleMachine(cpu_capacity, task.memory_resource)
                        if machine is not None:
                            inst = Instance(machine, self.env)
                            self.runtime_bins[bin_idx].put(inst)
                            inst.receive_task(task)
                            done = True
                            break
                


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('data.ini')
    default = config['DEFAULT']

    machines_info = pd.read_csv(default['machines_info_path'])
    print(f'Number of machines: {machines_info.shape[0]:,}')
    vendor = Vendor(machines_info)

    env = RealtimeEnvironment(factor=0.1, strict=False)
    scheduler = Scheduler(env, default['tasks_path'], vendor)
    env.run()
    