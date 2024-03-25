import csv
from collections import deque
import simpy
import numpy as np
import pandas as pd
import math
import configparser
from datetime import datetime
from matplotlib import pyplot as plt

from machine import Machine, MachineHolder, CpuCapacity


CPU_USAGE = []
MEMORY_USAGE = []

class Task(object):
    cpu_request: float
    memory_request: float
    runtime: float
    expected_finished: float
    is_running: bool = False

    def __init__(self, cpu, memory, runtime) -> None:
        self.cpu_request = cpu
        self.memory_request = memory
        self.runtime = runtime

    def start_running(self, env: simpy.Environment, instance_attached: "Instance", scheduler: "Scheduler"):
        self.is_running = True
        yield env.timeout(self.runtime)
        self.is_running = False
        scheduler.current_memory_requested -= self.memory_request
        scheduler.current_cpu_requested -= self.cpu_request

        instance_attached.current_cpu += self.cpu_request
        instance_attached.current_memory += self.memory_request

        instance_attached.tasks.remove(self) 
        

class Instance(object):
    machine: Machine
    env: simpy.RealtimeEnvironment
    current_cpu: float
    current_memory: float
    tasks: list[Task]

    def __init__(self, machine: Machine, env: simpy.RealtimeEnvironment) -> None:
        self.machine = machine
        self.env = env
        self.current_cpu = machine.cpu
        self.current_memory = machine.memory
        self.tasks = []

    def can_receive_workload(self, cpu_need: float, memory_need: float):
        return self.current_cpu >= cpu_need and self.current_memory >= memory_need

    def receive_task(self, task: Task, scheduler: "Scheduler"):
        self.tasks.append(task)
        try:
            if task.cpu_request > 0:
                task.succesful_get_cpu = False
                self.current_cpu -= task.cpu_request
                task.succesful_get_cpu = True
            if task.memory_request > 0:
                task.succesful_get_memory = False
                self.current_memory -= task.memory_request
                task.succesful_get_memory = True
        except simpy.Interrupt as interrupt:
            if hasattr(task, "succesful_get_cpu") and task.succesful_get_cpu:
                self.current_cpu += task.cpu_request
            if hasattr(task, "succesful_get_memory") and task.succesful_get_memory:
                self.current_memory += task.memory_request
            self.tasks.remove(task)
            return False

        task.expected_finished = self.env.now + task.runtime
        self.env.process(task.start_running(self.env, self, scheduler))
        return True

    def get_longest_remaining_runtime(self) -> float:
        if not self.tasks:
            return 0
        now = self.env.now
        return max([task.expected_finished - now if hasattr(task, "expected_finished") else task.runtime for task in self.tasks])


class Scheduler(object):
    BIN_STEP = 180
    runtime_bins: list[list[Instance]]
    normalized_resource_matrix: np.ndarray
    tasks_queue: deque[Task]
    read_all: bool = False
    num_of_tasks_assigned: int = 0
    num_of_time_get_machine: int = 0
    num_of_time_retrive_machine: int = 0
    current_cpu_requested: float = 0
    current_memory_requested: float = 0
    dequeue_all: bool = False
    keep_logging: list[bool]
    machine_holder: MachineHolder
    env: simpy.RealtimeEnvironment

    task_queue_thread: simpy.Process
    task_dequeue_thread: simpy.Process
    task_update_binIdx_threads: list[simpy.Process]
    machine_usage_logging_thread: simpy.Process

    def create_bins(self, no_of_bins: int):
        self.runtime_bins = [[] for _ in range(no_of_bins)]

    def get_bin_idx(self, runtime: float) -> int:
        if runtime < self.BIN_STEP:
            return 0
        
        bin_idx = math.floor(math.log(runtime // self.BIN_STEP, 2)) + 1
        if bin_idx > len(self.runtime_bins) - 1:
            bin_idx = len(self.runtime_bins) - 1
        return bin_idx

    def __init__(self, no_of_bins: int, env: simpy.RealtimeEnvironment, tasks_csv_path: str, vendor: MachineHolder) -> None:
        self.env = env
        self.create_bins(no_of_bins=no_of_bins)
        self.tasks_queue = deque()
        self.machine_holder = vendor

        self.keep_logging = [True for _ in range(len(self.runtime_bins))]
        self.task_queue_thread = self.env.process(self.add_task_to_queue(tasks_csv_path))
        self.machine_usage_logging_thread = self.env.process(self.machine_holder.log(self, CPU_USAGE, MEMORY_USAGE))

    def assign_task_check_start_time(self, start_time: float, task: Task) -> bool:
        if start_time > self.env.now:
            return False
        else:
            self.tasks_queue.append(task)
            return True

    def add_task_to_queue(self, tasks_csv_path: str):
        read = 0
        f = open(tasks_csv_path, 'r')
        reader = csv.reader(f)
        next(reader)

        for row in reader:
            start_time = float(row[0])
            task = Task(float(row[1]), float(row[2]), float(row[3]))
            while not self.assign_task_check_start_time(start_time, task):
                timeout = start_time - self.env.now
                if timeout > 0:
                    yield self.env.timeout(timeout)
            read += 1
            if read == 1e7:
                break

        print("All tasks read")
        self.read_all = True
        f.close()

    def try_assign_task_to_instance(self, task: Task, inst: Instance):
        if inst.receive_task(task, self):
            self.current_cpu_requested += task.cpu_request
            self.current_memory_requested += task.memory_request
            self.num_of_tasks_assigned += 1
            return True
        
        return False

    def try_assign_task(self, task: Task, bin_idx: int):
        for inst in self.runtime_bins[bin_idx]:
            if inst.can_receive_workload(task.cpu_request, task.memory_request):
                if self.try_assign_task_to_instance(task, inst):
                    return True
        return False
    
    def get_new_machine_assign_task_baseline(self, task: Task, bin_idx: int):
        machine = self.machine_holder.getSingleMachine()
        if machine is not None:
            self.num_of_time_get_machine += 1
            inst = Instance(machine, self.env)
            self.try_assign_task_to_instance(task, inst)
            self.runtime_bins[bin_idx].append(inst)
            return True
        else:
            return False
    
    def dequeue_task_and_assign_baseline(self):
        while self.tasks_queue:
            task = self.tasks_queue.popleft()
            bin_idx = 0 # only 1 bin = baseline
            succesful_assigned = self.try_assign_task(task, bin_idx)
            if succesful_assigned:
                continue
            else:
                succesful_assigned = self.get_new_machine_assign_task_baseline(task, bin_idx)
                if succesful_assigned:
                    continue
                else:
                    self.tasks_queue.appendleft(task)
                    yield self.env.timeout(10)

        if not self.read_all:
            print(f"Number of tasks assigned: {self.num_of_tasks_assigned}")
            yield self.env.timeout(10)
            yield self.env.process(self.dequeue_task_and_assign_baseline())

        self.dequeue_all = True


    def get_eligible_instance_similar_runtime(self, task: Task, bin_idx: int):
        eligible_insts = [inst for inst in self.runtime_bins[bin_idx] if inst.can_receive_workload(task.cpu_request, task.memory_request)]
        if eligible_insts:
            subtract_runtimes = [abs(inst.get_longest_remaining_runtime() - task.runtime) for inst in eligible_insts]
            return eligible_insts[np.argmin(subtract_runtimes)]


    def get_eligible_instance_most_resources(self, task: Task, bin_idx: int):
        eligible_insts = [inst for inst in self.runtime_bins[bin_idx] if inst.can_receive_workload(task.cpu_request, task.memory_request)]
        if eligible_insts:
            return eligible_insts[np.argmax([inst.current_cpu  * inst.current_memory for inst in eligible_insts])]
        
    
    def get_new_instance(self):
        new_machine = self.machine_holder.getSingleMachine()
        if new_machine is not None:
            self.num_of_time_get_machine += 1
            return Instance(new_machine, self.env)
        
        return None


    def dequeue_task_and_assign(self):
        while self.tasks_queue:
            succesful_assigned = False
            # get the task
            task = self.tasks_queue.popleft()
            # get the bin index of this task's runtime
            bin_idx = self.get_bin_idx(task.runtime)
            
            # check if there is any instance with similar runtime in the same bin
            same_bin_inst = self.get_eligible_instance_similar_runtime(task, bin_idx)
            if same_bin_inst is not None:
                succesful_assigned = self.try_assign_task_to_instance(task, same_bin_inst)
                if succesful_assigned:
                    continue

            # checking in the greater bins
            next_bin_idx = bin_idx + 1
            while not succesful_assigned and next_bin_idx < len(self.runtime_bins):
                next_bin_inst = self.get_eligible_instance_most_resources(task, next_bin_idx)
                if next_bin_inst is not None:
                    succesful_assigned = self.try_assign_task_to_instance(task, next_bin_inst)
                else:
                    next_bin_idx += 1
            
            # checking in the smaller bins
            if not succesful_assigned:
                next_bin_idx = bin_idx - 1
                while not succesful_assigned and next_bin_idx >= 0:
                    prev_bin_inst = self.get_eligible_instance_most_resources(task, next_bin_idx)
                    if prev_bin_inst is not None:
                        succesful_assigned = self.try_assign_task_to_instance(task, prev_bin_inst)
                        if succesful_assigned:
                            # instance is moved to the current bin
                            idx = self.runtime_bins[next_bin_idx].index(prev_bin_inst)
                            self.runtime_bins[bin_idx].append(self.runtime_bins[next_bin_idx].pop(idx))
                    else:
                        next_bin_idx -= 1

            # if still not assigned, then assign to new instance
            if not succesful_assigned:
                new_inst = self.get_new_instance()
                if new_inst is not None:
                    succesful_assigned = self.try_assign_task_to_instance(task, new_inst)
                    if succesful_assigned:
                        self.runtime_bins[self.get_bin_idx(task.runtime)].append(new_inst)
                else:
                    self.tasks_queue.appendleft(task)
                    yield self.env.timeout(10)

        if not self.read_all:
            print(f"Number of tasks assigned: {self.num_of_tasks_assigned}")
            yield self.env.timeout(10)
            yield self.env.process(self.dequeue_task_and_assign())

        self.dequeue_all = True

    def update_binIdx(self, bin_idx: int):
        while any(self.runtime_bins) or not self.dequeue_all:
            self.update_binIdx_helper(bin_idx)
            yield self.env.timeout(self.BIN_STEP * 2**(bin_idx - 2))    

        # finished
        self.keep_logging[bin_idx] = False

    def update_binIdx_baseline(self):
        while self.runtime_bins[0] or not self.dequeue_all:
            self.update_binIdx_helper_baseline()
            yield self.env.timeout(self.BIN_STEP * 2**-2)    

        # finished
        for i in range(len(self.runtime_bins)):
            self.keep_logging[i] = False

    def update_binIdx_helper_baseline(self):
        remove = 0
        print(f"bin 0, number of instance: {len(self.runtime_bins[0])}")
        i = len(self.runtime_bins[0]) - 1
        total_tasks = 0
        while i >= 0:
            longest_runtime = self.runtime_bins[0][i].get_longest_remaining_runtime()
            total_tasks += len(self.runtime_bins[0][i].tasks)
            # remove the machines if no task is running
            if longest_runtime <= 0:
                self.machine_holder.retriveSingleMachine(self.runtime_bins[0].pop(i).machine)
                self.num_of_time_retrive_machine += 1
                remove += 1
            i -= 1

        print(f"Current time is {self.env.now // 86400} days {(self.env.now % 86400) // 3600} hours {(self.env.now % 3600) // 60} minutes")
        print(f"There are {total_tasks} tasks running in bin 0")
        if remove > 0:
            print(f"Removed {remove} empty instances")
        print("-" * 50)

    def update_binIdx_helper(self, bin: int):
        remove = 0
        print(f"bin {bin}, number of instance: {len(self.runtime_bins[bin])}")
        i = len(self.runtime_bins[bin]) - 1
        total_tasks = 0
        while i >= 0:
            longest_runtime = self.runtime_bins[bin][i].get_longest_remaining_runtime()
            total_tasks += len(self.runtime_bins[bin][i].tasks)
            # remove the machines if no task is running
            if longest_runtime <= 0:
                self.machine_holder.retriveSingleMachine(self.runtime_bins[bin].pop(i).machine)
                self.num_of_time_retrive_machine += 1
                remove += 1
            else:
                new_bin_idx = self.get_bin_idx(longest_runtime)
                # need to update bin index
                if new_bin_idx != bin:
                    self.runtime_bins[new_bin_idx].append(self.runtime_bins[bin].pop(i))
            i -= 1

        print(f"Current time is {self.env.now // 86400} days {(self.env.now % 86400) // 3600} hours {(self.env.now % 3600) // 60} minutes")
        print(f"There are {total_tasks} tasks running in bin {bin}")
        if remove > 0:
            print(f"Removed {remove} empty instances")
        print("-" * 50)

    def run(self):
        yield self.task_queue_thread \
            & self.env.process(self.dequeue_task_and_assign()) \
            & simpy.AllOf(self.env, [self.env.process(self.update_binIdx(i)) for i in range(len(self.runtime_bins))]) \
            & self.machine_usage_logging_thread
        print("Finished!!!")

    def run_baseline(self):
        yield self.task_queue_thread \
            & self.env.process(self.dequeue_task_and_assign_baseline()) \
            & self.env.process(self.update_binIdx_baseline()) \
            & self.machine_usage_logging_thread
        print("Finished!!!")
                


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('data.ini')
    default = config['DEFAULT']

    machines_info = pd.read_csv(default['machines_info_path'])
    print(f'Number of machines: {machines_info.shape[0]:,}')

    env = simpy.Environment()
    scheduler = Scheduler(7, env, default['tasks_path'], MachineHolder(env=env, machines_df=machines_info))
    env.process(scheduler.run_baseline())
    env.run()

    current_time = lambda: datetime.now().strftime(r"%d_%m_%Y_%H_%M_%S")

    with open(f"logging/cpu_usage_{current_time()}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "cpu_utilization"])
        writer.writerows(CPU_USAGE)

    cpu_time, cpu_usage, cpu_utilization = zip(*CPU_USAGE)

    print(f"Average CPU usage: {sum(cpu_usage) / len(cpu_usage)}")
    plt.plot(cpu_time, cpu_usage)
    plt.xlabel("Time (hours)")
    plt.ylabel("CPU Usage (units)")
    plt.yscale("log")
    plt.show()

    print(f"Average CPU utilization: {sum(cpu_utilization) / len(cpu_utilization)}")
    plt.plot(cpu_time, cpu_utilization)
    plt.xlabel("Time (hours)")
    plt.ylabel("CPU Utilization (%)")
    plt.show()

    with open(f"logging/memory_usage_{current_time()}.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["time", "memory_utilization"])
        writer.writerows(MEMORY_USAGE)


    memory_time, memory_usage, memory_utilization = zip(*MEMORY_USAGE)
    print(f"Average Memory usage: {sum(memory_usage) / len(memory_usage)}")
    plt.plot(memory_time, memory_usage)
    plt.xlabel("Time (hours)")
    plt.ylabel("Memory Usage (units)")
    plt.yscale("log")
    plt.show()

    print(f"Average Memory utilization: {sum(memory_utilization) / len(memory_utilization)}")
    plt.plot(memory_time, memory_utilization)
    plt.xlabel("Time (hours)")
    plt.ylabel("Memory Utilization (%)")
    plt.show()