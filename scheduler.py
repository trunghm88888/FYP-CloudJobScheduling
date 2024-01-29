import csv
from collections import deque
from machine import Machine, MachineHolder, CpuCapacity
import simpy
import pandas as pd
import math
import configparser
from matplotlib import pyplot as plt


CPU_USAGE = []
MEMORY_USAGE = []

class Task(object):
    cpu_resource: float
    memory_resource: float
    runtime: float
    expected_finished: float
    is_running: bool = False

    def __init__(self, cpu, memory, runtime) -> None:
        self.cpu_resource = cpu
        self.memory_resource = memory
        self.runtime = runtime

    def start_running(self, env: simpy.Environment, instance_attached: "Instance"):
        self.is_running = True
        yield env.timeout(self.runtime)
        # print(f"Task finished at {env.now}")
        self.is_running = False

        if self.cpu_resource > 0:
            yield instance_attached.cpu_resource.put(self.cpu_resource)
        if self.memory_resource > 0:
            yield instance_attached.memory_resource.put(self.memory_resource)

        instance_attached.tasks.remove(self) 
        

class Instance(object):
    machine: Machine
    env: simpy.RealtimeEnvironment
    cpu_resource: simpy.Container
    memory_resource: simpy.Container
    tasks: list[Task]

    def __init__(self, machine: Machine, env: simpy.RealtimeEnvironment) -> None:
        self.machine = machine
        self.env = env
        self.cpu_resource = simpy.Container(env, capacity=machine.cpu, init=machine.cpu)
        self.memory_resource = simpy.Container(env, capacity=machine.memory, init=machine.memory)
        self.tasks = []

    def can_receive_workload(self, cpu_need: float, memory_need: float):
        return self.cpu_resource.level >= cpu_need and self.memory_resource.level >= memory_need

    def receive_task(self, task: Task):
        self.tasks.append(task)
        try:
            if task.cpu_resource > 0:
                task.succesful_get_cpu = False
                yield self.cpu_resource.get(task.cpu_resource)
                task.succesful_get_cpu = True
            if task.memory_resource > 0:
                task.succesful_get_memory = False
                yield self.memory_resource.get(task.memory_resource)
                task.succesful_get_memory = True
        except simpy.Interrupt as interrupt:
            if hasattr(task, "succesful_get_cpu") and task.succesful_get_cpu:
                yield self.cpu_resource.put(task.cpu_resource)
            if hasattr(task, "succesful_get_memory") and task.succesful_get_memory:
                yield self.memory_resource.put(task.memory_resource)
            self.tasks.remove(task)
            return

        task.expected_finished = self.env.now + task.runtime
        self.env.process(task.start_running(self.env, self))

    def get_longest_remaining_runtime(self) -> float:
        if not self.tasks:
            return 0
        now = self.env.now
        return max([task.expected_finished - now if hasattr(task, "expected_finished") else task.runtime for task in self.tasks])


class Scheduler(object):
    BIN_STEP = 450
    runtime_bins: list[list[Instance]]
    no_of_queues = 1
    tasks_queue: list[deque[Task]]
    read_all = False
    num_of_tasks_assigned: int = 0
    num_of_time_get_machine: int = 0
    num_of_time_retrive_machine: int = 0
    dequeue_all: list[bool]
    keep_logging: list[bool]
    machine_holder: MachineHolder
    env: simpy.RealtimeEnvironment

    task_queue_thread: simpy.Process
    task_dequeue_threads: list[simpy.Process]
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
        self.tasks_queue = [deque() for i in range(self.no_of_queues)]
        self.machine_holder = vendor

        self.dequeue_all = [False for _ in range(self.no_of_queues)]
        self.keep_logging = [True for _ in range(len(self.runtime_bins))]
        self.task_queue_thread = self.env.process(self.add_task_to_queue(tasks_csv_path))
        self.machine_usage_logging_thread = self.env.process(self.machine_holder.log(self, CPU_USAGE, MEMORY_USAGE))

    def assign_task_check_start_time(self, start_time: float, task: Task, queue_idx: int) -> bool:
        if start_time > self.env.now:
            return False
        else:
            self.tasks_queue[queue_idx].append(task)
            return True

    def add_task_to_queue(self, tasks_csv_path: str):
        read = 0
        f = open(tasks_csv_path, 'r')
        reader = csv.reader(f)
        next(reader)

        for row in reader:
            start_time = float(row[0])
            task = Task(float(row[1]), float(row[2]), float(row[3]))
            if task.runtime > 1e5:
                continue
            queue_idx = read % self.no_of_queues
            while not self.assign_task_check_start_time(start_time, task, queue_idx):
                timeout = start_time - self.env.now
                if timeout > 0:
                    yield self.env.timeout(timeout)
            read += 1
            if read == 1e6:
                break

        print("All tasks read")
        self.read_all = True
        f.close()

    def try_assign_task(self, task: Task, bin_idx: int):
        for inst in self.runtime_bins[bin_idx]:
            if inst.can_receive_workload(task.cpu_resource, task.memory_resource):
                try_assign = self.env.process(inst.receive_task(task))
                waiting = self.env.timeout(1)
                yield try_assign | waiting
                if not try_assign.triggered:
                    try_assign.interrupt("Timeout")
                    continue
                else:
                    return True
        return False
    
    def dequeue_task_and_assign_baseline(self, id: int):
        while self.tasks_queue[id]:
            task = self.tasks_queue[id].popleft()
            bin_idx = 0 # only 1 bin = baseline
            succesful_assigned = yield self.env.process(self.try_assign_task(task, bin_idx))
            if succesful_assigned:
                self.num_of_tasks_assigned += 1
                continue
            else:
                for cpu_capacity in CpuCapacity:
                    if cpu_capacity >= task.cpu_resource:
                        machine = self.machine_holder.getSingleMachine(cpu_capacity, task.memory_resource)
                        if machine is not None:
                            self.num_of_time_get_machine += 1
                            inst = Instance(machine, self.env)
                            self.env.process(inst.receive_task(task))
                            # print("Task assigned to new instance")
                            self.num_of_tasks_assigned += 1
                            self.runtime_bins[bin_idx].append(inst)
                            break
                        else:
                            if not CpuCapacity.is_max_cpu(cpu_capacity):
                                continue
                            else:
                                self.tasks_queue[id].appendleft(task)
                                # print(f"Task is waiting for machine")
                                yield self.env.timeout(10)

        if not self.read_all:
            print(f"Number of tasks assigned: {self.num_of_tasks_assigned}")
            yield self.env.timeout(10)
            yield self.env.process(self.dequeue_task_and_assign_baseline(id))

        self.dequeue_all[id] = True


    def dequeue_task_and_assign(self, id: int):
        while self.tasks_queue[id]:
            task = self.tasks_queue[id].popleft()
            bin_idx = self.get_bin_idx(task.runtime)
            
            next_bin_idx = bin_idx
            while next_bin_idx < len(self.runtime_bins):
                succesful_assigned = yield self.env.process(self.try_assign_task(task, next_bin_idx))
                if succesful_assigned:
                    self.num_of_tasks_assigned += 1
                    break
                else:
                    next_bin_idx += 1
            
            if not task.is_running:
                next_bin_idx = bin_idx - 1
                while next_bin_idx >= 0:
                    yield self.env.process(self.try_assign_task(task, next_bin_idx))
                    if task.is_running:
                        self.num_of_tasks_assigned += 1
                        break
                    else:
                        next_bin_idx -= 1

            if not task.is_running:
                for cpu_capacity in CpuCapacity:
                    if cpu_capacity >= task.cpu_resource:
                        machine = self.machine_holder.getSingleMachine(cpu_capacity, task.memory_resource)
                        if machine is not None:
                            self.num_of_time_get_machine += 1
                            inst = Instance(machine, self.env)
                            self.env.process(inst.receive_task(task))
                            # print("Task assigned to new instance")
                            self.num_of_tasks_assigned += 1
                            self.runtime_bins[bin_idx].append(inst)
                            break
                        else:
                            if not CpuCapacity.is_max_cpu(cpu_capacity):
                                continue
                            else:
                                self.tasks_queue[id].appendleft(task)
                                # print(f"Task is waiting for machine")
                                yield self.env.timeout(10)

        if not self.read_all:
            print(f"Number of tasks assigned: {self.num_of_tasks_assigned}")
            yield self.env.timeout(10)
            yield self.env.process(self.dequeue_task_and_assign(id))

        self.dequeue_all[id] = True

    def update_binIdx(self, bin_idx: int):
        while any(self.runtime_bins) or not all(self.dequeue_all):
            self.update_binIdx_helper(bin_idx)
            yield self.env.timeout(self.BIN_STEP * 2**(bin_idx - 2))    

        # finished
        self.keep_logging[bin_idx] = False

    def update_binIdx_baseline(self):
        while self.runtime_bins[0] or not all(self.dequeue_all):
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
            # print(f"number of tasks in instance {i}: {len(self.runtime_bins[bin][i].tasks)}, longest runtime: {longest_runtime}")
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
            # print(f"number of tasks in instance {i}: {len(self.runtime_bins[bin][i].tasks)}, longest runtime: {longest_runtime}")
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
        # print(f"Number of time get machine: {self.num_of_time_get_machine}")
        # print(f"Number of time retrive machine: {self.num_of_time_retrive_machine}")

    def run(self):
        yield self.task_queue_thread \
            & self.env.process(self.dequeue_task_and_assign(0)) \
            & simpy.AllOf(self.env, [self.env.process(self.update_binIdx(i)) for i in range(len(self.runtime_bins))]) \
            & self.machine_usage_logging_thread
        print("Finished!!!")

    def run_baseline(self):
        yield self.task_queue_thread \
            & self.env.process(self.dequeue_task_and_assign_baseline(0)) \
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
    scheduler = Scheduler(6, env, default['tasks_path'], MachineHolder(env=env, machines_df=machines_info))
    env.process(scheduler.run())
    env.run()

    cpu_time, cpu_usage = zip(*CPU_USAGE)
    print(f"Max CPU usage: {max(cpu_usage)}")
    print(f"Average CPU usage: {sum(cpu_usage) / len(cpu_usage)}")
    plt.plot(cpu_time, cpu_usage)
    plt.xlabel("Time (seconds)")
    plt.ylabel("CPU Usage (%)")
    plt.show()

    memory_time, memory_usage = zip(*MEMORY_USAGE)
    print(f"Max Memory usage: {max(memory_usage)}")
    print(f"Average Memory usage: {sum(memory_usage) / len(memory_usage)}")
    plt.plot(memory_time, memory_usage)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Memory Usage (%)")
    plt.show()