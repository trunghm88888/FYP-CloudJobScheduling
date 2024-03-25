import pandas as pd, numpy as np
from machine import MachineHolder
from matplotlib import pyplot as plt
from config_read_exports import machine_path, task_path, TASK_RUNTIME_NAME, TASK_START_TIME_NAME, TASK_CPU_REQ_NAME, TASK_MEMORY_REQ_NAME


if __name__ == '__main__':
    machines_info = pd.read_csv(machine_path)
    print(f'Number of machines: {machines_info.shape[0]:,}')
    vendor = MachineHolder(machines_info)
    print(f"Total CPUs in the cluster: {vendor.total_cpu_resource:,}")
    print(f"Min CPU: {vendor.min_cpu}, Max CPU: {vendor.max_cpu}")
    print(f"Total memory in the cluster: {vendor.total_memory_resource:,}")
    print(f"Min memory: {vendor.min_memory}, Max memory: {vendor.max_memory}")

    tasks = pd.read_csv(task_path)
    print(f'Number of tasks: {tasks.shape[0]:,}')

    print("-" * 50)
    print("Tasks cpu request statistics:")
    print(tasks[TASK_CPU_REQ_NAME].describe())
    plt.hist(tasks[TASK_CPU_REQ_NAME], bins=100)
    plt.xlabel("CPU request")
    plt.ylabel("Number of tasks")
    plt.show()

    print("-" * 50)
    print("Tasks memory request statistics:")
    print(tasks[TASK_MEMORY_REQ_NAME].describe())
    plt.hist(tasks[TASK_MEMORY_REQ_NAME], bins=100)
    plt.xlabel("Memory request")
    plt.ylabel("Number of tasks")
    plt.show()

    print("-" * 50)
    print("Tasks runtime statistics:")
    print(tasks[TASK_RUNTIME_NAME].describe())
    runtime_95th = np.percentile(tasks[TASK_RUNTIME_NAME], 95)
    print(f"Task runtime 95th percentile: {runtime_95th // 86400} days {(runtime_95th % 86400) // 3600} hours {(runtime_95th % 3600) // 60} minutes")

    print("-" * 50)
    print("Tasks start time statistics:")
    print(tasks[TASK_START_TIME_NAME].describe())
    plt.hist(tasks[TASK_START_TIME_NAME], bins=100)
    plt.xlabel("Task start timestamp (seconds)")
    plt.ylabel("Number of tasks")
    plt.show()