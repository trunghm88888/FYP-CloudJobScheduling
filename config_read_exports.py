import configparser

config = configparser.ConfigParser()
config.read('data.ini')
default = config['DEFAULT']

machine_path = default['machines_info_path']
task_path = default['tasks_path']

MACHINE_CPU_RES_NAME = 'CPUs'
MACHINE_MEMORY_RES_NAME = 'Memory'
TASK_RUNTIME_NAME = 'runtime'
TASK_START_TIME_NAME = 'start_time'
TASK_CPU_REQ_NAME = 'CPU_request'
TASK_MEMORY_REQ_NAME = 'memory_request'