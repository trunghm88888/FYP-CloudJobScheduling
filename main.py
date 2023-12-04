import pandas as pd, numpy as np
from machine import Vendor
import os, glob
import configparser


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('data.ini')
    default = config['DEFAULT']

    machines_info = pd.read_csv(default['machines_info_path'])
    print(f'Number of machines: {machines_info.shape[0]:,}')
    vendor = Vendor(machines_info)

    tasks = pd.read_csv(default['tasks_path'])
    print(f'Number of tasks: {tasks.shape[0]:,}')