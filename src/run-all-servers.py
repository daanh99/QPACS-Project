from dataclasses import dataclass
from typing import List, Tuple, Optional

import io
import os
import random
import socket
import string

import pandas as pd
import paramiko
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko import SSHClient
from paramiko.buffered_pipe import PipeTimeout

from src.GoogleCloudInfo import GoogleCloudInfo, cloud_info_list
from src.MasterNode import MasterNode
from src.SlaveNode import SlaveNode

ComputeEngine = get_driver(Provider.GCE)

home_user = 'am72ghiassi'
ex_id = ''.join(random.choice(string.ascii_lowercase) for i in range(8))  # Generate a random project id
EX_RUNTIME = 300  # seconds
print(f"Experiment ID: {ex_id}")


class ExperimentOptions:
    def __init__(self, cpu_cores: int, amount_ram: int, network: str, batch_size: int, amount_nodes: int):
        self.cpu_cores = cpu_cores
        self.amount_ram = amount_ram
        self.network = network
        self.batch_size = batch_size
        self.amount_nodes = amount_nodes

    def get_filename(self):
        return f"nodes{self.amount_nodes}-cores{self.cpu_cores}-memory{self.amount_ram}-network{self.network}-batchsize{self.batch_size}.log"


def run_command(ssh: SSHClient, command: str):
    print(f"Executing command: {command}")
    stdin, stdout, stderr = ssh.exec_command(command)
    stderr_output = stderr.read()
    if len(stderr_output) > 0:
        print(stdout.read())
        print(stderr_output)

@dataclass
class Master:
    ssh: SSHClient
    pubip: str
    privip: str


def read_log_file(file_name: str):
    with io.open(file_name, mode='r', encoding='utf-8') as file:
        file.seek(0, os.SEEK_END)
        file.seek(file.tell() - 1048, os.SEEK_SET)
        last_data = file.read()

    str_to_find = 'Wall clock time is '
    start_of_number = last_data.index(str_to_find) + len(str_to_find)
    next_space_index = last_data.index(' ', start_of_number)

    return float(last_data[start_of_number:next_space_index])


def execute_experiment(master: Master, slaves: List[SSHClient], options: ExperimentOptions) -> float:
    run_command(master.ssh, '/home/am72ghiassi/bd/spark/sbin/start-master.sh')
    for slave in slaves:
        run_command(slave, f'/home/am72ghiassi/bd/spark/sbin/start-slave.sh spark://{master.privip}:7077')

    max_epochs = 50
    command = f"/home/{home_user}/bd/spark/bin/spark-submit --master spark://{master.privip}:7077 --driver-cores 1 " + \
        f"--driver-memory 1G --total-executor-cores {options.amount_nodes} --executor-cores {options.cpu_cores} --executor-memory {options.amount_ram}M " + \
        f"--py-files /home/{home_user}/bd/spark/lib/bigdl-0.11.0-python-api.zip,/home/{home_user}/bd/codes/{options.network}.py " + \
        f"--properties-file /home/{home_user}/bd/spark/conf/spark-bigdl.conf " + \
        f"--jars /home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
        f"--conf spark.driver.extraClassPath=/home/{home_user}/bd/spark/lib/bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar " + \
        f"--conf spark.executer.extraClassPath=bigdl-SPARK_2.3-0.11.0-jar-with-dependencies.jar /home/{home_user}/bd/codes/{network}.py " + \
        f"--action train --dataPath /tmp/mnist --batchSize {options.batch_size} --endTriggerNum {max_epochs} " + \
        f"--learningRate 0.01 --learningrateDecay 0.0002 > {ex_id}-{options.filename}"

    print(f"Executing experiment: {command}")
    stdin, stdout, stderr = master.ssh.exec_command(command, timeout=EX_RUNTIME)

    try:
        print(stdout.read())
        print(stderr.read())
    except PipeTimeout:
        print("PipeTimeout")
    except socket.timeout:
        print("Socket timeout")
    # master.cancel()
    run_command(master.ssh, '/home/am72ghiassi/bd/spark/sbin/stop-master.sh')
    sftp = master.ssh.open_sftp()
    local_file_name = f'raw/binsearch/{options.filename}'
    sftp.get(f'{ex_id}-{options.filename}', local_file_name)
    duration = read_log_file(local_file_name)

    for slave in slaves:
        run_command(slave, f'/home/am72ghiassi/bd/spark/sbin/stop-slave.sh')

    return duration


def perform_binary_search(master: Master, slaves: List[SSHClient], options: ExperimentOptions) -> int:
    options.amount_nodes = 1
    durations = {0: -1, 1: read_log_file(options.get_filename())}
    print(f"Got a ground truth of {durations}")

    upper_bound = len(slaves) * 4
    lower_bound = 1
    while True:
        mid = int(lower_bound + (upper_bound - lower_bound) / 2) + 1

        if mid in durations and mid - 1 in durations:
            return mid

        if mid not in durations:
            options.amount_nodes = mid
            durations[mid] = execute_experiment(master, slaves, options)

        if mid - 1 not in durations:
            options.amount_nodes = mid - 1
            durations[mid - 1] = execute_experiment(master, slaves, options)

        speedup_mid = durations[mid] / durations[1]
        speedup_mid_before = durations[mid - 1] / durations[1]
        performance_extra_node = speedup_mid - speedup_mid_before
        if performance_extra_node > 0.05:
            lower_bound = mid
        else:
            upper_bound = mid


def connect_master_slaves() -> Tuple[Master, List[SSHClient]]:
    def connect_ssh(ip: str, cloud_info: GoogleCloudInfo) -> SSHClient:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
        ssh.connect(ip, port=22, username=cloud_info.ssh_user, key_filename=cloud_info.ssh_key_path)
        return ssh

    df_master = pd.read_csv('connections/master.csv', delimiter=',')
    master = Master(
        connect_ssh(df_master['external-IP'], cloud_info_list[int(df_master['cloud_index'])]),
        df_master['external-IP'],
        df_master['internal-IP']
    )
    df_slaves = pd.read_csv('connections/slaves.csv', delimiter=',')
    slaves = []
    for _, row in df_slaves.iterrows():
        slaves.append(connect_ssh(row['external-IP'], cloud_info_list[int(row['cloud_index'])]))

    return master, slaves


def main():
    master_ssh, slaves = connect_master_slaves()
    print(f"Retrieved master and {len(slaves)} slaves.")

    with io.open('results.csv', encoding='utf-8', mode='w') as file:
        file.write('cores,ram,network,batch_size,nodes\n')

    for cpu_cores in [2, 4]:
        for amount_ram in [2048, 4096]:
            for network in ['bi-rnn', 'lenet5']:
                for batch_size in [64, 256]:
                    options = ExperimentOptions(cpu_cores, amount_ram, network, batch_size, 0)
                    result = perform_binary_search(options)

                    with io.open('results.csv', encoding='utf-8', mode='a') as file:
                        file.write("%s,%s,%s,%s,%s\n" % (cpu_cores, amount_ram, network, batch_size, result))


if __name__ == '__main__':
    main()
